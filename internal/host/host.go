// Package host assembles the stable entry, state owner and release controller
// in a single process. Only business workers are replaced during a rollout.
package host

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/service"
	"aiagent-gateway/internal/state"
)

type localEntry struct{ e *entry.Entry }

func (l localEntry) Traffic(ctx context.Context) (p.Traffic, error) { return l.e.Traffic(), ctx.Err() }
func (l localEntry) Apply(ctx context.Context, rev uint64, next p.Traffic) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return l.e.Apply(rev, next)
}
func (l localEntry) Stats(ctx context.Context, since time.Time, worker string) (p.Statistics, error) {
	return l.e.Stats(since, worker), ctx.Err()
}

func Serve(ctx context.Context, cfg appconfig.Config, configPath, deploymentPath string) error {
	d, err := service.LoadDeployment(deploymentPath)
	if err != nil {
		return err
	}
	info, err := os.Lstat(deploymentPath)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return fmt.Errorf("untrusted deployment manifest")
	}
	uid := st.Uid
	if uid != 0 && uid != uint32(os.Geteuid()) {
		return fmt.Errorf("unexpected deployment owner")
	}
	configPath, err = filepath.Abs(configPath)
	if err != nil {
		return err
	}
	root := filepath.Dir(filepath.Dir(d.InitialRelease))
	if root == service.ReleasesDir {
		uid = 0
	}
	if err = service.ValidateArtifact(root, d.InitialRelease, uid); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts := cfg.Options()
	opts.DeferBackground = true
	g, err := gateway.Open(opts)
	if err != nil {
		return err
	}
	j, err := state.Open(filepath.Join(cfg.DataDir, "shared"))
	if err != nil {
		g.Close()
		return err
	}
	defer func() { g.Close(); j.Close() }()
	shared, err := gateway.NewSharedServer(g, j)
	if err != nil {
		return err
	}
	ej, err := state.Open(filepath.Join(cfg.DataDir, "entry"))
	if err != nil {
		return err
	}
	defer ej.Close()
	e, err := entry.Open(ej, cfg.AdminToken, shared.Resolve)
	if err != nil {
		return err
	}
	rj, err := state.Open(filepath.Join(cfg.DataDir, "release"))
	if err != nil {
		return err
	}
	defer rj.Close()
	driver := service.NewChildProcessDriver(service.ChildOptions{ConfigPath: configPath, ReleasesDir: root, Token: cfg.AdminToken, OwnerUID: uid, Freeze: shared.Freeze, Retire: shared.Retire, Count: shared.Count, Output: os.Stderr})
	defer driver.Close()
	c, err := rollout.Open(rj, driver, localEntry{e})
	if err != nil {
		return err
	}
	if err = c.RecoverHost(ctx); err != nil {
		return err
	}
	if c.Report().Phase == "" {
		if err = c.Begin(d.InitialRelease, d.Policy); err != nil {
			return err
		}
	}
	var accepting atomic.Bool
	ready := func(ctx context.Context) bool {
		if j.Healthy() != nil || ej.Healthy() != nil || rj.Healthy() != nil {
			return false
		}
		id, err := driver.Identity(ctx, e.Traffic().Stable)
		return err == nil && id.Ready
	}
	type endpoint struct {
		server   *http.Server
		listener net.Listener
	}
	var servers []endpoint
	defer func() {
		for _, s := range servers {
			s.server.Close()
			s.listener.Close()
		}
	}()
	add := func(network, address string, handler http.Handler, restricted bool) error {
		var l net.Listener
		var err error
		if network == "unix" {
			l, err = unixListener(address)
		} else {
			l, err = net.Listen(network, address)
		}
		if err != nil {
			return err
		}
		s := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
		if restricted {
			s.ConnContext = peerContext
			s.Handler = authorizePeer(handler, uid)
		}
		servers = append(servers, endpoint{s, l})
		return nil
	}
	// The state endpoint is available before Tick starts a worker. All public
	// mutation controls are isolated from the public admin and data handlers.
	for _, ep := range []struct {
		network, address string
		handler          http.Handler
		restricted       bool
	}{
		{"unix", cfg.StateSocket, shared.Handler(), false},
		{"unix", cfg.EntrySocket, e.Control(), true},
		{"unix", cfg.ControlSocket, c.Handler(cfg.AdminToken), true},
		{"tcp", cfg.ListenAddress(), gateway.PublicHandler(dataHandler(e, &accepting, ready), g.AdminHandler()), false},
	} {
		if err = add(ep.network, ep.address, ep.handler, ep.restricted); err != nil {
			return err
		}
	}
	g.StartBackground()
	errs := make(chan error, len(servers))
	for _, s := range servers {
		go func(s endpoint) { errs <- s.server.Serve(s.listener) }(s)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			if err := c.Tick(ctx); err != nil && ctx.Err() == nil {
				slog.Error("host rollout tick failed", "error", err)
			}
			if !accepting.Load() && ctx.Err() == nil && ready(ctx) {
				accepting.Store(true)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	slog.Info("gateway host started", "pid", os.Getpid(), "http", cfg.ListenAddress())
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	cancel()
	<-done
	accepting.Store(false)
	// Keep state available while ingress drains. Manual stop has a bounded
	// shutdown window; normal rollout drain is handled by the controller.
	shutdown, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	for i := len(servers) - 1; i >= 0; i-- {
		if e := servers[i].server.Shutdown(shutdown); e != nil {
			servers[i].server.Close()
		}
	}
	driver.Close()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Persisted entry targets describe dead children after a host restart. Do not
// expose those targets, or pass systemd's readiness check, before recovery.
func dataHandler(next http.Handler, accepting *atomic.Bool, ready func(context.Context) bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && (!accepting.Load() || (r.URL.Path == "/readyz" && !ready(r.Context()))) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"error":"HOST_NOT_READY"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func unixListener(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("control path is not a socket")
		}
		conn, e := net.DialTimeout("unix", path, time.Second)
		if e == nil {
			conn.Close()
			return nil, fmt.Errorf("control socket already active")
		}
		if err = os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

type peerKey struct{}
type peer struct {
	uid uint32
	err error
}

func peerContext(ctx context.Context, conn net.Conn) context.Context {
	uid, err := peerUID(conn)
	return context.WithValue(ctx, peerKey{}, peer{uid, err})
}
func authorizePeer(next http.Handler, uid uint32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := r.Context().Value(peerKey{}).(peer)
		if !ok || p.err != nil || p.uid != uid {
			http.Error(w, "forbidden local identity", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
