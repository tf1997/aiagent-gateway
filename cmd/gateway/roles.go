package main

import (
	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/service"
	"aiagent-gateway/internal/state"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func unixListener(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
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
	if err == nil {
		err = os.Chmod(path, 0600)
		if err != nil {
			l.Close()
		}
	}
	return l, err
}
func runRole(role string, args []string) int {
	if err := serveRole(role, args); err != nil {
		slog.Error("role failed", "role", role, "error", err)
		return 1
	}
	return 0
}
func serveRole(role string, args []string) error {
	fs := flag.NewFlagSet(role, flag.ContinueOnError)
	configPath := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
	slot := fs.String("slot", "", "worker slot")
	release := fs.String("release", "", "release digest")
	listen := fs.String("listen", "", "override YAML data listener for this role")
	admin := fs.String("admin", "", "override YAML admin listener for this role")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	cfg, err := appconfig.Load(*configPath)
	if err != nil {
		return err
	}
	opts := cfg.Options()
	if *listen == "" {
		*listen = cfg.ListenAddress()
	}
	if *admin == "" {
		*admin = cfg.AdminListenAddress()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	stateSocket := cfg.StateSocket
	entrySocket := cfg.EntrySocket
	controlSocket := cfg.ControlSocket
	type endpoint struct {
		server   *http.Server
		listener net.Listener
	}
	var servers []endpoint
	roleErrors := make(chan error, 1)
	add := func(address string, unix bool, h http.Handler) error {
		var l net.Listener
		var err error
		if unix {
			l, err = unixListener(address)
		} else {
			l, err = net.Listen("tcp", address)
		}
		if err != nil {
			return err
		}
		servers = append(servers, endpoint{&http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}, l})
		return nil
	}
	defer func() {
		for _, s := range servers {
			s.listener.Close()
		}
	}()
	switch role {
	case "worker":
		if *slot != "a" && *slot != "b" {
			return fmt.Errorf("worker requires slot a or b")
		}
		if *release == "" {
			return fmt.Errorf("worker requires release")
		}
		opts.StateSocket = stateSocket
		opts.WorkerID = *slot + ":" + p.ID("")
		g, err := gateway.Open(opts)
		if err != nil {
			return err
		}
		defer g.Close()
		identity := p.Identity{Protocol: 1, StateProtocol: 1, Release: *release, Instance: *slot, Incarnation: opts.WorkerID}
		go func() {
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if !g.WorkerIdentity(identity).Ready {
						roleErrors <- fmt.Errorf("worker lost shared state generation")
						return
					}
				}
			}
		}()
		if err = add(*listen, false, g.WorkerHandler(identity)); err != nil {
			return err
		}
		if err = add(*admin, false, g.WorkerControl(identity)); err != nil {
			return err
		}
	case "state-server":
		opts.DeferBackground = true
		g, err := gateway.Open(opts)
		if err != nil {
			return err
		}

		j, err := state.Open(filepath.Join(opts.DataDir, "shared"))
		if err != nil {
			g.Close()
			return err
		}
		defer func() { g.Close(); j.Close() }()
		shared, err := gateway.NewSharedServer(g, j)
		if err != nil {
			return err
		}
		g.StartBackground()
		if err = add(stateSocket, true, shared.Handler()); err != nil {
			return err
		}
		if err = add(*admin, false, g.AdminHandler()); err != nil {
			return err
		}
	case "entry":
		j, err := state.Open(filepath.Join(opts.DataDir, "entry"))
		if err != nil {
			return err
		}
		defer j.Close()
		e, err := entry.Open(j, opts.AdminToken, func(ctx context.Context, q p.Request) (p.Resolution, error) {
			return gateway.ResolveShared(stateSocket, opts.AdminToken, q)
		})
		if err != nil {
			return err
		}
		if err = add(*listen, false, e); err != nil {
			return err
		}
		if err = add(entrySocket, true, e.Control()); err != nil {
			return err
		}
	case "rollout-controller":
		d, err := service.LoadDeployment(cfg.Deployment)
		if err != nil {
			return err
		}
		j, err := state.Open(cfg.RolloutData)
		if err != nil {
			return err
		}
		defer j.Close()
		client := rollout.NewClient(entrySocket, opts.AdminToken)
		defer client.HTTP.CloseIdleConnections()
		driver := service.NewSystemdDriver(opts.AdminToken)
		c, err := rollout.Open(j, driver, client)
		if err != nil {
			return err
		}
		if err = add(controlSocket, true, c.Handler(opts.AdminToken)); err != nil {
			return err
		}
		for _, socket := range []string{stateSocket, entrySocket} {
			waitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			err = waitSocket(waitCtx, socket)
			stop()
			if err != nil {
				return err
			}
		}
		if c.Report().Phase == "" {
			if err = c.Begin(d.InitialRelease, d.Policy); err != nil {
				return err
			}
		}
		done := make(chan struct{})
		defer func() { cancel(); <-done }()
		go func() {
			defer close(done)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if err := c.Tick(ctx); err != nil {
						slog.Error("rollout tick failed", "error", err)
					}
				}
			}
		}()
	default:
		return fmt.Errorf("unknown role")
	}
	errs := make(chan error, len(servers))
	for _, s := range servers {
		go func(s endpoint) { errs <- s.server.Serve(s.listener) }(s)
	}
	slog.Info("gateway role started", "role", role)
	select {
	case <-ctx.Done():
	case err = <-errs:
		cancel()
	case err = <-roleErrors:
		cancel()
	}
	shutdown, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	for _, s := range servers {
		if e := s.server.Shutdown(shutdown); e != nil {
			s.server.Close()
		}
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func waitSocket(ctx context.Context, path string) error {
	for {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", path)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
