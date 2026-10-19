package process

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

// Gateway is the complete process runtime. The state owner and business
// executor are separate modules in this PID so both local and candidate
// execution use the same state dispatcher and lease accounting.
type Gateway struct {
	owner                 *handoff.Owner
	core, business        *gateway.Gateway
	shared                *shared.Server
	entry                 *entry.Entry
	journal, entryJournal *state.Journal
	target                p.Target
	identity              p.Identity
	ingress               atomic.Bool
	execution             atomic.Bool
	closeOnce             sync.Once
	closeErr              error
}

// OpenGateway acquires the process owner before opening any writable domain.
// Cold recovery must only run after the service manager has removed old
// incarnations. Live adoption uses a different protocol, never this function.
func OpenGateway(cfg appconfig.Config, target p.Target, identity p.Identity) (_ *Gateway, err error) {
	if identity.Incarnation != target.Incarnation || identity.Release != target.Release || identity.Instance != target.Instance {
		return nil, fmt.Errorf("process identity differs from traffic target")
	}
	n := &Gateway{target: target, identity: identity}
	defer func() {
		if err != nil {
			n.Close()
		}
	}()
	n.owner, err = handoff.Acquire(cfg.DataDir, handoff.Identity{Incarnation: identity.Incarnation, Release: identity.Release})
	if err != nil {
		return nil, err
	}
	if n.owner.Decision().Phase == "" {
		err = n.owner.Bootstrap()
	} else {
		_, err = n.owner.Recover()
	}
	if err != nil {
		return nil, err
	}
	opts := cfg.Options()
	opts.DeferBackground = true
	n.core, err = gateway.Open(opts)
	if err != nil {
		return nil, err
	}
	n.journal, err = state.Open(filepath.Join(cfg.DataDir, "shared"))
	if err != nil {
		return nil, err
	}
	n.shared, err = gateway.NewSharedServer(n.core, n.journal)
	if err != nil {
		return nil, err
	}
	opts.StateClient = shared.NewLocalClient(n.shared, identity.Incarnation)
	opts.WorkerID = identity.Incarnation
	n.business, err = gateway.Open(opts)
	if err != nil {
		return nil, err
	}
	n.entryJournal, err = state.Open(filepath.Join(cfg.DataDir, "entry"))
	if err != nil {
		return nil, err
	}
	n.entry, err = entry.Open(n.entryJournal, cfg.AdminToken, n.shared.Resolve)
	if err != nil {
		return nil, err
	}
	if err = n.entry.SetLocal(target, n.execute); err != nil {
		return nil, err
	}
	revision := n.entry.Traffic().Revision
	if err = n.entry.Apply(revision, p.Traffic{Revision: revision + 1, Stable: target}); err != nil {
		return nil, err
	}
	n.execution.Store(true)
	n.ingress.Store(true)
	n.core.StartBackground()
	return n, nil
}

func (n *Gateway) execute(w http.ResponseWriter, r *http.Request, done func(p.Result)) {
	if !n.execution.Load() {
		http.Error(w, "EXECUTION_NOT_ACCEPTING", http.StatusServiceUnavailable)
		done(p.Result{Profile: "http", Verdict: "excluded", Reason: "execution_not_accepting", Status: 503})
		return
	}
	n.business.ExecuteLocal(w, r, done)
}

func (n *Gateway) Ready() bool {
	return n.ingress.Load() && n.execution.Load() && n.StorageHealthy() && n.business.WorkerIdentity(n.identity).Ready
}

func (n *Gateway) StorageHealthy() bool {
	return n.core.StateHealthy() && n.journal.Healthy() == nil && n.entryJournal.Healthy() == nil
}

func (n *Gateway) DataHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && !n.Ready() {
			http.Error(w, "PROCESS_NOT_READY", http.StatusServiceUnavailable)
			return
		}
		n.entry.ServeHTTP(w, r)
	})
}

func (n *Gateway) AdminHandler() http.Handler { return n.core.AdminHandler() }
func (n *Gateway) StateHandler() http.Handler { return n.shared.Handler() }
func (n *Gateway) Identity() p.Identity       { return n.business.WorkerIdentity(n.identity) }

// StopIngress does not stop execution, state calls, or background work. The
// retiring entry may still have accepted requests which must finish normally.
func (n *Gateway) StopIngress()   { n.ingress.Store(false) }
func (n *Gateway) ResumeIngress() { n.ingress.Store(true) }
func (n *Gateway) StopExecution() { n.execution.Store(false) }

// Close is called after all accepted requests are drained or on startup error.
// The global owner lock is released after every writable domain is closed.
func (n *Gateway) Close() error {
	n.closeOnce.Do(func() {
		n.ingress.Store(false)
		n.execution.Store(false)
		if n.entry != nil {
			n.entry.Close()
		}
		if n.business != nil {
			n.closeErr = errors.Join(n.closeErr, n.business.Close())
		}
		if n.core != nil {
			n.closeErr = errors.Join(n.closeErr, n.core.Close())
		}
		if n.entryJournal != nil {
			n.closeErr = errors.Join(n.closeErr, n.entryJournal.Close())
		}
		if n.journal != nil {
			n.closeErr = errors.Join(n.closeErr, n.journal.Close())
		}
		if n.owner != nil {
			n.closeErr = errors.Join(n.closeErr, n.owner.Close())
		}
	})
	return n.closeErr
}

// ServeGateway exposes the complete runtime for development and regression
// tests. Installation remains on the existing path until live state adoption
// and the full rollout controller pass their acceptance tests.
func ServeGateway(ctx context.Context, cfg appconfig.Config, target p.Target, identity p.Identity) error {
	n, err := OpenGateway(cfg, target, identity)
	if err != nil {
		return err
	}
	defer n.Close()
	var sockets []*Socket
	var servers []*http.Server
	defer func() {
		for _, s := range servers {
			s.Close()
		}
		for _, s := range sockets {
			s.Close()
		}
	}()
	errs := make(chan error, 2)
	for _, ep := range []struct {
		address string
		handler http.Handler
	}{
		{cfg.ListenAddress(), n.DataHandler()}, {cfg.AdminListenAddress(), n.AdminHandler()},
	} {
		socket, e := Listen("tcp", ep.address)
		if e != nil {
			return e
		}
		sockets = append(sockets, socket)
		l, e := socket.Activate()
		if e != nil {
			return e
		}
		s := &http.Server{Handler: ep.handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
		servers = append(servers, s)
		go func() { errs <- s.Serve(l) }()
	}
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	n.StopIngress()
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, s := range sockets {
		_ = s.Pause()
	}
	for _, s := range servers {
		if e := s.Shutdown(shutdown); e != nil {
			_ = s.Close()
		}
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
