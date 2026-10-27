package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway/runtime/shared"
	p "aiagent-gateway/internal/releaseprotocol"
)

type ServiceOptions struct {
	ConfigPath, ReleasesDir, Unit string
	OwnerUID                      uint32
	Policy                        p.Policy
	Output                        io.Writer
}

type controlPeer struct{}

const (
	publicSocket = iota
	controlSocket
	inheritedSocketCount
	candidateConfigFD = 3 + inheritedSocketCount
	// CandidateChannelFD follows the HTTP listener, control listener and config.
	CandidateChannelFD = candidateConfigFD + 1
)

// Service owns the listeners and release controller in the complete gateway
// PID. A committed child receives those responsibilities before the source exits.
type Service struct {
	node                        atomic.Pointer[Gateway]
	controller                  *ProcessController
	opts                        ServiceOptions
	sockets                     [inheritedSocketCount]*Socket
	mu                          sync.Mutex
	servers                     []*http.Server
	publicActive, controlActive bool
	exit                        chan struct{}
	once                        sync.Once
	errs                        chan error
	retired                     atomic.Bool
	ownerChanged                func(*Gateway)
	queueStatus                 func() (shared.BarrierStatus, shared.BarrierStatus)
}

func newService(n *Gateway, sockets [inheritedSocketCount]*Socket, opts ServiceOptions, inherited bool) (*Service, error) {
	s := &Service{opts: opts, sockets: sockets, exit: make(chan struct{}), errs: make(chan error, 1), publicActive: inherited}
	s.node.Store(n)
	var err error
	s.controller, err = openProcessController(s, inherited)
	return s, err
}

func (s *Service) setOwner(n *Gateway) {
	s.node.Store(n)
	if s.ownerChanged != nil {
		s.ownerChanged(n)
	}
}

func (s *Service) serve(index int, handler http.Handler) error {
	l, err := s.sockets[index].Activate()
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	if index == controlSocket {
		server.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
			uid, err := peerUID(conn)
			return context.WithValue(ctx, controlPeer{}, err == nil && uid == s.opts.OwnerUID)
		}
	}
	s.servers = append(s.servers, server)
	go func() {
		if err := server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			select {
			case s.errs <- err:
			default:
			}
		}
	}()
	return nil
}

func (s *Service) acceptPublic() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.publicActive {
		return nil
	}
	if err := s.serve(publicSocket, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.node.Load().PublicHandler().ServeHTTP(w, r)
	})); err != nil {
		return err
	}
	s.publicActive = true
	return nil
}

func (s *Service) pausePublic() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.sockets[publicSocket].Pause()
	for _, server := range s.servers {
		server.SetKeepAlivesEnabled(false)
	}
	s.publicActive = false
}

func (s *Service) acceptControl() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.controlActive {
		return nil
	}
	if err := s.serve(controlSocket, s.controlHandler()); err != nil {
		return err
	}
	s.controlActive = true
	return nil
}

func (s *Service) pauseControl() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.sockets[controlSocket].Pause()
	s.controlActive = false
	for _, server := range s.servers {
		server.SetKeepAlivesEnabled(false)
	}
}

func (s *Service) ready(ctx context.Context, adopting bool) error {
	if s.opts.Unit == "" {
		return nil
	}
	if adopting {
		if err := WaitMainPID(ctx, s.opts.Unit, os.Getpid()); err != nil {
			return err
		}
	}
	if err := (Notifier{Socket: os.Getenv("NOTIFY_SOCKET")}).Ready(ctx); err != nil {
		return err
	}
	s.controller.mu.Lock()
	defer s.controller.mu.Unlock()
	s.controller.report.MainPID = os.Getpid()
	s.controller.report.LifecycleConfirmed = time.Now()
	return s.controller.save()
}

func (s *Service) retire() { s.retired.Store(true); s.once.Do(func() { close(s.exit) }) }

func (s *Service) run(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.exit:
			return nil
		case err := <-s.errs:
			return err
		case <-ticker.C:
			if err := s.controller.Tick(ctx); err != nil {
				if !s.controller.committed && !s.node.Load().StorageHealthy() {
					return err
				}
				if s.opts.Output != nil {
					fmt.Fprintln(s.opts.Output, "rollout:", err)
				}
			}
		}
	}
}

func (s *Service) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n := s.node.Load()
	var stopping *CandidateProcess
	if !s.retired.Load() {
		s.pausePublic()
		n.handoffMu.Lock()
		candidate := n.candidate
		n.handoffMu.Unlock()
		if candidate != nil {
			// This handle refers to a child of this process. A committed child
			// is deliberately left to its new lifecycle owner.
			d, err := readCurrentDecision(n)
			if err == nil && d.Owner.Incarnation != candidate.target.Incarnation {
				stopping = candidate
				_ = candidate.Pause(ctx)
			}
		}
	}
	s.mu.Lock()
	servers := append([]*http.Server(nil), s.servers...)
	s.mu.Unlock()
	for _, server := range servers {
		if server.Shutdown(ctx) != nil {
			_ = server.Close()
		}
	}
	if stopping != nil {
		for ctx.Err() == nil {
			select {
			case <-stopping.Done():
				goto stopped
			default:
			}
			if stopping.Stop(ctx, n) == nil {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(25 * time.Millisecond):
			}
		}
		select {
		case <-stopping.Done():
		default:
			_ = stopping.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-stopping.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}
stopped:
	for _, socket := range s.sockets {
		_ = socket.Close()
	}
	_ = s.node.Load().Close()
}

func (s *Service) controlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if allowed, _ := r.Context().Value(controlPeer{}).(bool); !allowed {
			w.WriteHeader(403)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+s.node.Load().config.AdminToken {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/control/v1/identity":
			if r.Method != "GET" {
				w.WriteHeader(405)
				return
			}
			_ = json.NewEncoder(w).Encode(s.node.Load().Identity())
		case "/control/v1/traffic", "/control/v1/stats":
			if r.Method != "GET" {
				w.WriteHeader(405)
				return
			}
			s.node.Load().entry.Control().ServeHTTP(w, r)
		default:
			s.controller.Handler().ServeHTTP(w, r)
		}
	})
}

func ServeService(ctx context.Context, cfg appconfig.Config, target p.Target, id p.Identity, opts ServiceOptions) error {
	if opts.Policy.Validate() != nil || opts.ConfigPath == "" || opts.ReleasesDir == "" {
		return fmt.Errorf("INVALID_SERVICE_OPTIONS")
	}
	n, err := OpenGatewayWithPolicy(cfg, target, id, opts.Policy.Handoff)
	if err != nil {
		return err
	}
	defer func() { _ = n.Close() }()
	var sockets [inheritedSocketCount]*Socket
	defer func() {
		for _, socket := range sockets {
			if socket != nil {
				_ = socket.Close()
			}
		}
	}()
	if err = os.MkdirAll(filepath.Dir(cfg.ControlSocket), 0700); err != nil {
		return err
	}
	if st, statErr := os.Lstat(cfg.ControlSocket); statErr == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("INVALID_CONTROL_SOCKET_PATH")
		}
		if err = os.Remove(cfg.ControlSocket); err != nil {
			return err
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	for i, ep := range []struct{ network, address string }{{"tcp", cfg.ListenAddress()}, {"unix", cfg.ControlSocket}} {
		sockets[i], err = Listen(ep.network, ep.address)
		if err != nil {
			return err
		}
	}
	if err = os.Chmod(cfg.ControlSocket, 0600); err != nil {
		return err
	}
	s, err := newService(n, sockets, opts, false)
	if err != nil {
		return err
	}
	defer s.close()
	if err = s.acceptPublic(); err != nil {
		return err
	}
	if err = s.acceptControl(); err != nil {
		return err
	}
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = s.ready(readyCtx, false); err != nil {
		return err
	}
	return s.run(ctx)
}
