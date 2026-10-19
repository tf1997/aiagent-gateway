// Package rollout adapts Agent requests to worker affinity and release verdicts.
// The generic entry and release controller remain outside the gateway packages.
package rollout

import (
	"context"
	"net/http"
	"sync/atomic"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/releaseprotocol"
)

// WorkerBackend supplies the stable handlers and live runtime state.
type WorkerBackend struct {
	Handler  http.Handler
	Remote   *shared.Client
	Refresh  func() error
	Ready    func() bool
	Revision func() uint64
}

type Worker struct {
	token    string
	backend  WorkerBackend
	requests atomic.Int64
}

func NewWorker(token string, backend WorkerBackend) *Worker {
	return &Worker{token: token, backend: backend}
}

// WorkerHandler emits trusted HTTP trailers only after the handler finishes.
// The stable entry combines them with its own downstream transport observation.
func (s *Worker) Handler(identity releaseprotocol.Identity) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !configuration.TokenOK("Bearer "+r.Header.Get(releaseprotocol.AuthHeader), s.token) {
			protocol.Problem(w, 401, "UNAUTHORIZED_ENTRY")
			return
		}
		w.Header().Add("Trailer", releaseprotocol.OutcomeTrailer)
		w.Header().Add("Trailer", releaseprotocol.ProfileTrailer)
		w.Header().Add("Trailer", releaseprotocol.ReasonTrailer)
		w.Header().Set(releaseprotocol.WorkerHeader, identity.Incarnation)
		s.execute(w, r, func(result releaseprotocol.Result) {
			w.Header().Set(releaseprotocol.OutcomeTrailer, result.Verdict)
			w.Header().Set(releaseprotocol.ProfileTrailer, result.Profile)
			w.Header().Set(releaseprotocol.ReasonTrailer, result.Reason)
		})
	})
}

// ExecuteLocal is only installed by the in-process traffic router. It has the
// same verdict collector as Handler, without internal HTTP headers or trailers.
func (s *Worker) ExecuteLocal(w http.ResponseWriter, r *http.Request, done func(releaseprotocol.Result)) {
	s.execute(w, r, done)
}

func (s *Worker) execute(w http.ResponseWriter, r *http.Request, done func(releaseprotocol.Result)) {
	if r.URL.Path == "/readyz" || r.URL.Path == "/healthz" {
		s.backend.Handler.ServeHTTP(w, r)
		return
	}
	if s.backend.Remote == nil || s.backend.Refresh() != nil {
		protocol.Problem(w, 503, "SHARED_STATE_UNAVAILABLE")
		done(releaseprotocol.Result{Profile: "http", Verdict: "failure", Reason: "state_unavailable", Status: 503})
		return
	}
	a := &proxyAttempt{}
	r = r.WithContext(context.WithValue(r.Context(), attemptKey{}, a))
	o := &resultWriter{ResponseWriter: w, profile: "http", attempt: a}
	s.requests.Add(1)
	defer s.requests.Add(-1)
	defer func() {
		v := recover()
		if v != nil {
			o.failure = "handler_aborted"
			if _, ok := v.(shared.Unavailable); ok && o.status == 0 {
				protocol.Problem(o, 503, "SHARED_STATE_UNAVAILABLE")
			} else {
				defer func() { panic(v) }()
			}
		}
		result := o.finish(r.Context())
		done(result)
		_ = s.backend.Remote.Call("request_result", shared.Request{ID: r.Header.Get(releaseprotocol.RequestHeader), Flag: result.Verdict == "failure"}, nil)
	}()
	s.backend.Handler.ServeHTTP(o, r)
}

func (s *Worker) Identity(base releaseprotocol.Identity) releaseprotocol.Identity {
	base.Protocol = 1
	base.StateProtocol = 1
	base.Ready = s.backend.Ready()
	base.ConfigRevision = s.backend.Revision()
	base.Inflight = s.requests.Load()
	if s.backend.Remote != nil {
		base.Ready = base.Ready && !s.backend.Remote.Failed()
		base.Inflight += s.backend.Remote.Active()
	}
	return base
}

func (s *Worker) Control(base releaseprotocol.Identity) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !configuration.TokenOK(r.Header.Get("Authorization"), s.token) {
			protocol.Problem(w, 401, "UNAUTHORIZED")
			return
		}
		if r.URL.Path != "/control/v1/identity" {
			protocol.Problem(w, 404, "NOT_FOUND")
			return
		}
		var err error
		if s.backend.Remote != nil {
			err = s.backend.Refresh()
		}
		identity := s.Identity(base)
		if err != nil {
			identity.Ready = false
		}
		protocol.WriteJSON(w, 200, identity)
	})
}
