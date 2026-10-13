package shared

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

type Backend struct {
	// Discovery performs network I/O before the state dispatcher is locked.
	Discover func(context.Context, Request)
	Execute  func(Request, *Leases) Response
	Freeze   func(bool)
}

// Server serializes shared operations and keeps retries within one generation
// idempotent. State and ownership remain with the runtime backend.
type Server struct {
	epoch   string
	token   string
	backend Backend
	mu      sync.Mutex
	leases  Leases
	replies map[string]cachedReply
	order   []string
}

type cachedReply struct {
	fingerprint string
	response    Response
}

func NewServer(token string, backend Backend) *Server {
	return &Server{epoch: releaseprotocol.ID("state_"), token: token, backend: backend, leases: Leases{items: map[string]lease{}}, replies: map[string]cachedReply{}}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) != 1 {
			protocol.Problem(w, 401, "UNAUTHORIZED")
			return
		}
		if r.URL.Path != "/control/v1/state" || r.Method != "POST" {
			protocol.Problem(w, 404, "NOT_FOUND")
			return
		}
		var q Request
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&q) != nil {
			protocol.Problem(w, 400, "INVALID_REQUEST")
			return
		}
		if q.Action == "acquire" || q.Action == "reserve" {
			s.backend.Discover(r.Context(), q)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		key := q.Worker + "/" + q.ID
		raw, _ := json.Marshal(q)
		sum := sha256.Sum256(raw)
		fingerprint := hex.EncodeToString(sum[:])
		if old, ok := s.replies[key]; ok {
			if old.fingerprint != fingerprint {
				protocol.WriteJSON(w, 200, Response{Error: "OPERATION_CONFLICT"})
				return
			}
			protocol.WriteJSON(w, 200, old.response)
			return
		}
		var out Response
		if q.Epoch != "" && q.Epoch != s.epoch {
			out.Error = "STATE_GENERATION_CHANGED"
		} else {
			out = s.backend.Execute(q, &s.leases)
		}
		out.Epoch = s.epoch
		if q.Action != "config" && q.Action != "status" && q.Action != "resolve" && q.Action != "list" && q.Action != "lookup" && q.Action != "cache_get" {
			s.replies[key] = cachedReply{fingerprint: fingerprint, response: out}
			s.order = append(s.order, key)
			if len(s.order) > 8192 {
				delete(s.replies, s.order[0])
				s.order = s.order[1:]
			}
		}
		protocol.WriteJSON(w, 200, out)
	})
}

func (s *Server) Freeze(frozen bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backend.Freeze(frozen)
}

func (s *Server) Retire(worker string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backend.Execute(Request{Action: "retire", Worker: worker}, &s.leases)
}

// Leases is used only while Server holds the dispatcher lock.
type Leases struct{ items map[string]lease }

type lease struct {
	worker  string
	release func()
}

func (l *Leases) Add(worker string, release func()) string {
	id := releaseprotocol.ID("lease_")
	l.items[id] = lease{worker: worker, release: release}
	return id
}

func (l *Leases) Count(worker string) int64 {
	var n int64
	for _, ref := range l.items {
		if worker == "" || ref.worker == worker {
			n++
		}
	}
	return n
}

func (l *Leases) Retire(worker string) {
	for id, ref := range l.items {
		if ref.worker == worker {
			ref.release()
			delete(l.items, id)
		}
	}
}

func (l *Leases) Release(worker, id string) (bool, error) {
	ref, ok := l.items[id]
	if !ok {
		return false, nil
	}
	if ref.worker != worker {
		return false, protocol.ErrResourceConflict
	}
	ref.release()
	delete(l.items, id)
	return true, nil
}
