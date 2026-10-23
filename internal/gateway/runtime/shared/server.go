package shared

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

type Backend struct {
	// Barrier is shared with all owner-local writers. Nil creates a private
	// barrier, suitable for a backend with no independent state writers.
	Barrier *Barrier
	// Discovery performs network I/O before the state dispatcher is locked.
	Discover     func(context.Context, Request)
	Execute      func(Request, *Leases) Response
	Freeze       func(bool)
	RestoreLease func(LeaseRecord) (func(), error)
}

// Server serializes shared operations and keeps retries within one generation
// idempotent. State and ownership remain with the runtime backend.
type Server struct {
	barrier       *Barrier
	revision      uint64
	epoch         string
	previousEpoch string
	token         string
	backend       Backend
	mu            sync.Mutex
	leases        Leases
	replies       map[string]cachedReply
	order         []string
	retired       map[string]uint64
}

type cachedReply struct {
	fingerprint string
	response    Response
}

func NewServer(token string, backend Backend) *Server {
	barrier := backend.Barrier
	if barrier == nil {
		barrier = &Barrier{}
	}
	return &Server{barrier: barrier, epoch: releaseprotocol.ID("state_"), token: token, backend: backend, leases: Leases{items: map[string]lease{}}, replies: map[string]cachedReply{}, retired: map[string]uint64{}}
}

// InitializeEpoch binds an unused dispatcher to the process owner decision.
func (s *Server) InitializeEpoch(epoch string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch == "" || s.barrier.Revision() != 0 || len(s.replies) != 0 {
		return fmt.Errorf("STATE_EPOCH_ALREADY_ACTIVE")
	}
	s.epoch = epoch
	return nil
}

// Admit extends the owner barrier to entry and release writes in this process.
func (s *Server) Admit() (func(), error)                  { return s.barrier.TryEnter() }
func (s *Server) ConfigureQueue(limits QueueLimits) error { return s.barrier.Configure(limits) }

func (s *Server) ReconfigureQueue(limits QueueLimits) error { return s.barrier.Reconfigure(limits) }

func (s *Server) Handler() http.Handler {
	return s.handler(s.token, "")
}

// HandlerFor binds a private channel credential to one execution holder.
// A candidate cannot claim another holder or invoke owner lifecycle controls.
func (s *Server) HandlerFor(token, worker string) http.Handler {
	if token == "" || worker == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			protocol.Problem(w, 403, "INVALID_STATE_PEER")
		})
	}
	return s.handler(token, worker)
}

func (s *Server) handler(token, worker string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
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
		if worker != "" && (q.Worker != worker || q.Action == "retire" || q.Action == "freeze") {
			protocol.Problem(w, 403, "STATE_PEER_FORBIDDEN")
			return
		}
		protocol.WriteJSON(w, 200, s.dispatch(r.Context(), q))
	})
}

// dispatch is shared by the local and UDS paths. It returns detached values:
// callers cannot mutate the owner's live configuration or cached receipts.
func (s *Server) dispatch(ctx context.Context, q Request) (out Response) {
	rawRequest, err := json.Marshal(q)
	if err != nil {
		return Response{Error: "INVALID_STATE_REQUEST"}
	}
	leave, err := s.barrier.enter(ctx, len(rawRequest))
	if err != nil {
		return Response{Error: err.Error()}
	}
	defer leave()
	if err := ctx.Err(); err != nil {
		return Response{Error: err.Error()}
	}
	if (q.Action == "acquire" || q.Action == "reserve") && s.backend.Discover != nil {
		s.backend.Discover(ctx, q)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() { out = copyResponse(out) }()
	if err := ctx.Err(); err != nil {
		return Response{Epoch: s.epoch, Error: err.Error()}
	}
	key := q.Worker + "/" + q.ID
	semantic := q
	semantic.Epoch = ""
	raw, _ := json.Marshal(semantic)
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	if q.Epoch != "" && q.Epoch != s.epoch {
		return Response{Epoch: s.epoch, Error: "STATE_GENERATION_CHANGED"}
	}
	if old, ok := s.replies[key]; ok {
		if old.fingerprint != fingerprint {
			return Response{Epoch: s.epoch, Error: "OPERATION_CONFLICT"}
		}
		out = old.response
		out.Epoch = s.epoch
		return out
	}
	readOnly := q.Action == "config" || q.Action == "status" || q.Action == "resolve" || q.Action == "list" || q.Action == "lookup" || q.Action == "cache_get"
	stream, sequence := operationSequence(q.ID)
	if floor, retired := s.retired[q.Worker+"/"+stream]; retired && !readOnly {
		if sequence <= floor {
			return Response{Epoch: s.epoch, Error: "OPERATION_RESULT_UNKNOWN"}
		}
	}
	out = s.backend.Execute(q, &s.leases)
	s.revision++
	out.Epoch = s.epoch
	if !readOnly {
		s.replies[key] = cachedReply{fingerprint: fingerprint, response: copyResponse(out)}
		s.order = append(s.order, key)
		if len(s.order) > 8192 {
			worker, id, _ := strings.Cut(s.order[0], "/")
			stream, sequence := operationSequence(id)
			boundary := worker + "/" + stream
			s.retired[boundary] = max(s.retired[boundary], sequence)
			delete(s.replies, s.order[0])
			s.order = s.order[1:]
		}
	}
	return out
}

func copyResponse(out Response) Response {
	raw, err := json.Marshal(out)
	var copy Response
	if err == nil {
		err = json.Unmarshal(raw, &copy)
	}
	if err != nil {
		return Response{Epoch: out.Epoch, Error: "STATE_RESPONSE_INVALID"}
	}
	return copy
}

func (s *Server) Freeze(frozen bool) error {
	leave, err := s.barrier.TryEnter()
	if err != nil {
		return err
	}
	defer leave()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backend.Freeze(frozen)
	s.revision++
	return nil
}

func (s *Server) Retire(worker string) error {
	leave, err := s.barrier.TryEnter()
	if err != nil {
		return err
	}
	defer leave()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.backend.Execute(Request{Action: "retire", Worker: worker}, &s.leases)
	s.revision++
	if out.Error != "" {
		return fmt.Errorf("%s", out.Error)
	}
	return nil
}

// Resolve uses the same dispatcher and backend as the authenticated RPC path.
// It is only exposed to the in-process entry, never to an external handler.
func (s *Server) Resolve(ctx context.Context, q releaseprotocol.Request) (releaseprotocol.Resolution, error) {
	if err := ctx.Err(); err != nil {
		return releaseprotocol.Resolution{}, err
	}
	out := s.dispatch(ctx, Request{Action: "resolve", Request: q})
	if out.Error != "" {
		return releaseprotocol.Resolution{}, fmt.Errorf("%s", out.Error)
	}
	return out.Resolution, nil
}

func (s *Server) Count(worker string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leases.Count(worker)
}

// Leases is used only while Server holds the dispatcher lock.
type Leases struct{ items map[string]lease }

type lease struct {
	record  LeaseRecord
	release func()
}

// LeaseRecord contains only stable identities; it never serializes closures or
// retries an upstream operation while restoring the lease on a new owner.
type LeaseRecord struct {
	ID, Worker, Kind, Owner, Route, Endpoint, Entry, Session string
	Resource                                                 protocol.ResourceKey
}

func (l *Leases) Add(record LeaseRecord, release func()) string {
	id := releaseprotocol.ID("lease_")
	record.ID = id
	l.items[id] = lease{record: record, release: release}
	return id
}

func (l *Leases) Count(worker string) int64 {
	var n int64
	for _, ref := range l.items {
		if worker == "" || ref.record.Worker == worker {
			n++
		}
	}
	return n
}

func (l *Leases) Retire(worker string) {
	for id, ref := range l.items {
		if ref.record.Worker == worker {
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
	if ref.record.Worker != worker {
		return false, protocol.ErrResourceConflict
	}
	ref.release()
	delete(l.items, id)
	return true, nil
}
