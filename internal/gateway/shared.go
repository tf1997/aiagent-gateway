package gateway

import (
	"aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type sharedRequest struct {
	Epoch                                                             string
	Request                                                           releaseprotocol.Request
	Action, Worker, ID, Owner, Route, Endpoint, Entry, Session, Lease string
	Revision                                                          uint64
	Key                                                               resourceKey
	Kind                                                              resourceKind
	Resource, Execution                                               resource
	Resources                                                         []resource
	Creation                                                          resourceCreation
	Observations                                                      []resourceObservation
	IDs                                                               []string
	State                                                             resourceState
	Flag                                                              bool
	Cache                                                             cacheRecord
}
type resourcePair struct {
	Key   resourceReverseKey
	Value resource
}
type sharedResponse struct {
	Epoch       string
	Resolution  releaseprotocol.Resolution
	Error       string
	Config      *Config
	Resource    resource
	Resources   []resource
	Reservation creationReservation
	Mapped      []resourcePair
	Lease, ID   string
	Flag        bool
	Endpoint    *Endpoint
	Cache       cacheRecord
	Count       int64
}
type sharedUnavailable struct{ error }
type sharedClient struct {
	epochMu               sync.Mutex
	epoch                 string
	socket, token, worker string
	client                *http.Client
	failed                atomic.Bool
	active                atomic.Int64
}

func newSharedClient(socket, token, worker string) *sharedClient {
	return &sharedClient{socket: socket, token: token, worker: worker, client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}}
}
func (c *sharedClient) call(action string, q sharedRequest, out *sharedResponse) error {
	if c.failed.Load() {
		return fmt.Errorf("SHARED_STATE_UNAVAILABLE")
	}
	c.epochMu.Lock()
	q.Epoch = c.epoch
	c.epochMu.Unlock()
	q.Action = action
	q.Worker = c.worker
	if q.ID == "" {
		q.ID = releaseprotocol.ID("op_")
	}
	raw, _ := json.Marshal(q)
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		req, _ := http.NewRequest("POST", "http://state/control/v1/state", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+c.token)
		res, err := c.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		var v sharedResponse
		err = json.NewDecoder(io.LimitReader(res.Body, 64<<20)).Decode(&v)
		res.Body.Close()
		if err != nil {
			last = err
			continue
		}
		if res.StatusCode != 200 {
			last = fmt.Errorf("shared state HTTP %d", res.StatusCode)
			break
		}
		if v.Epoch != "" {
			c.epochMu.Lock()
			if c.epoch == "" {
				c.epoch = v.Epoch
			}
			changed := c.epoch != v.Epoch
			c.epochMu.Unlock()
			if changed {
				c.failed.Store(true)
				return fmt.Errorf("STATE_GENERATION_CHANGED")
			}
		}
		if v.Error != "" {
			return resourceError(v.Error)
		}
		if out != nil {
			*out = v
		}
		return nil
	}
	c.failed.Store(true)
	return fmt.Errorf("SHARED_STATE_UNAVAILABLE: %w", last)
}
func resourceError(s string) error {
	for _, e := range []error{errResourceNotFound, errResourceConflict, errResourceCapacity, errResourceInUse, errResourceInvalid} {
		if e.Error() == s {
			return e
		}
	}
	return errors.New(s)
}
func (c *sharedClient) must(action string, q sharedRequest, out *sharedResponse) {
	if err := c.call(action, q, out); err != nil {
		panic(sharedUnavailable{err})
	}
}
func (c *sharedClient) release(id string) func() {
	c.active.Add(1)
	var once sync.Once
	return func() { once.Do(func() { _ = c.call("release", sharedRequest{Lease: id}, nil); c.active.Add(-1) }) }
}
func (g *Gateway) sharedAcquire(owner, key, sid, forced, entry string, expected *Route) (*lease, error) {
	var out sharedResponse
	rev := uint64(0)
	if expected != nil {
		rev = expected.Version
	}
	err := g.remote.call("acquire", sharedRequest{Owner: owner, Route: key, Session: sid, Endpoint: forced, Entry: entry, Revision: rev}, &out)
	if err != nil {
		return nil, err
	}
	rt := g.current.Load().Routes[key]
	if rt == nil || out.Endpoint == nil {
		g.remote.release(out.Lease)()
		return nil, fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
	}
	st := g.state(rt.ID, out.Endpoint.ID)
	return &lease{route: rt, ep: out.Endpoint, state: st, iface: out.Endpoint.Interfaces[entry], release: g.remote.release(out.Lease)}, nil
}
func (g *Gateway) refreshShared() error {
	var out sharedResponse
	if err := g.remote.call("config", sharedRequest{}, &out); err != nil {
		return err
	}
	if out.Config == nil {
		return fmt.Errorf("missing shared config")
	}
	current := g.current.Load()
	if current == nil || current.Config.Revision != out.Config.Revision {
		if err := validate(out.Config); err != nil {
			return err
		}
		g.current.Store(build(*out.Config))
	}
	return nil
}

// SharedServer owns the existing gateway state machinery; workers execute the
// protocol handlers against this single authority through domain operations.
type SharedServer struct {
	epoch   string
	g       *Gateway
	j       *state.Journal
	mu      sync.Mutex
	leases  map[string]sharedLease
	replies map[string]cachedReply
	order   []string
	frozen  bool
}
type sharedLease struct {
	worker  string
	release func()
}
type cachedReply struct {
	fingerprint string
	response    sharedResponse
}

func NewSharedServer(g *Gateway, j *state.Journal) (*SharedServer, error) {
	if err := g.EnableDurability(j); err != nil {
		return nil, err
	}
	g.resources.onFailure = func() { g.healthy.Store(false) }
	return &SharedServer{epoch: releaseprotocol.ID("state_"), g: g, j: j, leases: map[string]sharedLease{}, replies: map[string]cachedReply{}}, nil
}
func (s *SharedServer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tokenOK(r.Header.Get("Authorization"), s.g.opts.AdminToken) {
			problem(w, 401, "UNAUTHORIZED")
			return
		}
		if r.URL.Path != "/control/v1/state" || r.Method != "POST" {
			problem(w, 404, "NOT_FOUND")
			return
		}
		var q sharedRequest
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&q) != nil {
			problem(w, 400, "INVALID_REQUEST")
			return
		}
		// Discovery performs network I/O before taking the state dispatcher lock.
		if q.Action == "acquire" || q.Action == "reserve" {
			if rt := s.g.current.Load().Routes[q.Route]; rt != nil && rt.AgentType == "hermes" {
				if q.Entry == "a2a" || resourceInterface(q.Creation.Kind) == "a2a" {
					s.g.discoverA2A(r.Context(), rt)
				} else {
					s.g.discoverHermes(r.Context(), rt)
				}
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		key := q.Worker + "/" + q.ID
		sum := sha256.Sum256(rawValue(q))
		fingerprint := hex.EncodeToString(sum[:])
		if old, ok := s.replies[key]; ok {
			if old.fingerprint != fingerprint {
				writeJSON(w, 200, sharedResponse{Error: "OPERATION_CONFLICT"})
				return
			}
			writeJSON(w, 200, old.response)
			return
		}
		var out sharedResponse
		if q.Epoch != "" && q.Epoch != s.epoch {
			out.Error = "STATE_GENERATION_CHANGED"
		} else {
			out = s.execute(q)
		}
		out.Epoch = s.epoch
		if q.Action != "config" && q.Action != "status" && q.Action != "resolve" && q.Action != "list" && q.Action != "lookup" && q.Action != "cache_get" {
			s.replies[key] = cachedReply{fingerprint, out}
			s.order = append(s.order, key)
			if len(s.order) > 8192 {
				delete(s.replies, s.order[0])
				s.order = s.order[1:]
			}
		}
		writeJSON(w, 200, out)
	})
}
func (s *SharedServer) addLease(worker string, release func()) string {
	id := releaseprotocol.ID("lease_")
	s.leases[id] = sharedLease{worker, release}
	return id
}
func (s *SharedServer) execute(q sharedRequest) (out sharedResponse) {
	g := s.g
	d := g.resources
	var err error
	defer func() {
		if err != nil {
			out.Error = err.Error()
		}
	}()
	if !g.healthy.Load() && q.Action != "release" && q.Action != "retire" {
		out.Error = "STATE_NOT_READY"
		return
	}
	rt := g.current.Load().Routes[q.Route]
	switch q.Action {
	case "resolve":
		out.Resolution = g.resolveRollout(q.Request)
	case "request_result":
		g.requests.Add(1)
		if q.Flag {
			g.failures.Add(1)
		}
	case "config":
		c := g.current.Load().Config
		out.Config = &c
	case "freeze":
		g.mu.Lock()
		s.frozen = q.Flag
		g.configFrozen.Store(q.Flag)
		g.mu.Unlock()
	case "status":
		for _, l := range s.leases {
			if q.Worker == "" || l.worker == q.Worker {
				out.Count++
			}
		}
	case "retire":
		for k, l := range s.leases {
			if l.worker == q.Worker {
				l.release()
				delete(s.leases, k)
			}
		}
	case "release":
		if l, ok := s.leases[q.Lease]; ok {
			if l.worker != q.Worker {
				err = errResourceConflict
				break
			}
			l.release()
			delete(s.leases, q.Lease)
			err = g.persistSessions()
		}
	case "acquire":
		if rt == nil || rt.Version != q.Revision {
			err = fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
			break
		}
		var l *lease
		l, err = g.acquire(q.Owner, q.Route, q.Session, q.Endpoint, q.Entry, rt)
		if err == nil {
			if q.Session != "" {
				sh := g.shard(sessionKey{q.Owner, rt.ID, q.Session, q.Entry})
				sh.mu.Lock()
				if b := sh.items[sessionKey{q.Owner, rt.ID, q.Session, q.Entry}]; b != nil && b.WorkerOwner == "" {
					b.WorkerOwner = q.Worker
				}
				sh.mu.Unlock()
			}
			if err = g.persistSessions(); err != nil {
				l.release()
				break
			}
			out.Endpoint = l.ep
			out.Lease = s.addLease(q.Worker, l.release)
		}
	case "reserve":
		if rt == nil || rt.Version != q.Revision {
			err = fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
			break
		}
		out.Reservation, err = g.reserveResource(q.Owner, rt, q.Creation)
		if err == nil {
			err = s.assignWorker([]resource{out.Reservation.Resource}, q.Worker)
		}
	case "lookup":
		out.Resource, err = d.lookup(q.Key)
	case "pin":
		var release func()
		out.Resource, release, err = d.pin(q.Key)
		if err == nil {
			out.Lease = s.addLease(q.Worker, release)
		}
	case "update":
		err = d.update(q.Resource, q.State)
	case "alias":
		err = d.attachAlias(q.Resource, q.Endpoint, q.Flag)
	case "observe":
		var m map[resourceReverseKey]resource
		m, err = d.observeBatch(q.Owner, q.Route, q.Endpoint, q.Observations)
		for k, v := range m {
			out.Mapped = append(out.Mapped, resourcePair{k, v})
		}
		if err == nil {
			var refs []resource
			for _, v := range m {
				refs = append(refs, v)
			}
			err = s.assignWorker(refs, q.Worker)
		}
	case "hermes":
		err = d.observeHermes(q.Resource, q.Execution, q.IDs, q.Session, q.Endpoint, q.State)
	case "public":
		out.ID = d.publicUpstream(q.Owner, q.Route, q.Kind, q.Endpoint, q.Session)
	case "forget":
		err = d.forget(q.Resource)
	case "abandon":
		err = d.abandon(q.Resource)
	case "abandon_prepared":
		err = d.abandonPrepared(q.Resources, q.Flag)
	case "list":
		out.Resources = d.list(q.Owner, q.Route, q.Kind, q.Endpoint)
	case "has_task":
		out.Flag = d.hasTask(q.Owner, q.Route, q.Session)
	case "cache_acquire":
		var release func()
		release, err = g.acquireList(q.Entry)
		if err == nil {
			out.Lease = s.addLease(q.Worker, release)
		}
	case "cache_get":
		out.Cache, out.Flag = g.cacheGet(q.Entry, q.Session)
	case "cache_put":
		out.ID, err = g.cachePut(q.Entry, q.Cache)
	default:
		err = fmt.Errorf("unsupported state operation")
	}
	return
}
func (s *SharedServer) Freeze(frozen bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	s.frozen = frozen
	s.g.configFrozen.Store(frozen)
}
func (s *SharedServer) Retire(worker string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execute(sharedRequest{Action: "retire", Worker: worker})
}
func SharedControl(socket, token, worker, action string, flag bool) (int64, error) {
	c := newSharedClient(socket, token, worker)
	defer c.client.CloseIdleConnections()
	var out sharedResponse
	err := c.call(action, sharedRequest{Flag: flag}, &out)
	return out.Count, err
}
