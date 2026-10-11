package gateway

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type routeSchedule struct {
	mu      sync.Mutex
	weights map[string]int64
}

type sessionKey struct{ Owner, Route, ID, Interface string }
type binding struct {
	WorkerOwner string
	Endpoint    string
	Seen        time.Time
	TTL         time.Duration
	Active      int
}
type sessionShard struct {
	mu    sync.Mutex
	items map[sessionKey]*binding
}
type interfaceState struct {
	healthy     atomic.Bool
	active      atomic.Int64
	discovery   atomic.Pointer[capabilitySnapshot]
	discoveryMu sync.Mutex
}
type endpointState struct {
	active atomic.Int64
	proxy  interfaceState
	a2a    interfaceState
}

func (s *endpointState) iface(entry string) *interfaceState {
	if entry == "a2a" {
		return &s.a2a
	}
	return &s.proxy
}

type lease struct {
	route   *Route
	ep      *Endpoint
	state   *endpointState
	iface   EndpointInterface
	release func()
}

func (g *Gateway) shard(k sessionKey) *sessionShard {
	h := fnv.New32a()
	h.Write([]byte(k.Owner))
	h.Write([]byte{0})
	h.Write([]byte(k.Route))
	h.Write([]byte{0})
	h.Write([]byte(k.ID))
	return &g.shards[h.Sum32()%256]
}
func (g *Gateway) state(route, id string) *endpointState {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	k := route + "/" + id
	s := g.states[k]
	if s == nil {
		s = &endpointState{}
		s.proxy.healthy.Store(true)
		s.a2a.healthy.Store(true)
		g.states[k] = s
	}
	return s
}
func (g *Gateway) acquire(owner, key, sid, forced, entry string, expected *Route) (*lease, error) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	return g.acquireLocked(owner, key, sid, forced, entry, expected, false)
}

// Caller holds lifecycle.RLock. Never call with the resource directory locked.
func (g *Gateway) acquireLocked(owner, key, sid, forced, entry string, expected *Route, background bool) (*lease, error) {
	if g.remote != nil {
		return g.sharedAcquire(owner, key, sid, forced, entry, expected, background)
	}
	if !g.healthy.Load() {
		return nil, fmt.Errorf("GATEWAY_NOT_READY")
	}
	rt := g.current.Load().Routes[key]
	if rt == nil || !rt.Enabled || !exposes(rt, entry) {
		return nil, fmt.Errorf("ROUTE_UNAVAILABLE")
	}
	if expected != nil && rt != expected {
		return nil, fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
	}
	if g.inflight.Add(1) > int64(g.opts.MaxInflight) {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("CONCURRENCY_LIMIT")
	}
	if !background && !routeConsumer(rt, owner) {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("UNKNOWN_CALLER")
	}
	callerCount, count := g.beginCaller(owner)
	if count > int64(g.opts.MaxPerCaller) {
		g.endCaller(owner, callerCount)
		g.inflight.Add(-1)
		return nil, fmt.Errorf("CALLER_CONCURRENCY_LIMIT")
	}
	success := false
	defer func() {
		if !success {
			g.endCaller(owner, callerCount)
			g.inflight.Add(-1)
		}
	}()
	var sh *sessionShard
	var b *binding
	k := sessionKey{owner, rt.ID, sid, entry}
	if sid != "" {
		sh = g.shard(k)
		sh.mu.Lock()
		defer sh.mu.Unlock()
		b = sh.items[k]
		if b != nil && b.Active == 0 && time.Since(b.Seen) > b.TTL && (entry != "a2a" || !g.hasTask(owner, rt.ID, sid)) {
			delete(sh.items, k)
			g.bindings.Add(-1)
			b = nil
		}
		if b != nil {
			if forced != "" && forced != b.Endpoint {
				return nil, fmt.Errorf("SESSION_TASK_CONFLICT")
			}
			forced = b.Endpoint
		}
	}
	var ep *Endpoint
	if forced != "" {
		ep = endpoint(rt, forced)
		if ep == nil || ep.State == "disabled" || !g.interfaceReady(rt, ep, entry) {
			return nil, fmt.Errorf("SESSION_ENDPOINT_UNAVAILABLE")
		}
	} else {
		ep = g.selectEndpoint(rt, entry)
		if ep == nil {
			return nil, fmt.Errorf("NO_HEALTHY_ENDPOINT")
		}
	}
	st := g.state(rt.ID, ep.ID)
	if st.active.Add(1) > int64(g.opts.MaxPerEndpoint) {
		st.active.Add(-1)
		return nil, fmt.Errorf("ENDPOINT_CONCURRENCY_LIMIT")
	}
	defer func() {
		if !success {
			st.active.Add(-1)
		}
	}()
	if sh != nil && b == nil {
		if g.bindings.Add(1) > int64(g.opts.MaxSessions) {
			g.bindings.Add(-1)
			return nil, fmt.Errorf("SESSION_CAPACITY")
		}
		b = &binding{Endpoint: ep.ID, Seen: time.Now(), TTL: time.Duration(rt.Session.IdleTTLSeconds) * time.Second}
		sh.items[k] = b
	}
	if b != nil {
		b.Active++
		b.Seen = time.Now()
	}
	ist := st.iface(entry)
	ist.active.Add(1)
	success = true
	var once sync.Once
	return &lease{route: rt, ep: ep, state: st, iface: ep.Interfaces[entry], release: func() {
		once.Do(func() {
			ist.active.Add(-1)
			st.active.Add(-1)
			g.endCaller(owner, callerCount)
			g.inflight.Add(-1)
			if sh != nil {
				sh.mu.Lock()
				b.Active--
				b.Seen = time.Now()
				sh.mu.Unlock()
			}
		})
	}}, nil
}
func (g *Gateway) selectEndpoint(r *Route, entry string) *Endpoint {
	g.schedMu.Lock()
	scheduleKey := r.ID + "/" + entry
	schedule := g.sched[scheduleKey]
	if schedule == nil {
		schedule = &routeSchedule{weights: map[string]int64{}}
		g.sched[scheduleKey] = schedule
	}
	g.schedMu.Unlock()
	schedule.mu.Lock()
	defer schedule.mu.Unlock()
	m := schedule.weights
	for id := range m {
		if endpoint(r, id) == nil {
			delete(m, id)
		}
	}
	var best *Endpoint
	var total int64
	for i := range r.Endpoints {
		e := &r.Endpoints[i]
		if e.State != "active" || !g.interfaceReady(r, e, entry) {
			continue
		}
		m[e.ID] += int64(e.Weight)
		total += int64(e.Weight)
		if best == nil || m[e.ID] > m[best.ID] {
			best = e
		}
	}
	if best != nil {
		m[best.ID] -= total
	}
	return best
}
func (g *Gateway) references(route, id string) int {
	n := 0
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.Lock()
		for k, b := range sh.items {
			if k.Route == route && (id == "" || b.Endpoint == id) {
				n++
			}
		}
		sh.mu.Unlock()
	}
	n += g.resources.references(route, id)
	g.statesMu.Lock()
	for k, st := range g.states {
		if (id != "" && k == route+"/"+id) || (id == "" && len(k) > len(route) && k[:len(route)+1] == route+"/") {
			n += int(st.active.Load())
		}
	}
	g.statesMu.Unlock()
	return n
}
func (g *Gateway) hasTask(owner, route, sid string) bool {
	return g.resources.hasTask(owner, route, sid)
}
func (g *Gateway) sessions(route, ep string) []map[string]any {
	out := []map[string]any{}
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.Lock()
		for k, b := range sh.items {
			if k.Route == route && (ep == "" || ep == b.Endpoint) {
				out = append(out, map[string]any{"owner_id": k.Owner, "session_id": k.ID, "endpoint_id": b.Endpoint, "last_seen_at": b.Seen, "active": b.Active, "interface": k.Interface})
			}
		}
		sh.mu.Unlock()
	}
	return out
}
func (g *Gateway) maintenance(ctx context.Context) {
	defer g.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	nextHealth := time.Now()
	shardIndex := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for j := 0; j < 16; j++ {
				sh := &g.shards[shardIndex%256]
				shardIndex++
				sh.mu.Lock()
				for k, b := range sh.items {
					if b.Active == 0 && time.Since(b.Seen) > b.TTL && (k.Interface != "a2a" || !g.hasTask(k.Owner, k.Route, k.ID)) {
						delete(sh.items, k)
						g.bindings.Add(-1)
					}
				}
				sh.mu.Unlock()
			}
			g.cleanupResources()
			_ = g.persistSessions()
			if time.Now().After(nextHealth) {
				g.retireCaches()
				g.checkHealth(ctx)
				nextHealth = time.Now().Add(g.opts.HealthInterval)
			}
		}
	}
}
func (g *Gateway) checkHealth(ctx context.Context) {
	client := &http.Client{Transport: g.transport(3000), Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, r := range g.current.Load().Routes {
		if r.AgentType == "hermes" {
			g.discoverHermes(ctx, r)
			g.discoverA2A(ctx, r)
		}
		for _, ep := range r.Endpoints {
			for entry, iface := range ep.Interfaces {
				if iface.HealthURL == "" || ep.State == "disabled" || !exposes(r, entry) {
					continue
				}
				req, err := http.NewRequestWithContext(ctx, "GET", iface.HealthURL, nil)
				if err != nil {
					continue
				}
				if iface.CredentialRef != "" {
					req.Header.Set("Authorization", "Bearer "+g.opts.Credentials[iface.CredentialRef])
				}
				res, err := client.Do(req)
				ok := err == nil
				if res != nil {
					ok = res.StatusCode >= 200 && res.StatusCode < 300
					res.Body.Close()
				}
				g.lifecycle.RLock()
				latest := g.current.Load().Routes[r.Key]
				if latest != nil && latest.ID == r.ID {
					currentEP := endpoint(latest, ep.ID)
					if currentEP != nil && currentEP.Interfaces[entry] == iface {
						g.state(r.ID, ep.ID).iface(entry).healthy.Store(ok)
					}
				}
				g.lifecycle.RUnlock()
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

// Called under the lifecycle write lock before publishing a new configuration.
func (g *Gateway) resetHealth(old *snapshot, next *Config) {
	for _, rt := range next.Routes {
		previous := old.Routes[rt.Key]
		if previous == nil || previous.ID != rt.ID {
			continue
		}
		for _, ep := range rt.Endpoints {
			before := endpoint(previous, ep.ID)
			if before == nil {
				continue
			}
			for entry, iface := range ep.Interfaces {
				if before.Interfaces[entry] != iface {
					g.state(rt.ID, ep.ID).iface(entry).healthy.Store(true)
				}
			}
		}
	}
}

// Retire configuration-dependent caches without interrupting active requests.
func (g *Gateway) retireCaches() {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	s := g.current.Load()
	live := map[string]bool{}
	routes := map[string]bool{}
	timeouts := map[int]bool{3000: true}
	for _, r := range s.Routes {
		for _, entry := range r.Expose {
			routes[r.ID+"/"+entry] = true
		}
		timeouts[r.Timeout.ResponseHeaderMS] = true
		for _, e := range r.Endpoints {
			live[r.ID+"/"+e.ID] = true
		}
	}
	g.statesMu.Lock()
	for k, st := range g.states {
		if !live[k] && st.active.Load() == 0 {
			delete(g.states, k)
		}
	}
	g.statesMu.Unlock()
	g.schedMu.Lock()
	for id := range g.sched {
		if !routes[id] {
			delete(g.sched, id)
		}
	}
	g.schedMu.Unlock()
	g.transportMu.Lock()
	for ms, t := range g.transports {
		if !timeouts[ms] {
			t.CloseIdleConnections()
			delete(g.transports, ms)
		}
	}
	g.transportMu.Unlock()
}

// Resource creation reserves the instance before any upstream request. Replays
// reuse the reservation even if that instance is now unhealthy or draining;
// acquireResource decides availability without selecting a replacement.
func (g *Gateway) reserveResource(owner string, rt *Route, spec resourceCreation) (creationReservation, error) {
	if g.remote != nil {
		var out sharedResponse
		err := g.remote.call("reserve", sharedRequest{Owner: owner, Route: rt.Key, Revision: rt.Version, Creation: spec}, &out)
		return out.Reservation, err
	}
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !g.healthy.Load() || g.current.Load().Routes[rt.Key] != rt || !rt.Enabled || !exposes(rt, resourceInterface(spec.Kind)) {
		return creationReservation{}, fmt.Errorf("ROUTE_UNAVAILABLE")
	}
	if !routeConsumer(rt, owner) {
		return creationReservation{}, errResourceNotFound
	}
	return g.resources.reserve(owner, rt.ID, spec, func(parent *resource) (string, error) {
		var ep *Endpoint
		if parent != nil {
			if parent.State == resourceReserved || parent.State == resourceUnknown {
				return "", errResourceInUse
			}
			ep = endpoint(rt, parent.EndpointID)
		} else {
			ep = g.selectEndpoint(rt, resourceInterface(spec.Kind))
		}
		newSession := parent == nil || spec.Kind == hermesSession || spec.Kind == a2aContext
		if ep == nil || ep.State == "disabled" || (ep.State == "draining" && newSession) || !g.interfaceReady(rt, ep, resourceInterface(spec.Kind)) {
			return "", fmt.Errorf("NO_HEALTHY_ENDPOINT")
		}
		return ep.ID, nil
	})
}
func (g *Gateway) acquireResource(owner string, rt *Route, kind resourceKind, id string) (*lease, resource, error) {
	return g.acquireOwnedResource(owner, rt, kind, id, false)
}
func (g *Gateway) acquireOwnedResource(owner string, rt *Route, kind resourceKind, id string, background bool) (*lease, resource, error) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !background && !routeConsumer(rt, owner) {
		return nil, resource{}, errResourceNotFound
	}
	res, unpin, err := g.resources.pin(resourceKey{owner, rt.ID, kind, id})
	if err != nil {
		return nil, resource{}, err
	}
	l, err := g.acquireLocked(owner, rt.Key, "", res.EndpointID, res.Interface, rt, background)
	if err != nil {
		unpin()
		return nil, resource{}, err
	}
	release := l.release
	l.release = func() { release(); unpin() }
	return l, res, nil
}
func (g *Gateway) cleanupResources() {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	g.resources.cleanup(func(r resource) bool { return g.state(r.RouteID, r.EndpointID).active.Load() > 0 })
}
