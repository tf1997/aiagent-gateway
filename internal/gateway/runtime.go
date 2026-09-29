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

type sessionKey struct{ Owner, Route, ID string }
type binding struct {
	Endpoint string
	Seen     time.Time
	TTL      time.Duration
	Active   int
}
type sessionShard struct {
	mu    sync.Mutex
	items map[sessionKey]*binding
}
type endpointState struct {
	healthy atomic.Bool
	active  atomic.Int64
}
type lease struct {
	route   *Route
	ep      *Endpoint
	state   *endpointState
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
		s.healthy.Store(true)
		g.states[k] = s
	}
	return s
}
func (g *Gateway) acquire(owner, key, sid, forced string) (*lease, error) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !g.healthy.Load() {
		return nil, fmt.Errorf("GATEWAY_NOT_READY")
	}
	rt := g.current.Load().Routes[key]
	if rt == nil || !rt.Enabled {
		return nil, fmt.Errorf("ROUTE_UNAVAILABLE")
	}
	if g.inflight.Add(1) > int64(g.opts.MaxInflight) {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("CONCURRENCY_LIMIT")
	}
	callerCount := g.callerCounts[owner]
	if callerCount == nil {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("UNKNOWN_CALLER")
	}
	if callerCount.Add(1) > int64(g.opts.MaxPerCaller) {
		callerCount.Add(-1)
		g.inflight.Add(-1)
		return nil, fmt.Errorf("CALLER_CONCURRENCY_LIMIT")
	}
	success := false
	defer func() {
		if !success {
			callerCount.Add(-1)
			g.inflight.Add(-1)
		}
	}()
	var sh *sessionShard
	var b *binding
	k := sessionKey{owner, rt.ID, sid}
	if sid != "" {
		sh = g.shard(k)
		sh.mu.Lock()
		defer sh.mu.Unlock()
		b = sh.items[k]
		if b != nil && b.Active == 0 && time.Since(b.Seen) > b.TTL && !g.hasTask(owner, rt.ID, sid) {
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
		if ep == nil || ep.State == "disabled" || !g.state(rt.ID, ep.ID).healthy.Load() {
			return nil, fmt.Errorf("SESSION_ENDPOINT_UNAVAILABLE")
		}
	} else {
		ep = g.selectEndpoint(rt)
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
	success = true
	var once sync.Once
	return &lease{route: rt, ep: ep, state: st, release: func() {
		once.Do(func() {
			st.active.Add(-1)
			callerCount.Add(-1)
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
func (g *Gateway) selectEndpoint(r *Route) *Endpoint {
	g.schedMu.Lock()
	schedule := g.sched[r.ID]
	if schedule == nil {
		schedule = &routeSchedule{weights: map[string]int64{}}
		g.sched[r.ID] = schedule
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
		if e.State != "active" || !g.state(r.ID, e.ID).healthy.Load() {
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
	g.a2aMu.Lock()
	for _, t := range g.tasks {
		if t.RouteID == route && (id == "" || t.EndpointID == id) {
			n++
		}
	}
	for _, c := range g.contexts {
		if c.RouteID == route && (id == "" || c.EndpointID == id) {
			n++
		}
	}
	g.a2aMu.Unlock()
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
	g.a2aMu.Lock()
	defer g.a2aMu.Unlock()
	for _, t := range g.tasks {
		if t.Owner == owner && t.RouteID == route && t.SessionID == sid && !t.Terminal {
			return true
		}
	}
	return false
}
func (g *Gateway) sessions(route, ep string) []map[string]any {
	out := []map[string]any{}
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.Lock()
		for k, b := range sh.items {
			if k.Route == route && (ep == "" || ep == b.Endpoint) {
				out = append(out, map[string]any{"owner_id": k.Owner, "session_id": k.ID, "endpoint_id": b.Endpoint, "last_seen_at": b.Seen, "active": b.Active})
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
					if b.Active == 0 && time.Since(b.Seen) > b.TTL && !g.hasTask(k.Owner, k.Route, k.ID) {
						delete(sh.items, k)
						g.bindings.Add(-1)
					}
				}
				sh.mu.Unlock()
			}
			g.cleanupA2A()
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
		for _, ep := range r.Endpoints {
			if ep.HealthURL == "" || ep.State == "disabled" {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, "GET", ep.HealthURL, nil)
			if err != nil {
				continue
			}
			res, err := client.Do(req)
			ok := err == nil
			if res != nil {
				ok = res.StatusCode >= 200 && res.StatusCode < 400
				res.Body.Close()
			}
			g.lifecycle.RLock()
			latest := g.current.Load().Routes[r.Key]
			if latest != nil {
				currentEP := endpoint(latest, ep.ID)
				if currentEP != nil && currentEP.URL == ep.URL && currentEP.HealthURL == ep.HealthURL {
					g.state(r.ID, ep.ID).healthy.Store(ok)
				}
			}
			g.lifecycle.RUnlock()
			if ctx.Err() != nil {
				return
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
		routes[r.ID] = true
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
