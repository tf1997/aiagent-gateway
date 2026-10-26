package runtime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
)

type routeSchedule struct {
	mu      sync.Mutex
	weights map[string]int64
}

type lease struct {
	route   *protocol.Route
	ep      *protocol.Endpoint
	state   *endpointState
	iface   protocol.EndpointInterface
	release func()
}

func (g *Gateway) acquire(owner, key, sid, forced, entry string, expected *protocol.Route) (*lease, error) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	return g.acquireLocked(owner, key, sid, forced, entry, expected, false)
}

// Caller holds lifecycle.RLock. Never call with the resource directory locked.
func (g *Gateway) acquireLocked(owner, key, sid, forced, entry string, expected *protocol.Route, background bool) (*lease, error) {
	if g.remote != nil {
		return g.sharedAcquire(owner, key, sid, forced, entry, expected, background)
	}
	if !g.healthy.Load() {
		return nil, fmt.Errorf("GATEWAY_NOT_READY")
	}
	rt := g.current.Load().Routes[key]
	if rt == nil || !rt.Enabled || !protocol.Exposes(rt, entry) {
		return nil, fmt.Errorf("ROUTE_UNAVAILABLE")
	}
	if expected != nil && rt != expected {
		return nil, fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
	}
	if g.inflight.Add(1) > int64(g.opts.MaxInflight) {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("CONCURRENCY_LIMIT")
	}
	if !background && !configuration.HasConsumer(rt, owner) {
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
		defer func() { g.recordSessionLocked(k, sh.items[k]) }()
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
	var ep *protocol.Endpoint
	if forced != "" {
		ep = rt.Endpoint(forced)
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
				g.recordSessionLocked(k, b)
				sh.mu.Unlock()
			}
		})
	}}, nil
}

func (g *Gateway) selectEndpoint(r *protocol.Route, entry string) *protocol.Endpoint {
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
	defer func() { g.recorder.Put("schedule/"+scheduleKey, schedule.weights) }()
	m := schedule.weights
	for id := range m {
		if r.Endpoint(id) == nil {
			delete(m, id)
		}
	}
	var best *protocol.Endpoint
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
	n += g.resources.References(route, id)
	g.statesMu.Lock()
	for k, st := range g.states {
		if (id != "" && k == route+"/"+id) || (id == "" && len(k) > len(route) && k[:len(route)+1] == route+"/") {
			n += int(st.active.Load())
		}
	}
	g.statesMu.Unlock()
	return n
}

// Counter entries exist only while requests are active. Token rotation keeps
// the same owner; removing a consumer cannot reset an in-flight concurrency limit.
func (g *Gateway) beginCaller(owner string) (*atomic.Int64, int64) {
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	c := g.callerCounts[owner]
	if c == nil {
		c = &atomic.Int64{}
		g.callerCounts[owner] = c
	}
	return c, c.Add(1)
}

func (g *Gateway) endCaller(owner string, c *atomic.Int64) {
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	if c.Add(-1) == 0 {
		delete(g.callerCounts, owner)
	}
}
