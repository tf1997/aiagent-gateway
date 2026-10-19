package runtime

import (
	"context"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

// Reconcile a bounded, rotating set independently of health checks. Temporary
// failures retain ownership; a missing upstream task is not proof of completion.
func (g *Gateway) reconcileLoop(ctx context.Context) {
	defer g.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refs := g.resources.List("", "", "", "")
			if len(refs) == 0 {
				continue
			}
			for n := 0; n < min(16, len(refs)); n++ {
				ref := refs[offset%len(refs)]
				offset++
				if ref.ActiveRefs > 0 || ref.State == protocol.ResourceTerminal || ref.Upstream == "" || (ref.Kind == protocol.HermesSession && ref.State != protocol.ResourceUnknown) || ref.Kind == protocol.A2AContext {
					continue
				}
				probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				g.reconcileResource(probeCtx, ref)
				cancel()
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func (g *Gateway) reconcileResource(ctx context.Context, ref protocol.Resource) {
	leave, err := g.stateBarrier.TryEnter()
	if err != nil {
		return
	}
	defer leave()
	if ctx.Err() != nil {
		return
	}
	var rt *protocol.Route
	for _, r := range g.current.Load().Routes {
		if r.ID == ref.RouteID {
			rt = r
			break
		}
	}
	if rt == nil || !rt.Enabled {
		return
	}
	// Revoking access does not cancel upstream work; continue observing known
	// resources using the stored owner, without re-enabling consumer access.
	caller := protocol.Caller{ID: ref.Owner, Reconcile: true}
	if ref.Kind == protocol.A2ATask {
		value, err := g.a2aHandler.FetchTask(ctx, caller, rt, ref, map[string]any{})
		if err != nil || value == nil {
			g.resources.Update(ref, protocol.ResourceUnknown)
		}
		return
	}
	if rt.AgentType != "hermes" {
		return
	}
	g.hermesHandler.Reconcile(ctx, caller, rt, ref)
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
			g.maintainState(&shardIndex)
			if time.Now().After(nextHealth) {
				g.retireCaches()
				g.checkHealth(ctx)
				nextHealth = time.Now().Add(g.opts.HealthInterval)
			}
		}
	}
}

// Admission covers the complete maintenance batch, including journal writes.
// A skipped tick does not advance its cursor or extend any resource TTL.
func (g *Gateway) maintainState(shardIndex *int) {
	leave, err := g.stateBarrier.TryEnter()
	if err != nil {
		return
	}
	defer leave()
	for j := 0; j < 16; j++ {
		sh := &g.shards[*shardIndex%256]
		*shardIndex++
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
}

// Retire configuration-dependent caches without interrupting active requests.
func (g *Gateway) retireCaches() {
	leave, err := g.stateBarrier.TryEnter()
	if err != nil {
		return
	}
	defer leave()
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

func (g *Gateway) cleanupResources() {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	g.resources.Cleanup(func(r protocol.Resource) bool { return g.state(r.RouteID, r.EndpointID).active.Load() > 0 })
}
