package runtime

import (
	"fmt"
	"sync"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
)

// restoreSharedLease rebuilds accounting only. It never performs discovery,
// endpoint selection, resource creation, or any other upstream operation.
func (g *Gateway) restoreSharedLease(record shared.LeaseRecord) (func(), error) {
	switch record.Kind {
	case "pin":
		_, release, err := g.resources.Pin(record.Resource)
		return release, err
	case "cache_acquire":
		if record.Entry != "tasks" && record.Entry != "sessions" {
			return nil, fmt.Errorf("invalid list lease")
		}
		return g.listCache(record.Entry).Acquire()
	case "acquire":
		return g.restoreExecutionLease(record)
	default:
		return nil, fmt.Errorf("unknown lease kind")
	}
}

func (g *Gateway) restoreExecutionLease(record shared.LeaseRecord) (func(), error) {
	rt := g.current.Load().Routes[record.Route]
	if rt == nil || rt.Endpoint(record.Endpoint) == nil || record.Owner == "" || (record.Entry != "a2a" && record.Entry != "proxy") {
		return nil, fmt.Errorf("lease references missing route or endpoint")
	}
	var sh *sessionShard
	var b *binding
	if record.Session != "" {
		key := sessionKey{Owner: record.Owner, Route: rt.ID, ID: record.Session, Interface: record.Entry}
		sh = g.shard(key)
		sh.mu.Lock()
		defer sh.mu.Unlock()
		b = sh.items[key]
		if b == nil || b.Endpoint != record.Endpoint {
			return nil, fmt.Errorf("lease references missing or changed session")
		}
	}
	if g.inflight.Add(1) > int64(g.opts.MaxInflight) {
		g.inflight.Add(-1)
		return nil, fmt.Errorf("restored global capacity exceeded")
	}
	caller, count := g.beginCaller(record.Owner)
	if count > int64(g.opts.MaxPerCaller) {
		g.endCaller(record.Owner, caller)
		g.inflight.Add(-1)
		return nil, fmt.Errorf("restored caller capacity exceeded")
	}
	st := g.state(rt.ID, record.Endpoint)
	if st.active.Add(1) > int64(g.opts.MaxPerEndpoint) {
		st.active.Add(-1)
		g.endCaller(record.Owner, caller)
		g.inflight.Add(-1)
		return nil, fmt.Errorf("restored endpoint capacity exceeded")
	}
	iface := st.iface(record.Entry)
	iface.active.Add(1)
	if b != nil {
		b.Active++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			iface.active.Add(-1)
			st.active.Add(-1)
			g.endCaller(record.Owner, caller)
			g.inflight.Add(-1)
			if sh != nil {
				sh.mu.Lock()
				b.Active--
				b.Seen = time.Now()
				sh.mu.Unlock()
			}
		})
	}, nil
}
