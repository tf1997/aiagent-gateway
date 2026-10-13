package runtime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"aiagent-gateway/internal/state"
)

type durableSession struct {
	WorkerOwner string
	Key         sessionKey
	Endpoint    string
	Seen        time.Time
	TTL         time.Duration
}

func stateKey(v any) string {
	raw, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func rawValue(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

// EnableDurability is called before the shared service accepts requests.
func (g *Gateway) EnableDurability(j *state.Journal) error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	if err := g.resources.EnableDurability(j, func(route, id string) bool {
		rt := g.current.Load().RoutesByID(route)
		return rt != nil && rt.Endpoint(id) != nil
	}); err != nil {
		return err
	}
	for key, raw := range j.Snapshot() {
		if !strings.HasPrefix(strings.TrimPrefix(key, "runtime/"), "session/") {
			continue
		}
		var x durableSession
		if json.Unmarshal(raw, &x) != nil || x.TTL <= 0 {
			return fmt.Errorf("invalid durable session")
		}
		rt := g.current.Load().RoutesByID(x.Key.Route)
		if rt == nil || rt.Endpoint(x.Endpoint) == nil {
			return fmt.Errorf("durable session references missing endpoint")
		}
		sh := g.shard(x.Key)
		sh.mu.Lock()
		sh.items[x.Key] = &binding{Endpoint: x.Endpoint, Seen: x.Seen, TTL: x.TTL, WorkerOwner: x.WorkerOwner}
		sh.mu.Unlock()
		g.bindings.Add(1)
	}
	g.journal = j
	return nil
}

func (g *Gateway) persistSessions() error {
	if g.journal == nil {
		return nil
	}
	g.sessionPersist.Lock()
	defer g.sessionPersist.Unlock()
	values := map[string]json.RawMessage{}
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.Lock()
		for k, b := range sh.items {
			values["session/"+stateKey(k)] = rawValue(durableSession{b.WorkerOwner, k, b.Endpoint, b.Seen, b.TTL})
		}
		sh.mu.Unlock()
	}
	if err := g.journal.Replace("session/", values); err != nil {
		g.healthy.Store(false)
		return err
	}
	return nil
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

func (g *Gateway) shard(k sessionKey) *sessionShard {
	h := fnv.New32a()
	h.Write([]byte(k.Owner))
	h.Write([]byte{0})
	h.Write([]byte(k.Route))
	h.Write([]byte{0})
	h.Write([]byte(k.ID))
	return &g.shards[h.Sum32()%256]
}

func (g *Gateway) hasTask(owner, route, sid string) bool {
	return g.resources.HasTask(owner, route, sid)
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

func (g *Gateway) sessionOwner(owner, route, session, entry string) string {
	key := sessionKey{owner, route, session, entry}
	sh := g.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if b := sh.items[key]; b != nil {
		return b.WorkerOwner
	}
	return ""
}
