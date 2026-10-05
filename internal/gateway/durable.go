package gateway

import (
	"aiagent-gateway/internal/state"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type durableResource struct {
	Resource    resource `json:"resource"`
	Fingerprint string   `json:"fingerprint"`
	Aliases     []string `json:"aliases"`
	Upstreams   []string `json:"upstreams"`
}
type durableOperation struct {
	Key   operationKey
	Value operationReservation
}
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

// EnableDurability is called before the shared service accepts requests. Runtime
// connection references are rebuilt by workers, never restored from disk.
func (g *Gateway) EnableDurability(j *state.Journal) error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	d := g.resources
	d.mu.Lock()
	defer d.mu.Unlock()
	all := map[string]json.RawMessage{}
	for k, v := range j.Snapshot() {
		all[strings.TrimPrefix(k, "runtime/")] = v
	}
	for key, raw := range all {
		if !strings.HasPrefix(key, "resource/") {
			continue
		}
		var x durableResource
		if json.Unmarshal(raw, &x) != nil || !validResourceID(x.Resource.ID) || !validResourceState(x.Resource.State) {
			return fmt.Errorf("invalid durable resource")
		}
		r := x.Resource
		r.ActiveRefs = 0
		r.ChildRefs = 0
		rt := g.current.Load().RoutesByID(r.RouteID)
		if rt == nil || endpoint(rt, r.EndpointID) == nil {
			return fmt.Errorf("durable resource references missing endpoint")
		}
		d.items[r.key()] = &resourceRecord{resource: r, fingerprint: x.Fingerprint, aliases: x.Aliases, upstreams: x.Upstreams}
	}
	for _, r := range d.items {
		d.counts[r.Kind]++
		if r.ParentID != "" {
			p := d.items[r.parent()]
			if p == nil || p.EndpointID != r.EndpointID {
				return fmt.Errorf("invalid durable parent")
			}
			p.ChildRefs++
		}
		for _, a := range r.aliases {
			k := resourceKey{r.Owner, r.RouteID, r.Kind, a}
			if _, ok := d.aliases[k]; ok {
				return fmt.Errorf("duplicate resource alias")
			}
			d.aliases[k] = r.key()
			d.aliasCount++
		}
		for i, u := range r.upstreams {
			k := resourceReverseKey{r.Owner, r.RouteID, r.Kind, r.EndpointID, u}
			if _, ok := d.reverse[k]; ok {
				return fmt.Errorf("duplicate resource reverse key")
			}
			d.reverse[k] = r.key()
			if i > 0 {
				d.aliasCount++
			}
		}
	}
	for key, raw := range all {
		if strings.HasPrefix(key, "operation/") {
			var x durableOperation
			if json.Unmarshal(raw, &x) != nil {
				return fmt.Errorf("invalid durable operation")
			}
			r := d.items[x.Value.Resource]
			if r == nil {
				return fmt.Errorf("orphan durable operation")
			}
			d.operations[x.Key] = x.Value
			r.operationRefs++
		}
		if strings.HasPrefix(key, "session/") {
			var x durableSession
			if json.Unmarshal(raw, &x) != nil || x.TTL <= 0 {
				return fmt.Errorf("invalid durable session")
			}
			rt := g.current.Load().RoutesByID(x.Key.Route)
			if rt == nil || endpoint(rt, x.Endpoint) == nil {
				return fmt.Errorf("durable session references missing endpoint")
			}
			sh := g.shard(x.Key)
			sh.mu.Lock()
			sh.items[x.Key] = &binding{Endpoint: x.Endpoint, Seen: x.Seen, TTL: x.TTL, WorkerOwner: x.WorkerOwner}
			sh.mu.Unlock()
			g.bindings.Add(1)
		}
	}
	g.journal = j
	d.persist = func() error {
		resources := map[string]json.RawMessage{}
		operations := map[string]json.RawMessage{}
		for k, r := range d.items {
			copy := r.resource
			copy.ActiveRefs = 0
			copy.ChildRefs = 0
			copy.Seen = copy.Seen.Truncate(time.Minute).Add(time.Minute)
			resources["resource/"+stateKey(k)] = rawValue(durableResource{copy, r.fingerprint, r.aliases, r.upstreams})
		}
		for k, v := range d.operations {
			v.Seen = v.Seen.Truncate(time.Minute).Add(time.Minute)
			operations["operation/"+stateKey(k)] = rawValue(durableOperation{k, v})
		}
		// Resource and operation changes must share one journal transaction.
		values := map[string]json.RawMessage{}
		for k, v := range resources {
			values["runtime/"+k] = v
		}
		for k, v := range operations {
			values["runtime/"+k] = v
		}
		return j.Replace("runtime/", values)
	}
	return nil
}
func (s *snapshot) RoutesByID(id string) *Route {
	for _, r := range s.Routes {
		if r.ID == id {
			return r
		}
	}
	return nil
}
func (d *resourceDirectory) commit(err *error) {
	if d.persist == nil || *err != nil {
		return
	}
	if e := d.persist(); e != nil {
		*err = fmt.Errorf("STATE_PERSISTENCE_FAILED: %w", e)
		if d.onFailure != nil {
			d.onFailure()
		}
	}
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
