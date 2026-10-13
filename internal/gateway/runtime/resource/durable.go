package resource

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/state"
)

type durableResource struct {
	Resource    protocol.Resource `json:"resource"`
	Fingerprint string            `json:"fingerprint"`
	Aliases     []string          `json:"aliases"`
	Upstreams   []string          `json:"upstreams"`
}

type durableOperation struct {
	Key   operationKey
	Value operationReservation
}

func stateKey(v any) string {
	raw, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func rawValue(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

// EnableDurability restores resource ownership before the shared service starts.
// Runtime connection references are rebuilt by workers, never restored.
func (d *Directory) EnableDurability(j *state.Journal, validEndpoint func(route, endpoint string) bool) error {
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
		if json.Unmarshal(raw, &x) != nil || !protocol.ValidResourceID(x.Resource.ID) || !validResourceState(x.Resource.State) {
			return fmt.Errorf("invalid durable resource")
		}
		r := x.Resource
		r.ActiveRefs = 0
		r.ChildRefs = 0
		if !validEndpoint(r.RouteID, r.EndpointID) {
			return fmt.Errorf("durable resource references missing endpoint")
		}
		d.items[r.Key()] = &resourceRecord{Resource: r, fingerprint: x.Fingerprint, aliases: x.Aliases, upstreams: x.Upstreams}
	}
	for _, r := range d.items {
		d.counts[r.Kind]++
		if r.ParentID != "" {
			p := d.items[r.Parent()]
			if p == nil || p.EndpointID != r.EndpointID {
				return fmt.Errorf("invalid durable parent")
			}
			p.ChildRefs++
		}
		for _, a := range r.aliases {
			k := protocol.ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, ID: a}
			if _, ok := d.aliases[k]; ok {
				return fmt.Errorf("duplicate resource alias")
			}
			d.aliases[k] = r.Key()
			d.aliasCount++
		}
		for i, u := range r.upstreams {
			k := protocol.ResourceReverseKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, Endpoint: r.EndpointID, ID: u}
			if _, ok := d.reverse[k]; ok {
				return fmt.Errorf("duplicate resource reverse key")
			}
			d.reverse[k] = r.Key()
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
	}
	d.persist = func() error {
		resources := map[string]json.RawMessage{}
		operations := map[string]json.RawMessage{}
		for k, r := range d.items {
			copy := r.Resource
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

func (d *Directory) commit(err *error) {
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
