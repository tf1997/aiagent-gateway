// Package resource owns resource identities, aliases, reservations and their
// lifecycle. Mutable records and directory locks stay inside this package.
package resource

import (
	"sort"
	"sync"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

type operationKey struct{ Owner, Route, Operation, Key string }

type resourceRecord struct {
	protocol.Resource
	fingerprint   string
	aliases       []string
	upstreams     []string
	operationRefs int
}

type operationReservation struct {
	Resource    protocol.ResourceKey
	Fingerprint string
	Seen        time.Time
}

type Directory struct {
	remote    *shared.Client
	persist   func() error
	onFailure func()

	mu                                                    sync.Mutex
	items                                                 map[protocol.ResourceKey]*resourceRecord
	reverse                                               map[protocol.ResourceReverseKey]protocol.ResourceKey
	aliases                                               map[protocol.ResourceKey]protocol.ResourceKey
	operations                                            map[operationKey]operationReservation
	counts                                                map[protocol.ResourceKind]int
	max, maxAliases, maxOperations, maxSessions, maxTasks int
	aliasCount                                            int // Client aliases plus additional upstream lineage IDs.
	now                                                   func() time.Time
}

func New(o Limits, remote *shared.Client) *Directory {
	return &Directory{
		remote:        remote,
		items:         map[protocol.ResourceKey]*resourceRecord{},
		reverse:       map[protocol.ResourceReverseKey]protocol.ResourceKey{},
		aliases:       map[protocol.ResourceKey]protocol.ResourceKey{},
		operations:    map[operationKey]operationReservation{},
		counts:        map[protocol.ResourceKind]int{},
		max:           o.MaxResources,
		maxAliases:    o.MaxResourceAliases,
		maxOperations: o.MaxDataIdempotency,
		maxSessions:   o.MaxSessions,
		maxTasks:      o.MaxTasks,
		now:           time.Now,
	}
}

func resourcePrefix(k protocol.ResourceKind) string {
	switch k {
	case protocol.HermesSession:
		return "session_"
	case protocol.HermesRun:
		return "run_"
	case protocol.HermesResponse:
		return "response_"
	case protocol.A2AContext:
		return "ctx_"
	case protocol.A2ATask:
		return "task_"
	}
	return "resource_"
}

func (d *Directory) lookupLocked(k protocol.ResourceKey) *resourceRecord {
	if r := d.items[k]; r != nil {
		return r
	}
	if canonical, ok := d.aliases[k]; ok {
		return d.items[canonical]
	}
	return nil
}

func (d *Directory) touchLocked(r *resourceRecord, now time.Time) {
	for r != nil {
		r.Seen = now
		if r.ParentID == "" {
			return
		}
		r = d.items[r.Parent()]
	}
}

func (d *Directory) Lookup(k protocol.ResourceKey) (protocol.Resource, error) {
	if d.remote != nil {
		var out shared.Response
		err := d.remote.Call("lookup", shared.Request{Key: k}, &out)
		return out.Resource, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.lookupLocked(k)
	if r == nil {
		return protocol.Resource{}, protocol.ErrResourceNotFound
	}
	d.touchLocked(r, d.now())
	return r.Resource, nil
}

func (d *Directory) Pin(k protocol.ResourceKey) (protocol.Resource, func(), error) {
	if d.remote != nil {
		var out shared.Response
		err := d.remote.Call("pin", shared.Request{Key: k}, &out)
		if err != nil {
			return protocol.Resource{}, nil, err
		}
		return out.Resource, d.remote.Release(out.Lease), nil
	}
	d.mu.Lock()
	r := d.lookupLocked(k)
	if r == nil {
		d.mu.Unlock()
		return protocol.Resource{}, nil, protocol.ErrResourceNotFound
	}
	r.ActiveRefs++
	d.touchLocked(r, d.now())
	out := r.Resource
	d.mu.Unlock()
	var once sync.Once
	return out, func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if current := d.items[out.Key()]; current != nil && current.Generation == out.Generation {
				current.ActiveRefs--
				d.touchLocked(current, d.now())
			}
		})
	}, nil
}

func (d *Directory) capacityLocked(add map[protocol.ResourceKind]int) bool {
	n := 0
	for _, v := range add {
		n += v
	}
	return len(d.items)+n <= d.max && d.counts[protocol.HermesSession]+d.counts[protocol.A2AContext]+add[protocol.HermesSession]+add[protocol.A2AContext] <= d.maxSessions && d.counts[protocol.HermesRun]+d.counts[protocol.A2ATask]+add[protocol.HermesRun]+add[protocol.A2ATask] <= d.maxTasks
}

func (d *Directory) insertLocked(r *resourceRecord) {
	d.items[r.Key()] = r
	d.counts[r.Kind]++
	if r.Upstream != "" {
		d.reverse[protocol.ResourceReverseKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, Endpoint: r.EndpointID, ID: r.Upstream}] = r.Key()
		r.upstreams = []string{r.Upstream}
	}
	if r.ParentID != "" {
		d.items[r.Parent()].ChildRefs++
	}
}

func (d *Directory) List(owner, route string, kind protocol.ResourceKind, ep string) []protocol.Resource {
	if d.remote != nil {
		var out shared.Response
		d.remote.Must("list", shared.Request{Owner: owner, Route: route, Kind: kind, Endpoint: ep}, &out)
		return out.Resources
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []protocol.Resource{}
	for _, r := range d.items {
		if (owner == "" || owner == r.Owner) && (route == "" || route == r.RouteID) && (kind == "" || kind == r.Kind) && (ep == "" || ep == r.EndpointID) {
			out = append(out, r.Resource)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID == out[j].ID {
			return out[i].Owner < out[j].Owner
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (d *Directory) References(route, ep string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, r := range d.items {
		if r.RouteID == route && (ep == "" || r.EndpointID == ep) {
			n++
		}
	}
	return n
}

func (d *Directory) Sizes() (resources, aliases, operations int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items), d.aliasCount, len(d.operations)
}

func (d *Directory) HasTask(owner, route, sid string) bool {
	if d.remote != nil {
		var out shared.Response
		d.remote.Must("has_task", shared.Request{Owner: owner, Route: route, Session: sid}, &out)
		return out.Flag
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.items {
		if r.Owner == owner && r.RouteID == route && r.Kind == protocol.A2ATask && r.SessionID == sid && r.State != protocol.ResourceTerminal {
			return true
		}
	}
	return false
}
