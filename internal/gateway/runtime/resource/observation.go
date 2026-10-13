package resource

import (
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

// attachAlias preserves old upstream lineage IDs and may promote a new active
// upstream ID. Client aliases never cross caller/route/kind boundaries.
func (d *Directory) AttachAlias(r protocol.Resource, alias string, upstream bool) (err error) {
	if d.remote != nil {
		return d.remote.Call("alias", shared.Request{Resource: r, Endpoint: alias, Flag: upstream}, nil)
	}
	if (upstream && !protocol.ValidUpstreamID(alias)) || (!upstream && !protocol.ValidResourceID(alias)) {
		return protocol.ErrResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	current := d.items[r.Key()]
	if current == nil || current.Generation != r.Generation {
		return protocol.ErrResourceNotFound
	}
	add := false
	rk := protocol.ResourceReverseKey{Owner: current.Owner, Route: current.RouteID, Kind: current.Kind, Endpoint: current.EndpointID, ID: alias}
	ak := protocol.ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, ID: alias}
	if upstream {
		if key, ok := d.reverse[rk]; ok {
			if key != r.Key() {
				return protocol.ErrResourceConflict
			}
		} else {
			add = true
		}
	} else {
		if other := d.lookupLocked(ak); other != nil {
			if other != current {
				return protocol.ErrResourceConflict
			}
		} else {
			add = true
		}
	}
	// The first upstream ID occupies the resource's primary reverse slot.
	cost := add && (!upstream || len(current.upstreams) > 0)
	if cost && d.aliasCount >= d.maxAliases {
		return protocol.ErrResourceCapacity
	}
	if add {
		if upstream {
			d.reverse[rk] = r.Key()
			current.upstreams = append(current.upstreams, alias)
		} else {
			d.aliases[ak] = r.Key()
			current.aliases = append(current.aliases, alias)
		}
	}
	if cost {
		d.aliasCount++
	}
	if upstream {
		current.Upstream = alias
	}
	d.touchLocked(current, d.now())
	return nil
}

// observeBatch stages every record and parent link before committing. Capacity,
// conflicts or dangling references cannot leave half an event registered.
func (d *Directory) ObserveBatch(owner, route, ep string, obs []protocol.ResourceObservation) (result map[protocol.ResourceReverseKey]protocol.Resource, err error) {
	if d.remote != nil {
		var out shared.Response
		err = d.remote.Call("observe", shared.Request{Owner: owner, Route: route, Endpoint: ep, Observations: obs}, &out)
		m := map[protocol.ResourceReverseKey]protocol.Resource{}
		for _, p := range out.Mapped {
			m[p.Key] = p.Value
		}
		return m, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	staged := map[protocol.ResourceKey]*resourceRecord{}
	rev := map[protocol.ResourceReverseKey]protocol.ResourceKey{}
	add := map[protocol.ResourceKind]int{}
	now := d.now()
	resolve := func(rk protocol.ResourceReverseKey) *resourceRecord {
		key, ok := rev[rk]
		if !ok {
			key = d.reverse[rk]
		}
		if r := staged[key]; r != nil {
			return r
		}
		return d.items[key]
	}
	for _, o := range obs {
		if protocol.ResourceInterface(o.Kind) == "" || !protocol.ValidUpstreamID(o.Upstream) || (!o.ReferenceOnly && o.State != "" && !validResourceState(o.State)) {
			return nil, protocol.ErrResourceInvalid
		}
		if o.ReferenceOnly {
			if o.ParentUpstream != "" {
				return nil, protocol.ErrResourceInvalid
			}
			continue
		}
		rk := protocol.ResourceReverseKey{Owner: owner, Route: route, Kind: o.Kind, Endpoint: ep, ID: o.Upstream}
		r := resolve(rk)
		if r == nil {
			r = &resourceRecord{Resource: protocol.Resource{ID: protocol.NewID(resourcePrefix(o.Kind)), Owner: owner, RouteID: route, Kind: o.Kind, EndpointID: ep, Interface: protocol.ResourceInterface(o.Kind), Upstream: o.Upstream, Generation: protocol.NewID("gen_"), State: o.State, Seen: now, SessionID: o.SessionID}}
			if r.State == "" {
				r.State = protocol.ResourceUnknown
			}
			add[o.Kind]++
		} else {
			copy := *r
			r = &copy
			if r.SessionID != o.SessionID {
				return nil, protocol.ErrResourceConflict
			}
			if r.State != protocol.ResourceTerminal && o.State != "" {
				r.State = o.State
			}
			r.Seen = now
		}
		staged[r.Key()] = r
		rev[rk] = r.Key()
	}
	if !d.capacityLocked(add) {
		return nil, protocol.ErrResourceCapacity
	}
	for _, o := range obs {
		rk := protocol.ResourceReverseKey{Owner: owner, Route: route, Kind: o.Kind, Endpoint: ep, ID: o.Upstream}
		r := resolve(rk)
		if r == nil {
			return nil, protocol.ErrResourceNotFound
		}
		if o.ParentUpstream != "" {
			// Protocol mapping currently supplies A2A task -> context parent links.
			if o.Kind != protocol.A2ATask {
				return nil, protocol.ErrResourceInvalid
			}
			parent := resolve(protocol.ResourceReverseKey{Owner: owner, Route: route, Kind: protocol.A2AContext, Endpoint: ep, ID: o.ParentUpstream})
			if parent == nil {
				return nil, protocol.ErrResourceNotFound
			}
			if r.ParentID != "" && r.Parent() != parent.Key() {
				return nil, protocol.ErrResourceConflict
			}
			r.ParentID = parent.ID
			r.ParentKind = parent.Kind
		}
	}
	// Publish records before applying parent deltas; parent and child may both
	// be new, and map iteration order is arbitrary.
	deltas := map[protocol.ResourceKey]int{}
	for key, r := range staged {
		old := d.items[key]
		if r.ParentID != "" && (old == nil || old.ParentID == "") {
			deltas[r.Parent()]++
		}
		if old == nil {
			d.counts[r.Kind]++
			r.upstreams = []string{r.Upstream}
		}
		d.items[key] = r
	}
	for rk, key := range rev {
		d.reverse[rk] = key
	}
	for parent, n := range deltas {
		d.items[parent].ChildRefs += n
	}
	out := map[protocol.ResourceReverseKey]protocol.Resource{}
	for _, o := range obs {
		rk := protocol.ResourceReverseKey{Owner: owner, Route: route, Kind: o.Kind, Endpoint: ep, ID: o.Upstream}
		r := d.items[d.reverse[rk]]
		d.touchLocked(r, now)
		out[rk] = r.Resource
	}
	return out, nil
}
