package resource

import (
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

// Observe the adapter's known protocol fields atomically, before exposing any
// IDs. Session compression may add lineage aliases; execution identity is fixed.
func (d *Directory) ObserveHermes(session, execution protocol.Resource, sessionIDs []string, activeSession, executionID string, state protocol.ResourceState) (err error) {
	if d.remote != nil {
		return d.remote.Call("hermes", shared.Request{Resource: session, Execution: execution, IDs: sessionIDs, Session: activeSession, Endpoint: executionID, State: state}, nil)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	s := d.items[session.Key()]
	if s == nil || s.Generation != session.Generation {
		return protocol.ErrResourceNotFound
	}
	var x *resourceRecord
	if execution.ID != "" {
		x = d.items[execution.Key()]
		if x == nil || x.Generation != execution.Generation || x.EndpointID != s.EndpointID || x.ParentID != s.ID {
			return protocol.ErrResourceConflict
		}
		if executionID != "" && x.Upstream != "" && x.Upstream != executionID {
			return protocol.ErrResourceConflict
		}
	}
	add := map[protocol.ResourceReverseKey]*resourceRecord{}
	for _, id := range append(sessionIDs, activeSession) {
		if id == "" {
			continue
		}
		if !protocol.ValidResourceID(id) {
			return protocol.ErrResourceInvalid
		}
		key := protocol.ResourceReverseKey{Owner: s.Owner, Route: s.RouteID, Kind: s.Kind, Endpoint: s.EndpointID, ID: id}
		if previous, ok := d.reverse[key]; ok {
			if previous != s.Key() {
				return protocol.ErrResourceConflict
			}
		} else {
			add[key] = s
		}
	}
	if executionID != "" {
		if x == nil || !protocol.ValidResourceID(executionID) {
			return protocol.ErrResourceInvalid
		}
		key := protocol.ResourceReverseKey{Owner: x.Owner, Route: x.RouteID, Kind: x.Kind, Endpoint: x.EndpointID, ID: executionID}
		if previous, ok := d.reverse[key]; ok {
			if previous != x.Key() {
				return protocol.ErrResourceConflict
			}
		} else {
			add[key] = x
		}
	}
	cost := len(add)
	// An execution's first upstream ID uses its primary reverse slot.
	if x != nil && x.Upstream == "" && executionID != "" {
		cost--
	}
	if d.aliasCount+cost > d.maxAliases {
		return protocol.ErrResourceCapacity
	}
	for key, record := range add {
		d.reverse[key] = record.Key()
		record.upstreams = append(record.upstreams, key.ID)
	}
	d.aliasCount += cost
	if activeSession != "" {
		s.Upstream = activeSession
	}
	if s.State == protocol.ResourceReserved || s.State == protocol.ResourceUnknown {
		s.State = protocol.ResourceLive
	}
	if x != nil {
		if executionID != "" {
			x.Upstream = executionID
		}
		if x.State != protocol.ResourceTerminal && state != "" {
			x.State = state
		}
		d.touchLocked(x, d.now())
	}
	d.touchLocked(s, d.now())
	return nil
}

func (d *Directory) PublicUpstream(owner, route string, kind protocol.ResourceKind, ep, id string) string {
	if d.remote != nil {
		var out shared.Response
		d.remote.Must("public", shared.Request{Owner: owner, Route: route, Kind: kind, Endpoint: ep, Session: id}, &out)
		return out.ID
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reverse[protocol.ResourceReverseKey{Owner: owner, Route: route, Kind: kind, Endpoint: ep, ID: id}].ID
}

func (d *Directory) AbandonPrepared(created []protocol.Resource, response bool) (err error) {
	if d.remote != nil {
		return d.remote.Call("abandon_prepared", shared.Request{Resources: created, Flag: response}, nil)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	for i := len(created) - 1; i >= 0; i-- {
		expected := created[i]
		r := d.items[expected.Key()]
		if r == nil || r.Generation != expected.Generation || r.ActiveRefs > 0 || r.ChildRefs > 0 {
			continue
		}
		if r.State != protocol.ResourceReserved && !(response && r.Kind == protocol.HermesSession && r.State == protocol.ResourceLive) {
			continue
		}
		for key, op := range d.operations {
			if op.Resource == r.Key() {
				delete(d.operations, key)
			}
		}
		d.removeLocked(r)
	}
	return nil
}
