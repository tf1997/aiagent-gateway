package resource

import (
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

func validResourceState(s protocol.ResourceState) bool {
	return s == protocol.ResourceReserved || s == protocol.ResourceLive || s == protocol.ResourceWaiting || s == protocol.ResourceUnknown || s == protocol.ResourceTerminal
}

func (d *Directory) Update(r protocol.Resource, state protocol.ResourceState) (err error) {
	if d.remote != nil {
		return d.remote.Call("update", shared.Request{Resource: r, State: state}, nil)
	}
	if !validResourceState(state) {
		return protocol.ErrResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	current := d.items[r.Key()]
	if current == nil || current.Generation != r.Generation {
		return protocol.ErrResourceNotFound
	}
	if state == protocol.ResourceReserved && current.State != protocol.ResourceReserved {
		return protocol.ErrResourceConflict
	}
	// A late observation must not resurrect a terminal execution.
	if current.State != protocol.ResourceTerminal {
		current.State = state
	}
	d.touchLocked(current, d.now())
	return nil
}

func (d *Directory) removeLocked(r *resourceRecord) {
	delete(d.items, r.Key())
	d.counts[r.Kind]--
	for _, alias := range r.aliases {
		delete(d.aliases, protocol.ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, ID: alias})
		d.aliasCount--
	}
	for _, up := range r.upstreams {
		delete(d.reverse, protocol.ResourceReverseKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, Endpoint: r.EndpointID, ID: up})
	}
	if len(r.upstreams) > 1 {
		d.aliasCount -= len(r.upstreams) - 1
	}
	if r.ParentID != "" {
		if parent := d.items[r.Parent()]; parent != nil {
			parent.ChildRefs--
		}
	}
}

func (d *Directory) Forget(r protocol.Resource) (err error) {
	if d.remote != nil {
		return d.remote.Call("forget", shared.Request{Resource: r}, nil)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	current := d.items[r.Key()]
	if current == nil || current.Generation != r.Generation {
		return protocol.ErrResourceNotFound
	}
	if current.ActiveRefs > 0 || current.ChildRefs > 0 || current.operationRefs > 0 || current.State == protocol.ResourceUnknown || current.State == protocol.ResourceWaiting || ((current.Kind == protocol.A2ATask || current.Kind == protocol.HermesRun) && current.State != protocol.ResourceTerminal) {
		return protocol.ErrResourceInUse
	}
	d.removeLocked(current)
	return nil
}

// Abandon is only for a reservation known not to have been sent. Adapters mark
// it unknown before dispatch; after that, uncertain outcomes retain ownership.
func (d *Directory) Abandon(r protocol.Resource) (err error) {
	if d.remote != nil {
		return d.remote.Call("abandon", shared.Request{Resource: r}, nil)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	current := d.items[r.Key()]
	if current == nil || current.Generation != r.Generation {
		return protocol.ErrResourceNotFound
	}
	if current.State != protocol.ResourceReserved || current.ActiveRefs > 0 || current.ChildRefs > 0 {
		return protocol.ErrResourceInUse
	}
	for key, op := range d.operations {
		if op.Resource == r.Key() {
			delete(d.operations, key)
		}
	}
	d.removeLocked(current)
	return nil
}

func resourceExpires(r *resourceRecord) bool {
	if r.ActiveRefs > 0 || r.ChildRefs > 0 || r.operationRefs > 0 {
		return false
	}
	if r.State == protocol.ResourceReserved || r.State == protocol.ResourceUnknown || r.State == protocol.ResourceWaiting {
		return false
	}
	return (r.Kind != protocol.A2ATask && r.Kind != protocol.HermesRun) || r.State == protocol.ResourceTerminal
}

func (d *Directory) Cleanup(busy func(protocol.Resource) bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var err error
	defer d.commit(&err)
	now := d.now()
	ttl := 24 * time.Hour
	for key, op := range d.operations {
		r := d.items[op.Resource]
		if r != nil && now.Sub(op.Seen) > ttl && r.State != protocol.ResourceReserved && r.State != protocol.ResourceUnknown && r.State != protocol.ResourceWaiting && (r.Kind != protocol.HermesRun && r.Kind != protocol.A2ATask || r.State == protocol.ResourceTerminal) && r.ActiveRefs == 0 {
			r.operationRefs--
			delete(d.operations, key)
		}
	}
	queue := make([]protocol.ResourceKey, 0, len(d.items))
	for key := range d.items {
		queue = append(queue, key)
	}
	for len(queue) > 0 {
		key := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		r := d.items[key]
		if r == nil || !resourceExpires(r) || now.Sub(r.Seen) <= ttl || busy(r.Resource) {
			continue
		}
		parent := r.Parent()
		d.removeLocked(r)
		if parent.ID != "" {
			queue = append(queue, parent)
		}
	}
}
