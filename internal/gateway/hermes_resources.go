package gateway

// Observe the adapter's known protocol fields atomically, before exposing any
// IDs. Session compression may add lineage aliases; execution identity is fixed.
func (d *resourceDirectory) observeHermes(session, execution resource, sessionIDs []string, activeSession, executionID string, state resourceState) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.items[session.key()]
	if s == nil || s.Generation != session.Generation {
		return errResourceNotFound
	}
	var x *resourceRecord
	if execution.ID != "" {
		x = d.items[execution.key()]
		if x == nil || x.Generation != execution.Generation || x.EndpointID != s.EndpointID || x.ParentID != s.ID {
			return errResourceConflict
		}
		if executionID != "" && x.Upstream != "" && x.Upstream != executionID {
			return errResourceConflict
		}
	}
	add := map[resourceReverseKey]*resourceRecord{}
	for _, id := range append(sessionIDs, activeSession) {
		if id == "" {
			continue
		}
		if !validResourceID(id) {
			return errResourceInvalid
		}
		key := resourceReverseKey{s.Owner, s.RouteID, s.Kind, s.EndpointID, id}
		if previous, ok := d.reverse[key]; ok {
			if previous != s.key() {
				return errResourceConflict
			}
		} else {
			add[key] = s
		}
	}
	if executionID != "" {
		if x == nil || !validResourceID(executionID) {
			return errResourceInvalid
		}
		key := resourceReverseKey{x.Owner, x.RouteID, x.Kind, x.EndpointID, executionID}
		if previous, ok := d.reverse[key]; ok {
			if previous != x.key() {
				return errResourceConflict
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
		return errResourceCapacity
	}
	for key, record := range add {
		d.reverse[key] = record.key()
		record.upstreams = append(record.upstreams, key.ID)
	}
	d.aliasCount += cost
	if activeSession != "" {
		s.Upstream = activeSession
	}
	if s.State == resourceReserved || s.State == resourceUnknown {
		s.State = resourceLive
	}
	if x != nil {
		if executionID != "" {
			x.Upstream = executionID
		}
		if x.State != resourceTerminal && state != "" {
			x.State = state
		}
		d.touchLocked(x, d.now())
	}
	d.touchLocked(s, d.now())
	return nil
}

func (d *resourceDirectory) publicUpstream(owner, route string, kind resourceKind, ep, id string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reverse[resourceReverseKey{owner, route, kind, ep, id}].ID
}

// Only this request's fresh, unsent reservations can be rolled back. Responses
// needs a live synthetic parent before reserving its first child.
func (h *hermesRequest) abandonPrepared() {
	if h.sent {
		return
	}
	d := h.g.resources
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(h.created) - 1; i >= 0; i-- {
		expected := h.created[i]
		r := d.items[expected.key()]
		if r == nil || r.Generation != expected.Generation || r.ActiveRefs > 0 || r.ChildRefs > 0 {
			continue
		}
		if r.State != resourceReserved && !(h.op.action == "response" && r.Kind == hermesSession && r.State == resourceLive) {
			continue
		}
		for key, op := range d.operations {
			if op.Resource == r.key() {
				delete(d.operations, key)
			}
		}
		d.removeLocked(r)
	}
}
