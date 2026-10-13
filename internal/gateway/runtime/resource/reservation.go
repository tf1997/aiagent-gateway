package resource

import (
	"strings"
	"unicode"

	"aiagent-gateway/internal/gateway/protocol"
)

// choose is called only for a new resource, under the directory lock and the
// gateway lifecycle read lock. It selects an endpoint; it must not perform I/O.
func (d *Directory) Reserve(owner, route string, s protocol.ResourceCreation, choose func(*protocol.Resource) (string, error)) (result protocol.CreationReservation, err error) {
	if protocol.ResourceInterface(s.Kind) == "" || (s.Alias != "" && !protocol.ValidResourceID(s.Alias)) || s.Fingerprint == "" || s.Operation == "" || len(s.IdempotencyKey) > 256 || strings.IndexFunc(s.IdempotencyKey, unicode.IsControl) >= 0 {
		return protocol.CreationReservation{}, protocol.ErrResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer d.commit(&err)
	now := d.now()
	operation := protocol.ResourceInterface(s.Kind) + "/" + s.Operation
	op := operationKey{owner, route, operation, s.IdempotencyKey}
	var existing *resourceRecord
	if s.IdempotencyKey != "" {
		if old, ok := d.operations[op]; ok {
			if old.Fingerprint != s.Fingerprint {
				return protocol.CreationReservation{}, protocol.ErrResourceConflict
			}
			existing = d.items[old.Resource]
			if existing == nil {
				return protocol.CreationReservation{}, protocol.ErrResourceNotFound
			}
		}
	}
	if s.Alias != "" {
		if r := d.lookupLocked(protocol.ResourceKey{Owner: owner, Route: route, Kind: s.Kind, ID: s.Alias}); r != nil {
			if existing != nil && existing != r {
				return protocol.CreationReservation{}, protocol.ErrResourceConflict
			}
			existing = r
		}
	}
	var parent *resourceRecord
	if s.ParentID != "" {
		parent = d.lookupLocked(protocol.ResourceKey{Owner: owner, Route: route, Kind: s.ParentKind, ID: s.ParentID})
		if parent == nil {
			return protocol.CreationReservation{}, protocol.ErrResourceNotFound
		}
		if parent.Interface != protocol.ResourceInterface(s.Kind) {
			return protocol.CreationReservation{}, protocol.ErrResourceConflict
		}
	}
	if existing != nil && (existing.Kind != s.Kind || existing.fingerprint != s.Fingerprint || (parent == nil && existing.ParentID != "") || (parent != nil && existing.Parent() != parent.Key())) {
		return protocol.CreationReservation{}, protocol.ErrResourceConflict
	}
	_, knownOp := d.operations[op]
	if s.IdempotencyKey != "" && !knownOp && len(d.operations) >= d.maxOperations {
		return protocol.CreationReservation{}, protocol.ErrResourceCapacity
	}
	aliasKey := protocol.ResourceKey{Owner: owner, Route: route, Kind: s.Kind, ID: s.Alias}
	addAlias := s.Alias != "" && d.lookupLocked(aliasKey) == nil
	if addAlias && d.aliasCount >= d.maxAliases {
		return protocol.CreationReservation{}, protocol.ErrResourceCapacity
	}
	first := existing == nil
	if first {
		if !d.capacityLocked(map[protocol.ResourceKind]int{s.Kind: 1}) {
			return protocol.CreationReservation{}, protocol.ErrResourceCapacity
		}
		var pv *protocol.Resource
		if parent != nil {
			copy := parent.Resource
			pv = &copy
		}
		ep, err := choose(pv)
		if err != nil {
			return protocol.CreationReservation{}, err
		}
		if ep == "" || (parent != nil && ep != parent.EndpointID) {
			return protocol.CreationReservation{}, protocol.ErrResourceConflict
		}
		r := protocol.Resource{ID: protocol.NewID(resourcePrefix(s.Kind)), Owner: owner, RouteID: route, Kind: s.Kind, EndpointID: ep, Interface: protocol.ResourceInterface(s.Kind), Generation: protocol.NewID("gen_"), State: protocol.ResourceReserved, Seen: now}
		if s.Kind == protocol.HermesSession {
			r.Upstream = protocol.NewID("hgs_")
		}
		if parent != nil {
			r.ParentID = parent.ID
			r.ParentKind = parent.Kind
		}
		existing = &resourceRecord{Resource: r, fingerprint: s.Fingerprint}
		d.insertLocked(existing)
	}
	if addAlias {
		d.aliases[aliasKey] = existing.Key()
		existing.aliases = append(existing.aliases, s.Alias)
		d.aliasCount++
	}
	if s.IdempotencyKey != "" {
		if !knownOp {
			existing.operationRefs++
		}
		d.operations[op] = operationReservation{existing.Key(), s.Fingerprint, now}
	}
	d.touchLocked(existing, now)
	out := protocol.CreationReservation{Resource: existing.Resource, First: first}
	if s.IdempotencyKey != "" {
		out.UpstreamIdempotencyKey = protocol.NamespacedIdempotency(owner, route, operation, s.IdempotencyKey)
	}
	return out, nil
}
