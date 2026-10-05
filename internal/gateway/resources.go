package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

type resourceKind string

const (
	hermesSession  resourceKind = "hermes_session"
	hermesRun      resourceKind = "hermes_run"
	hermesResponse resourceKind = "hermes_response"
	a2aContext     resourceKind = "a2a_context"
	a2aTask        resourceKind = "a2a_task"
)

type resourceState string

const (
	resourceReserved resourceState = "reserved"
	resourceLive     resourceState = "live"
	resourceWaiting  resourceState = "waiting"
	resourceUnknown  resourceState = "unknown"
	resourceTerminal resourceState = "terminal"
)

var (
	errResourceNotFound = errors.New("RESOURCE_NOT_FOUND")
	errResourceConflict = errors.New("RESOURCE_CONFLICT")
	errResourceCapacity = errors.New("RESOURCE_CAPACITY")
	errResourceInUse    = errors.New("RESOURCE_IN_USE")
	errResourceInvalid  = errors.New("INVALID_RESOURCE")
)

type resourceKey struct {
	Owner, Route string
	Kind         resourceKind
	ID           string
}
type resourceReverseKey struct {
	Owner, Route string
	Kind         resourceKind
	Endpoint, ID string
}
type operationKey struct{ Owner, Route, Operation, Key string }

// Values returned to adapters are copies. Generation guards late responses and
// releases; only the directory may mutate live records.
type resource struct {
	ID         string        `json:"public_id"`
	Owner      string        `json:"owner_id"`
	RouteID    string        `json:"route_id"`
	Kind       resourceKind  `json:"resource_kind"`
	EndpointID string        `json:"endpoint_id"`
	Interface  string        `json:"interface"`
	Upstream   string        `json:"upstream_id"`
	ParentID   string        `json:"parent_id,omitempty"`
	ParentKind resourceKind  `json:"parent_kind,omitempty"`
	Generation string        `json:"binding_generation"`
	State      resourceState `json:"lifecycle_state"`
	Seen       time.Time     `json:"last_seen_at"`
	ActiveRefs int           `json:"active_refs"`
	ChildRefs  int           `json:"child_refs"`
	SessionID  string        `json:"session_id,omitempty"` // Legacy A2A routing hint.
}

func (r resource) key() resourceKey { return resourceKey{r.Owner, r.RouteID, r.Kind, r.ID} }
func (r resource) parent() resourceKey {
	return resourceKey{r.Owner, r.RouteID, r.ParentKind, r.ParentID}
}

type resourceRecord struct {
	resource
	fingerprint   string
	aliases       []string
	upstreams     []string
	operationRefs int
}
type operationReservation struct {
	Resource    resourceKey
	Fingerprint string
	Seen        time.Time
}
type resourceDirectory struct {
	mu                                                    sync.Mutex
	items                                                 map[resourceKey]*resourceRecord
	reverse                                               map[resourceReverseKey]resourceKey
	aliases                                               map[resourceKey]resourceKey
	operations                                            map[operationKey]operationReservation
	counts                                                map[resourceKind]int
	max, maxAliases, maxOperations, maxSessions, maxTasks int
	aliasCount                                            int // Client aliases plus additional upstream lineage IDs.
	now                                                   func() time.Time
}

func newResourceDirectory(o Options) *resourceDirectory {
	return &resourceDirectory{items: map[resourceKey]*resourceRecord{}, reverse: map[resourceReverseKey]resourceKey{}, aliases: map[resourceKey]resourceKey{}, operations: map[operationKey]operationReservation{}, counts: map[resourceKind]int{}, max: o.MaxResources, maxAliases: o.MaxResourceAliases, maxOperations: o.MaxDataIdempotency, maxSessions: o.MaxSessions, maxTasks: o.MaxTasks, now: time.Now}
}
func resourceInterface(k resourceKind) string {
	switch k {
	case hermesSession, hermesRun, hermesResponse:
		return "proxy"
	case a2aContext, a2aTask:
		return "a2a"
	}
	return ""
}
func resourcePrefix(k resourceKind) string {
	switch k {
	case hermesSession:
		return "session_"
	case hermesRun:
		return "run_"
	case hermesResponse:
		return "response_"
	case a2aContext:
		return "ctx_"
	case a2aTask:
		return "task_"
	}
	return "resource_"
}
func validResourceID(s string) bool {
	return s != "" && len(s) <= 256 && s != "." && s != ".." && !strings.ContainsAny(s, "/\\") && strings.IndexFunc(s, unicode.IsControl) < 0
}
func validUpstreamID(s string) bool {
	return s != "" && len(s) <= 4096 && strings.IndexFunc(s, unicode.IsControl) < 0
}
func (d *resourceDirectory) lookupLocked(k resourceKey) *resourceRecord {
	if r := d.items[k]; r != nil {
		return r
	}
	if canonical, ok := d.aliases[k]; ok {
		return d.items[canonical]
	}
	return nil
}
func (d *resourceDirectory) touchLocked(r *resourceRecord, now time.Time) {
	for r != nil {
		r.Seen = now
		if r.ParentID == "" {
			return
		}
		r = d.items[r.parent()]
	}
}
func (d *resourceDirectory) lookup(k resourceKey) (resource, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.lookupLocked(k)
	if r == nil {
		return resource{}, errResourceNotFound
	}
	d.touchLocked(r, d.now())
	return r.resource, nil
}
func (d *resourceDirectory) pin(k resourceKey) (resource, func(), error) {
	d.mu.Lock()
	r := d.lookupLocked(k)
	if r == nil {
		d.mu.Unlock()
		return resource{}, nil, errResourceNotFound
	}
	r.ActiveRefs++
	d.touchLocked(r, d.now())
	out := r.resource
	d.mu.Unlock()
	var once sync.Once
	return out, func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if current := d.items[out.key()]; current != nil && current.Generation == out.Generation {
				current.ActiveRefs--
				d.touchLocked(current, d.now())
			}
		})
	}, nil
}
func (d *resourceDirectory) capacityLocked(add map[resourceKind]int) bool {
	n := 0
	for _, v := range add {
		n += v
	}
	return len(d.items)+n <= d.max && d.counts[hermesSession]+d.counts[a2aContext]+add[hermesSession]+add[a2aContext] <= d.maxSessions && d.counts[hermesRun]+d.counts[a2aTask]+add[hermesRun]+add[a2aTask] <= d.maxTasks
}
func (d *resourceDirectory) insertLocked(r *resourceRecord) {
	d.items[r.key()] = r
	d.counts[r.Kind]++
	if r.Upstream != "" {
		d.reverse[resourceReverseKey{r.Owner, r.RouteID, r.Kind, r.EndpointID, r.Upstream}] = r.key()
		r.upstreams = []string{r.Upstream}
	}
	if r.ParentID != "" {
		d.items[r.parent()].ChildRefs++
	}
}

type resourceCreation struct {
	Kind           resourceKind
	Alias          string
	ParentKind     resourceKind
	ParentID       string
	Operation      string
	IdempotencyKey string
	Fingerprint    string
}
type creationReservation struct {
	Resource               resource
	First                  bool
	UpstreamIdempotencyKey string
}

// choose is called only for a new resource, under the directory lock and the
// gateway lifecycle read lock. It selects an endpoint; it must not perform I/O.
func (d *resourceDirectory) reserve(owner, route string, s resourceCreation, choose func(*resource) (string, error)) (creationReservation, error) {
	if resourceInterface(s.Kind) == "" || (s.Alias != "" && !validResourceID(s.Alias)) || s.Fingerprint == "" || s.Operation == "" || len(s.IdempotencyKey) > 256 || strings.IndexFunc(s.IdempotencyKey, unicode.IsControl) >= 0 {
		return creationReservation{}, errResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	operation := resourceInterface(s.Kind) + "/" + s.Operation
	op := operationKey{owner, route, operation, s.IdempotencyKey}
	var existing *resourceRecord
	if s.IdempotencyKey != "" {
		if old, ok := d.operations[op]; ok {
			if old.Fingerprint != s.Fingerprint {
				return creationReservation{}, errResourceConflict
			}
			existing = d.items[old.Resource]
			if existing == nil {
				return creationReservation{}, errResourceNotFound
			}
		}
	}
	if s.Alias != "" {
		if r := d.lookupLocked(resourceKey{owner, route, s.Kind, s.Alias}); r != nil {
			if existing != nil && existing != r {
				return creationReservation{}, errResourceConflict
			}
			existing = r
		}
	}
	var parent *resourceRecord
	if s.ParentID != "" {
		parent = d.lookupLocked(resourceKey{owner, route, s.ParentKind, s.ParentID})
		if parent == nil {
			return creationReservation{}, errResourceNotFound
		}
		if parent.Interface != resourceInterface(s.Kind) {
			return creationReservation{}, errResourceConflict
		}
	}
	if existing != nil && (existing.Kind != s.Kind || existing.fingerprint != s.Fingerprint || (parent == nil && existing.ParentID != "") || (parent != nil && existing.parent() != parent.key())) {
		return creationReservation{}, errResourceConflict
	}
	_, knownOp := d.operations[op]
	if s.IdempotencyKey != "" && !knownOp && len(d.operations) >= d.maxOperations {
		return creationReservation{}, errResourceCapacity
	}
	aliasKey := resourceKey{owner, route, s.Kind, s.Alias}
	addAlias := s.Alias != "" && d.lookupLocked(aliasKey) == nil
	if addAlias && d.aliasCount >= d.maxAliases {
		return creationReservation{}, errResourceCapacity
	}
	first := existing == nil
	if first {
		if !d.capacityLocked(map[resourceKind]int{s.Kind: 1}) {
			return creationReservation{}, errResourceCapacity
		}
		var pv *resource
		if parent != nil {
			copy := parent.resource
			pv = &copy
		}
		ep, err := choose(pv)
		if err != nil {
			return creationReservation{}, err
		}
		if ep == "" || (parent != nil && ep != parent.EndpointID) {
			return creationReservation{}, errResourceConflict
		}
		r := resource{ID: newID(resourcePrefix(s.Kind)), Owner: owner, RouteID: route, Kind: s.Kind, EndpointID: ep, Interface: resourceInterface(s.Kind), Generation: newID("gen_"), State: resourceReserved, Seen: now}
		if s.Kind == hermesSession {
			r.Upstream = newID("hgs_")
		}
		if parent != nil {
			r.ParentID = parent.ID
			r.ParentKind = parent.Kind
		}
		existing = &resourceRecord{resource: r, fingerprint: s.Fingerprint}
		d.insertLocked(existing)
	}
	if addAlias {
		d.aliases[aliasKey] = existing.key()
		existing.aliases = append(existing.aliases, s.Alias)
		d.aliasCount++
	}
	if s.IdempotencyKey != "" {
		if !knownOp {
			existing.operationRefs++
		}
		d.operations[op] = operationReservation{existing.key(), s.Fingerprint, now}
	}
	d.touchLocked(existing, now)
	out := creationReservation{Resource: existing.resource, First: first}
	if s.IdempotencyKey != "" {
		out.UpstreamIdempotencyKey = namespacedIdempotency(owner, route, operation, s.IdempotencyKey)
	}
	return out, nil
}
func namespacedIdempotency(owner, route, operation, key string) string {
	raw, _ := json.Marshal([]string{owner, route, operation, key})
	sum := sha256.Sum256(raw)
	return "gw_" + hex.EncodeToString(sum[:])
}

// Adapters explicitly name the headers affecting a request's semantics.
func dataFingerprint(method, path string, body []byte, h http.Header, headerNames ...string) string {
	headers := map[string][]string{}
	for _, name := range headerNames {
		headers[http.CanonicalHeaderKey(name)] = h.Values(name)
	}
	raw, _ := json.Marshal(struct {
		Method, Path string
		Body         []byte
		Headers      map[string][]string
	}{method, path, body, headers})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func validResourceState(s resourceState) bool {
	return s == resourceReserved || s == resourceLive || s == resourceWaiting || s == resourceUnknown || s == resourceTerminal
}
func (d *resourceDirectory) update(r resource, state resourceState) error {
	if !validResourceState(state) {
		return errResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.items[r.key()]
	if current == nil || current.Generation != r.Generation {
		return errResourceNotFound
	}
	if state == resourceReserved && current.State != resourceReserved {
		return errResourceConflict
	}
	// A late observation must not resurrect a terminal execution.
	if current.State != resourceTerminal {
		current.State = state
	}
	d.touchLocked(current, d.now())
	return nil
}

// attachAlias preserves old upstream lineage IDs and may promote a new active
// upstream ID. Client aliases never cross caller/route/kind boundaries.
func (d *resourceDirectory) attachAlias(r resource, alias string, upstream bool) error {
	if (upstream && !validUpstreamID(alias)) || (!upstream && !validResourceID(alias)) {
		return errResourceInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.items[r.key()]
	if current == nil || current.Generation != r.Generation {
		return errResourceNotFound
	}
	add := false
	rk := resourceReverseKey{current.Owner, current.RouteID, current.Kind, current.EndpointID, alias}
	ak := resourceKey{r.Owner, r.RouteID, r.Kind, alias}
	if upstream {
		if key, ok := d.reverse[rk]; ok {
			if key != r.key() {
				return errResourceConflict
			}
		} else {
			add = true
		}
	} else {
		if other := d.lookupLocked(ak); other != nil {
			if other != current {
				return errResourceConflict
			}
		} else {
			add = true
		}
	}
	// The first upstream ID occupies the resource's primary reverse slot.
	cost := add && (!upstream || len(current.upstreams) > 0)
	if cost && d.aliasCount >= d.maxAliases {
		return errResourceCapacity
	}
	if add {
		if upstream {
			d.reverse[rk] = r.key()
			current.upstreams = append(current.upstreams, alias)
		} else {
			d.aliases[ak] = r.key()
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

type resourceObservation struct {
	Kind                                resourceKind
	Upstream, ParentUpstream, SessionID string
	State                               resourceState
	ReferenceOnly                       bool
}

// observeBatch stages every record and parent link before committing. Capacity,
// conflicts or dangling references cannot leave half an event registered.
func (d *resourceDirectory) observeBatch(owner, route, ep string, obs []resourceObservation) (map[resourceReverseKey]resource, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	staged := map[resourceKey]*resourceRecord{}
	rev := map[resourceReverseKey]resourceKey{}
	add := map[resourceKind]int{}
	now := d.now()
	resolve := func(rk resourceReverseKey) *resourceRecord {
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
		if resourceInterface(o.Kind) == "" || !validUpstreamID(o.Upstream) || (!o.ReferenceOnly && o.State != "" && !validResourceState(o.State)) {
			return nil, errResourceInvalid
		}
		if o.ReferenceOnly {
			if o.ParentUpstream != "" {
				return nil, errResourceInvalid
			}
			continue
		}
		rk := resourceReverseKey{owner, route, o.Kind, ep, o.Upstream}
		r := resolve(rk)
		if r == nil {
			r = &resourceRecord{resource: resource{ID: newID(resourcePrefix(o.Kind)), Owner: owner, RouteID: route, Kind: o.Kind, EndpointID: ep, Interface: resourceInterface(o.Kind), Upstream: o.Upstream, Generation: newID("gen_"), State: o.State, Seen: now, SessionID: o.SessionID}}
			if r.State == "" {
				r.State = resourceUnknown
			}
			add[o.Kind]++
		} else {
			copy := *r
			r = &copy
			if r.SessionID != o.SessionID {
				return nil, errResourceConflict
			}
			if r.State != resourceTerminal && o.State != "" {
				r.State = o.State
			}
			r.Seen = now
		}
		staged[r.key()] = r
		rev[rk] = r.key()
	}
	if !d.capacityLocked(add) {
		return nil, errResourceCapacity
	}
	for _, o := range obs {
		rk := resourceReverseKey{owner, route, o.Kind, ep, o.Upstream}
		r := resolve(rk)
		if r == nil {
			return nil, errResourceNotFound
		}
		if o.ParentUpstream != "" {
			// Protocol mapping currently supplies A2A task -> context parent links.
			if o.Kind != a2aTask {
				return nil, errResourceInvalid
			}
			parent := resolve(resourceReverseKey{owner, route, a2aContext, ep, o.ParentUpstream})
			if parent == nil {
				return nil, errResourceNotFound
			}
			if r.ParentID != "" && r.parent() != parent.key() {
				return nil, errResourceConflict
			}
			r.ParentID = parent.ID
			r.ParentKind = parent.Kind
		}
	}
	// Publish records before applying parent deltas; parent and child may both
	// be new, and map iteration order is arbitrary.
	deltas := map[resourceKey]int{}
	for key, r := range staged {
		old := d.items[key]
		if r.ParentID != "" && (old == nil || old.ParentID == "") {
			deltas[r.parent()]++
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
	out := map[resourceReverseKey]resource{}
	for _, o := range obs {
		rk := resourceReverseKey{owner, route, o.Kind, ep, o.Upstream}
		r := d.items[d.reverse[rk]]
		d.touchLocked(r, now)
		out[rk] = r.resource
	}
	return out, nil
}
func (d *resourceDirectory) removeLocked(r *resourceRecord) {
	delete(d.items, r.key())
	d.counts[r.Kind]--
	for _, alias := range r.aliases {
		delete(d.aliases, resourceKey{r.Owner, r.RouteID, r.Kind, alias})
		d.aliasCount--
	}
	for _, up := range r.upstreams {
		delete(d.reverse, resourceReverseKey{r.Owner, r.RouteID, r.Kind, r.EndpointID, up})
	}
	if len(r.upstreams) > 1 {
		d.aliasCount -= len(r.upstreams) - 1
	}
	if r.ParentID != "" {
		if parent := d.items[r.parent()]; parent != nil {
			parent.ChildRefs--
		}
	}
}
func (d *resourceDirectory) forget(r resource) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.items[r.key()]
	if current == nil || current.Generation != r.Generation {
		return errResourceNotFound
	}
	if current.ActiveRefs > 0 || current.ChildRefs > 0 || current.operationRefs > 0 || current.State == resourceUnknown || current.State == resourceWaiting || ((current.Kind == a2aTask || current.Kind == hermesRun) && current.State != resourceTerminal) {
		return errResourceInUse
	}
	d.removeLocked(current)
	return nil
}

// Abandon is only for a reservation known not to have been sent. Adapters mark
// it unknown before dispatch; after that, uncertain outcomes retain ownership.
func (d *resourceDirectory) abandon(r resource) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.items[r.key()]
	if current == nil || current.Generation != r.Generation {
		return errResourceNotFound
	}
	if current.State != resourceReserved || current.ActiveRefs > 0 || current.ChildRefs > 0 {
		return errResourceInUse
	}
	for key, op := range d.operations {
		if op.Resource == r.key() {
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
	if r.State == resourceReserved || r.State == resourceUnknown || r.State == resourceWaiting {
		return false
	}
	return (r.Kind != a2aTask && r.Kind != hermesRun) || r.State == resourceTerminal
}
func (d *resourceDirectory) cleanup(busy func(resource) bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	ttl := 24 * time.Hour
	for key, op := range d.operations {
		r := d.items[op.Resource]
		if r != nil && now.Sub(op.Seen) > ttl && r.State != resourceReserved && r.State != resourceUnknown && r.State != resourceWaiting && (r.Kind != hermesRun && r.Kind != a2aTask || r.State == resourceTerminal) && r.ActiveRefs == 0 {
			r.operationRefs--
			delete(d.operations, key)
		}
	}
	queue := make([]resourceKey, 0, len(d.items))
	for key := range d.items {
		queue = append(queue, key)
	}
	for len(queue) > 0 {
		key := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		r := d.items[key]
		if r == nil || !resourceExpires(r) || now.Sub(r.Seen) <= ttl || busy(r.resource) {
			continue
		}
		parent := r.parent()
		d.removeLocked(r)
		if parent.ID != "" {
			queue = append(queue, parent)
		}
	}
}
func (d *resourceDirectory) list(owner, route string, kind resourceKind, ep string) []resource {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []resource{}
	for _, r := range d.items {
		if (owner == "" || owner == r.Owner) && (route == "" || route == r.RouteID) && (kind == "" || kind == r.Kind) && (ep == "" || ep == r.EndpointID) {
			out = append(out, r.resource)
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
func (d *resourceDirectory) references(route, ep string) int {
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
func (d *resourceDirectory) sizes() (resources, aliases, operations int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items), d.aliasCount, len(d.operations)
}
func (d *resourceDirectory) hasTask(owner, route, sid string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.items {
		if r.Owner == owner && r.RouteID == route && r.Kind == a2aTask && r.SessionID == sid && r.State != resourceTerminal {
			return true
		}
	}
	return false
}
