package resource

import (
	"cmp"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"aiagent-gateway/internal/gateway/protocol"
)

// Image is an exact live image, distinct from the rounded, inactive records
// used for cold recovery. Export requires the owner's admission barrier.
type Image struct {
	Resources  []ResourceImage  `json:"resources"`
	Operations []OperationImage `json:"operations"`
}

type ResourceImage struct {
	Resource      protocol.Resource `json:"resource"`
	Fingerprint   string            `json:"fingerprint"`
	Aliases       []string          `json:"aliases"`
	Upstreams     []string          `json:"upstreams"`
	OperationRefs int               `json:"operation_refs"`
}

type OperationImage struct {
	Owner, Route, Operation, Key string
	Resource                     protocol.ResourceKey
	Fingerprint                  string
	Seen                         time.Time
}

func (d *Directory) ExportImage() Image {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := Image{Resources: []ResourceImage{}, Operations: []OperationImage{}}
	for _, r := range d.items {
		x := ResourceImage{r.Resource, r.fingerprint, append([]string{}, r.aliases...), append([]string{}, r.upstreams...), r.operationRefs}
		sort.Strings(x.Aliases)
		sort.Strings(x.Upstreams)
		v.Resources = append(v.Resources, x)
	}
	for k, op := range d.operations {
		v.Operations = append(v.Operations, OperationImage{k.Owner, k.Route, k.Operation, k.Key, op.Resource, op.Fingerprint, op.Seen})
	}
	sort.Slice(v.Resources, func(i, j int) bool {
		a, b := v.Resources[i].Resource, v.Resources[j].Resource
		return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.RouteID, b.RouteID), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.ID, b.ID)) < 0
	})
	sort.Slice(v.Operations, func(i, j int) bool {
		a, b := v.Operations[i], v.Operations[j]
		return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Route, b.Route), cmp.Compare(a.Operation, b.Operation), cmp.Compare(a.Key, b.Key)) < 0
	})
	return v
}

// RestoreImage builds isolated indexes without opening storage, touching TTLs,
// expiring entries, or calling an upstream. Failure discards the entire view.
func RestoreImage(v Image, limits Limits, validEndpoint func(route, endpoint string) bool) (*Directory, error) {
	if limits.MaxResources <= 0 || limits.MaxResourceAliases <= 0 || limits.MaxDataIdempotency <= 0 || limits.MaxSessions <= 0 || limits.MaxTasks <= 0 || validEndpoint == nil {
		return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_LIMITS")
	}
	if len(v.Resources) > limits.MaxResources || len(v.Operations) > limits.MaxDataIdempotency {
		return nil, protocol.ErrResourceCapacity
	}
	d := New(limits, nil)
	for _, x := range v.Resources {
		r := x.Resource
		if !protocol.ValidResourceID(r.ID) || r.Owner == "" || r.Generation == "" || r.Seen.IsZero() || !validResourceState(r.State) || protocol.ResourceInterface(r.Kind) == "" || r.Interface != protocol.ResourceInterface(r.Kind) || r.ActiveRefs < 0 || r.ChildRefs < 0 || x.OperationRefs < 0 || d.items[r.Key()] != nil || !validEndpoint(r.RouteID, r.EndpointID) {
			return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_RECORD")
		}
		if (r.ParentID == "") != (r.ParentKind == "") {
			return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_PARENT")
		}
		d.items[r.Key()] = &resourceRecord{Resource: r, fingerprint: x.Fingerprint, aliases: append([]string{}, x.Aliases...), upstreams: append([]string{}, x.Upstreams...)}
		d.counts[r.Kind]++
	}
	if !d.capacityLocked(nil) {
		return nil, protocol.ErrResourceCapacity
	}
	children := map[protocol.ResourceKey]int{}
	for _, r := range d.items {
		if r.ParentID != "" {
			p := d.items[r.Parent()]
			if p == nil || p == r || p.EndpointID != r.EndpointID || p.Interface != r.Interface {
				return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_PARENT")
			}
			children[p.Key()]++
		}
		for _, alias := range r.aliases {
			k := protocol.ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, ID: alias}
			if !protocol.ValidResourceID(alias) || d.items[k] != nil || d.aliases[k].ID != "" {
				return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_ALIAS")
			}
			d.aliases[k] = r.Key()
			d.aliasCount++
		}
		found := r.Upstream == "" && len(r.upstreams) == 0
		for i, upstream := range r.upstreams {
			k := protocol.ResourceReverseKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, Endpoint: r.EndpointID, ID: upstream}
			if !protocol.ValidUpstreamID(upstream) || d.reverse[k].ID != "" {
				return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_UPSTREAM")
			}
			d.reverse[k] = r.Key()
			found = found || upstream == r.Upstream
			if i > 0 {
				d.aliasCount++
			}
		}
		if !found {
			return nil, fmt.Errorf("RESOURCE_IMAGE_MISSING_ACTIVE_UPSTREAM")
		}
		if d.aliasCount > limits.MaxResourceAliases {
			return nil, protocol.ErrResourceCapacity
		}
	}
	// A linear walk detects cycles even in a malicious, very deep parent chain.
	visited := map[protocol.ResourceKey]uint8{}
	for key := range d.items {
		var path []protocol.ResourceKey
		for k := key; k.ID != "" && visited[k] != 2; k = d.items[k].Parent() {
			if visited[k] == 1 {
				return nil, fmt.Errorf("RESOURCE_IMAGE_PARENT_CYCLE")
			}
			visited[k] = 1
			path = append(path, k)
		}
		for _, k := range path {
			visited[k] = 2
		}
	}
	for _, x := range v.Operations {
		k := operationKey{x.Owner, x.Route, x.Operation, x.Key}
		r := d.items[x.Resource]
		_, duplicate := d.operations[k]
		if r == nil || duplicate || x.Owner != r.Owner || x.Route != r.RouteID || !strings.HasPrefix(x.Operation, r.Interface+"/") || len(x.Operation) <= len(r.Interface)+1 || x.Key == "" || len(x.Key) > 256 || strings.IndexFunc(x.Key, unicode.IsControl) >= 0 || x.Fingerprint == "" || x.Fingerprint != r.fingerprint || x.Seen.IsZero() {
			return nil, fmt.Errorf("INVALID_RESOURCE_IMAGE_OPERATION")
		}
		d.operations[k] = operationReservation{x.Resource, x.Fingerprint, x.Seen}
		r.operationRefs++
	}
	for _, x := range v.Resources {
		r := d.items[x.Resource.Key()]
		if r.ChildRefs != children[r.Key()] || r.operationRefs != x.OperationRefs {
			return nil, fmt.Errorf("RESOURCE_IMAGE_REFERENCE_MISMATCH")
		}
	}
	return d, nil
}

// Peek resolves public IDs and aliases without extending their lifetime.
func (d *Directory) Peek(k protocol.ResourceKey) (protocol.Resource, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.lookupLocked(k)
	if r == nil {
		return protocol.Resource{}, false
	}
	return r.Resource, true
}
