// Package protocol defines the contracts shared by the runtime and adapters.
package protocol

import (
	"context"
	"net/http"
)

// Backend gives protocol handlers access to scheduling and ownership without
// exposing the runtime's locks, counters or storage implementation.
// The runtime supplies these operations once, before accepting requests.
type Backend struct {
	PublicURL          func() string
	Background         func() context.Context
	RegisterBackground func() (done func(), accepted bool)
	RecordFailure      func()
	Transport          func(int) http.RoundTripper
	UpstreamHeaders    func(http.Header, Caller, EndpointInterface)
	DiscoverHermes     func(context.Context, *Route)
	DiscoverA2A        func(context.Context, *Route)

	Acquire              func(owner, key, session, forced, entry string, expected *Route) (*Lease, error)
	AcquireResource      func(owner string, route *Route, kind ResourceKind, id string) (*Lease, Resource, error)
	AcquireOwnedResource func(owner string, route *Route, kind ResourceKind, id string, background bool) (*Lease, Resource, error)
	ReserveResource      func(owner string, route *Route, creation ResourceCreation) (CreationReservation, error)
	Resources            ResourceStore

	AcquireList func(kind string) (func(), error)
	CacheGet    func(kind, key string) (ListSnapshot, bool)
	CachePut    func(kind string, snapshot ListSnapshot) (string, error)
}

// Lease pins one endpoint until Release is called. Capabilities remain a
// diagnostic snapshot and never determine whether a lease can be acquired.
type Lease struct {
	Route        *Route
	Endpoint     *Endpoint
	Interface    EndpointInterface
	Release      func()
	Capabilities func() *Capabilities
}

// ResourceStore preserves the same ownership operations for local and shared
// state. Protocol handlers only receive copies of resource metadata.
type ResourceStore struct {
	Lookup          func(ResourceKey) (Resource, error)
	Pin             func(ResourceKey) (Resource, func(), error)
	List            func(owner, route string, kind ResourceKind, endpoint string) []Resource
	Update          func(Resource, ResourceState) error
	ObserveBatch    func(owner, route, endpoint string, observations []ResourceObservation) (map[ResourceReverseKey]Resource, error)
	ObserveHermes   func(session, execution Resource, sessionIDs []string, activeSession, executionID string, state ResourceState) error
	PublicUpstream  func(owner, route string, kind ResourceKind, endpoint, id string) string
	AbandonPrepared func(created []Resource, response bool) error
}
