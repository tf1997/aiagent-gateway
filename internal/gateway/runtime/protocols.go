package runtime

import (
	"context"
	"fmt"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/hermes"
	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

// The runtime owns mutable state; adapters operate through this shared set of
// scheduling, resource and transport operations.
func (g *Gateway) initProtocols() {
	backend := &protocol.Backend{
		PublicURL:          func() string { return g.opts.PublicURL },
		Background:         func() context.Context { return g.background },
		RegisterBackground: g.registerBackground,
		RecordFailure:      func() { g.failures.Add(1) },
		Transport:          g.transport,
		UpstreamHeaders:    g.upstreamHeaders,
		DiscoverHermes:     g.discoverHermes,
		DiscoverA2A:        g.discoverA2A,
		Acquire: func(owner, key, session, forced, entry string, expected *protocol.Route) (*protocol.Lease, error) {
			l, err := g.acquire(owner, key, session, forced, entry, expected)
			return protocolLease(l, entry), err
		},
		AcquireResource: func(owner string, route *protocol.Route, kind protocol.ResourceKind, id string) (*protocol.Lease, protocol.Resource, error) {
			l, ref, err := g.acquireResource(owner, route, kind, id)
			return protocolLease(l, protocol.ResourceInterface(kind)), ref, err
		},
		AcquireOwnedResource: func(owner string, route *protocol.Route, kind protocol.ResourceKind, id string, background bool) (*protocol.Lease, protocol.Resource, error) {
			l, ref, err := g.acquireOwnedResource(owner, route, kind, id, background)
			return protocolLease(l, protocol.ResourceInterface(kind)), ref, err
		},
		ReserveResource: g.reserveResource,
		Resources: protocol.ResourceStore{
			Lookup:          g.resources.Lookup,
			Pin:             g.resources.Pin,
			List:            g.resources.List,
			Update:          g.resources.Update,
			ObserveBatch:    g.resources.ObserveBatch,
			ObserveHermes:   g.resources.ObserveHermes,
			PublicUpstream:  g.resources.PublicUpstream,
			AbandonPrepared: g.resources.AbandonPrepared,
		},
		AcquireList: g.acquireList,
		CacheGet:    g.cacheGet,
		CachePut:    g.cachePut,
	}
	g.a2aHandler = a2a.New(backend)
	g.hermesHandler = hermes.New(backend)
}

func protocolLease(l *lease, entry string) *protocol.Lease {
	if l == nil {
		return nil
	}
	return &protocol.Lease{
		Route: l.route, Endpoint: l.ep, Interface: l.iface, Release: l.release,
		Capabilities: func() *protocol.Capabilities { return l.state.iface(entry).discovery.Load() },
	}
}

func (g *Gateway) registerBackground() (func(), bool) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !g.healthy.Load() {
		return nil, false
	}
	g.wg.Add(1)
	return g.wg.Done, true
}

// Resource creation reserves the instance before any upstream request. Replays
// reuse the reservation even if that instance is now unhealthy or draining;
// acquireResource decides availability without selecting a replacement.
func (g *Gateway) reserveResource(owner string, rt *protocol.Route, spec protocol.ResourceCreation) (protocol.CreationReservation, error) {
	if g.remote != nil {
		var out shared.Response
		err := g.remote.Call("reserve", shared.Request{Owner: owner, Route: rt.Key, Revision: rt.Version, Creation: spec}, &out)
		return out.Reservation, err
	}
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !g.healthy.Load() || g.current.Load().Routes[rt.Key] != rt || !rt.Enabled || !protocol.Exposes(rt, protocol.ResourceInterface(spec.Kind)) {
		return protocol.CreationReservation{}, fmt.Errorf("ROUTE_UNAVAILABLE")
	}
	if !configuration.HasConsumer(rt, owner) {
		return protocol.CreationReservation{}, protocol.ErrResourceNotFound
	}
	return g.resources.Reserve(owner, rt.ID, spec, func(parent *protocol.Resource) (string, error) {
		var ep *protocol.Endpoint
		if parent != nil {
			if parent.State == protocol.ResourceReserved || parent.State == protocol.ResourceUnknown {
				return "", protocol.ErrResourceInUse
			}
			ep = rt.Endpoint(parent.EndpointID)
		} else {
			ep = g.selectEndpoint(rt, protocol.ResourceInterface(spec.Kind))
		}
		newSession := parent == nil || spec.Kind == protocol.HermesSession || spec.Kind == protocol.A2AContext
		if ep == nil || ep.State == "disabled" || (ep.State == "draining" && newSession) || !g.interfaceReady(rt, ep, protocol.ResourceInterface(spec.Kind)) {
			return "", fmt.Errorf("NO_HEALTHY_ENDPOINT")
		}
		return ep.ID, nil
	})
}

func (g *Gateway) acquireResource(owner string, rt *protocol.Route, kind protocol.ResourceKind, id string) (*lease, protocol.Resource, error) {
	return g.acquireOwnedResource(owner, rt, kind, id, false)
}

func (g *Gateway) acquireOwnedResource(owner string, rt *protocol.Route, kind protocol.ResourceKind, id string, background bool) (*lease, protocol.Resource, error) {
	g.lifecycle.RLock()
	defer g.lifecycle.RUnlock()
	if !background && !configuration.HasConsumer(rt, owner) {
		return nil, protocol.Resource{}, protocol.ErrResourceNotFound
	}
	res, unpin, err := g.resources.Pin(protocol.ResourceKey{Owner: owner, Route: rt.ID, Kind: kind, ID: id})
	if err != nil {
		return nil, protocol.Resource{}, err
	}
	l, err := g.acquireLocked(owner, rt.Key, "", res.EndpointID, res.Interface, rt, background)
	if err != nil {
		unpin()
		return nil, protocol.Resource{}, err
	}
	release := l.release
	l.release = func() { release(); unpin() }
	return l, res, nil
}
