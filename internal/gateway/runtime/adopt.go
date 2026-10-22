package runtime

import (
	"context"
	"fmt"

	"aiagent-gateway/internal/gateway/protocol"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

// OpenStateOwner acquires the configuration writer lock and reconstructs an
// exact image into a sealed runtime. The process coordinator must already own
// owner.lock and the supplied domain journal. No data is written or upstream
// contacted here. Only a durable COMMITTED decision authorizes Resume.
func OpenStateOwner(o Options, view *StateView, journal *state.Journal, epoch string) (_ *Gateway, _ *shared.Server, err error) {
	return openStateOwner(o, view, journal, epoch, false)
}

// RestoreStateOwner restores a sealed source after a durable abort. It retains
// the source epoch so live candidates can finish against the restored owner.
func RestoreStateOwner(o Options, view *StateView, journal *state.Journal) (*Gateway, *shared.Server, error) {
	if view == nil {
		return nil, nil, fmt.Errorf("INVALID_STATE_OWNER_RESTORATION")
	}
	return openStateOwner(o, view, journal, view.Metadata().Epoch, true)
}

func openStateOwner(o Options, view *StateView, journal *state.Journal, epoch string, restoreSource bool) (_ *Gateway, _ *shared.Server, err error) {
	if view == nil || journal == nil || journal.Healthy() != nil || o.StateSocket != "" || o.StateClient != nil || epoch == "" || (!restoreSource && epoch == view.Metadata().Epoch) {
		return nil, nil, fmt.Errorf("INVALID_STATE_OWNER_ADOPTION")
	}
	// Detach all mutable indexes from the read-only view retained by the caller.
	v, err := OpenStateView(view.raw, view.digest, len(view.raw), o)
	if err != nil {
		return nil, nil, err
	}
	o.DeferBackground = true
	g, err := open(o, &v.image)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			g.Close()
		}
	}()
	image := &v.image
	limits := resourcedir.Limits{MaxResources: g.opts.MaxResources, MaxResourceAliases: g.opts.MaxResourceAliases, MaxDataIdempotency: g.opts.MaxDataIdempotency, MaxSessions: g.opts.MaxSessions, MaxTasks: g.opts.MaxTasks}
	// Active counts are rebuilt from descriptors exactly once. Persistent
	// parent/operation references remain as validated in the full image.
	for i := range image.Resources.Resources {
		image.Resources.Resources[i].Resource.ActiveRefs = 0
	}
	g.resources, err = resourcedir.RestoreImage(image.Resources, limits, func(route, ep string) bool {
		rt := v.config.RoutesByID(route)
		return rt != nil && rt.Endpoint(ep) != nil
	})
	if err != nil {
		return nil, nil, err
	}
	g.resources.AttachDurability(journal)
	g.journal = journal
	for _, s := range image.Sessions {
		g.shard(s.Key).items[s.Key] = &binding{Endpoint: s.Endpoint, WorkerOwner: s.WorkerOwner, Seen: s.Seen, TTL: s.TTL}
	}
	g.bindings.Store(image.Bindings)
	g.requests.Store(image.Requests)
	g.failures.Store(image.Failures)
	g.configFrozen.Store(image.ConfigFrozen)
	image.Tasks.Active, image.SessionLists.Active = 0, 0
	g.taskLists, err = shared.RestoreListImage(image.Tasks)
	if err != nil {
		return nil, nil, err
	}
	g.sessionLists, err = shared.RestoreListImage(image.SessionLists)
	if err != nil {
		return nil, nil, err
	}
	for key, ep := range image.Endpoints {
		st := &endpointState{}
		restoreInterface(&st.proxy, ep.Proxy)
		restoreInterface(&st.a2a, ep.A2A)
		g.states[key] = st
	}
	for key, weights := range image.Schedules {
		g.sched[key] = &routeSchedule{weights: weights}
	}
	g.initProtocols()
	g.initRollout()
	s := g.newDispatcher()
	if err = s.Seal(context.Background()); err != nil {
		return nil, nil, err
	}
	if restoreSource {
		err = s.RestoreSource(image.Transfer)
	} else {
		err = s.Import(image.Transfer, epoch)
	}
	if err != nil {
		return nil, nil, err
	}
	return g, s, nil
}

func restoreInterface(st *interfaceState, image interfaceImage) {
	st.healthy.Store(image.Healthy)
	if image.Discovery != nil {
		d := image.Discovery.Value
		d.Interface = image.Discovery.Interface
		st.discovery.Store((*protocol.Capabilities)(&d))
	}
}
