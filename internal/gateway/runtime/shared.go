package runtime

import (
	"context"
	"fmt"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

type SharedServer = shared.Server

func NewSharedServer(g *Gateway, journal *state.Journal) (*SharedServer, error) {
	if g.remote != nil || g.dispatcher != nil {
		return nil, fmt.Errorf("shared server requires an unused state owner")
	}
	if err := g.EnableDurability(journal); err != nil {
		return nil, err
	}
	return g.newDispatcher(), nil
}

func (g *Gateway) newDispatcher() *SharedServer {
	g.resources.SetFailureHandler(func() { g.healthy.Store(false) })
	g.dispatcher = shared.NewServer(g.opts.AdminToken, shared.Backend{
		Barrier:      &g.stateBarrier,
		Discover:     g.discoverShared,
		Execute:      g.executeShared,
		Freeze:       g.freezeConfig,
		RestoreLease: g.restoreSharedLease,
	})
	return g.dispatcher
}

func (g *Gateway) discoverShared(ctx context.Context, q shared.Request) {
	if rt := g.current.Load().Routes[q.Route]; rt != nil && rt.AgentType == "hermes" {
		if q.Entry == "a2a" || protocol.ResourceInterface(q.Creation.Kind) == "a2a" {
			g.discoverA2A(ctx, rt)
		} else {
			g.discoverHermes(ctx, rt)
		}
	}
}

func (g *Gateway) freezeConfig(frozen bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configFrozen.Store(frozen)
}

func SharedControl(socket, token, worker, action string, flag bool) (int64, error) {
	return shared.Control(socket, token, worker, action, flag)
}

func ResolveShared(socket, token string, q releaseprotocol.Request) (releaseprotocol.Resolution, error) {
	return shared.Resolve(socket, token, q)
}

func (g *Gateway) sharedAcquire(owner, key, sid, forced, entry string, expected *protocol.Route, background bool) (*lease, error) {
	var out shared.Response
	rev := uint64(0)
	if expected != nil {
		rev = expected.Version
	}
	err := g.remote.Call("acquire", shared.Request{Owner: owner, Route: key, Session: sid, Endpoint: forced, Entry: entry, Revision: rev, Flag: background}, &out)
	if err != nil {
		return nil, err
	}
	rt := g.current.Load().Routes[key]
	if rt == nil || out.Endpoint == nil {
		g.remote.Release(out.Lease)()
		return nil, fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
	}
	st := g.state(rt.ID, out.Endpoint.ID)
	return &lease{route: rt, ep: out.Endpoint, state: st, iface: out.Endpoint.Interfaces[entry], release: g.remote.Release(out.Lease)}, nil
}

func (g *Gateway) refreshShared() error {
	var out shared.Response
	if err := g.remote.Call("config", shared.Request{}, &out); err != nil {
		return err
	}
	if out.Config == nil {
		return fmt.Errorf("missing shared config")
	}
	current := g.current.Load()
	if current == nil || current.Config.Revision != out.Config.Revision {
		if err := configuration.Validate(out.Config); err != nil {
			return err
		}
		g.current.Store(configuration.Build(*out.Config))
	}
	return nil
}

func (g *Gateway) executeShared(q shared.Request, leases *shared.Leases) (out shared.Response) {
	d := g.resources
	var err error
	defer func() {
		if err != nil {
			out.Error = err.Error()
		}
	}()
	if !g.healthy.Load() && q.Action != "release" && q.Action != "retire" {
		out.Error = "STATE_NOT_READY"
		return
	}
	rt := g.current.Load().Routes[q.Route]
	switch q.Action {
	case "resolve":
		out.Resolution = g.resolveRollout(q.Request)
	case "request_result":
		g.requests.Add(1)
		if q.Flag {
			g.failures.Add(1)
		}
	case "config":
		c := g.current.Load().Config
		out.Config = &c
	case "freeze":
		g.freezeConfig(q.Flag)
	case "status":
		out.Count = leases.Count(q.Worker)
	case "retire":
		leases.Retire(q.Worker)
	case "release":
		var released bool
		released, err = leases.Release(q.Worker, q.Lease)
		if err == nil && released {
			err = g.persistSessions()
		}
	case "acquire":
		if rt == nil || rt.Version != q.Revision {
			err = fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
			break
		}
		var l *lease
		g.lifecycle.RLock()
		l, err = g.acquireLocked(q.Owner, q.Route, q.Session, q.Endpoint, q.Entry, rt, q.Flag)
		g.lifecycle.RUnlock()
		if err == nil {
			if q.Session != "" {
				sh := g.shard(sessionKey{q.Owner, rt.ID, q.Session, q.Entry})
				sh.mu.Lock()
				if b := sh.items[sessionKey{q.Owner, rt.ID, q.Session, q.Entry}]; b != nil && b.WorkerOwner == "" {
					b.WorkerOwner = q.Worker
				}
				sh.mu.Unlock()
			}
			if err = g.persistSessions(); err != nil {
				l.release()
				break
			}
			out.Endpoint = l.ep
			out.Lease = leases.Add(shared.LeaseRecord{Worker: q.Worker, Kind: "acquire", Owner: q.Owner, Route: q.Route, Endpoint: l.ep.ID, Entry: q.Entry, Session: q.Session}, l.release)
		}
	case "reserve":
		if rt == nil || rt.Version != q.Revision {
			err = fmt.Errorf("ROUTE_CONFIGURATION_CHANGED")
			break
		}
		out.Reservation, err = g.reserveResource(q.Owner, rt, q.Creation)
		if err == nil {
			err = g.resources.AssignWorker([]protocol.Resource{out.Reservation.Resource}, q.Worker)
		}
	case "lookup":
		out.Resource, err = d.Lookup(q.Key)
	case "pin":
		var release func()
		out.Resource, release, err = d.Pin(q.Key)
		if err == nil {
			out.Lease = leases.Add(shared.LeaseRecord{Worker: q.Worker, Kind: "pin", Resource: q.Key}, release)
		}
	case "update":
		err = d.Update(q.Resource, q.State)
	case "alias":
		err = d.AttachAlias(q.Resource, q.Endpoint, q.Flag)
	case "observe":
		var m map[protocol.ResourceReverseKey]protocol.Resource
		m, err = d.ObserveBatch(q.Owner, q.Route, q.Endpoint, q.Observations)
		for k, v := range m {
			out.Mapped = append(out.Mapped, shared.ResourcePair{Key: k, Value: v})
		}
		if err == nil {
			var refs []protocol.Resource
			for _, v := range m {
				refs = append(refs, v)
			}
			err = g.resources.AssignWorker(refs, q.Worker)
		}
	case "hermes":
		err = d.ObserveHermes(q.Resource, q.Execution, q.IDs, q.Session, q.Endpoint, q.State)
	case "public":
		out.ID = d.PublicUpstream(q.Owner, q.Route, q.Kind, q.Endpoint, q.Session)
	case "forget":
		err = d.Forget(q.Resource)
	case "abandon":
		err = d.Abandon(q.Resource)
	case "abandon_prepared":
		err = d.AbandonPrepared(q.Resources, q.Flag)
	case "list":
		out.Resources = d.List(q.Owner, q.Route, q.Kind, q.Endpoint)
	case "has_task":
		out.Flag = d.HasTask(q.Owner, q.Route, q.Session)
	case "cache_acquire":
		var release func()
		release, err = g.acquireList(q.Entry)
		if err == nil {
			out.Lease = leases.Add(shared.LeaseRecord{Worker: q.Worker, Kind: "cache_acquire", Entry: q.Entry}, release)
		}
	case "cache_get":
		out.Cache, out.Flag = g.cacheGet(q.Entry, q.Session)
	case "cache_put":
		out.ID, err = g.cachePut(q.Entry, q.Cache)
	default:
		err = fmt.Errorf("unsupported state operation")
	}
	return
}

func (g *Gateway) listCache(kind string) *shared.ListCache {
	if kind == "sessions" {
		return g.sessionLists
	}
	return g.taskLists
}

func (g *Gateway) cacheGet(kind, key string) (protocol.ListSnapshot, bool) {
	if g.remote != nil {
		var out shared.Response
		g.remote.Must("cache_get", shared.Request{Entry: kind, Session: key}, &out)
		return out.Cache, out.Flag
	}
	return g.listCache(kind).Get(key)
}

func (g *Gateway) cachePut(kind string, v protocol.ListSnapshot) (string, error) {
	if g.remote != nil {
		var out shared.Response
		err := g.remote.Call("cache_put", shared.Request{Entry: kind, Cache: v}, &out)
		return out.ID, err
	}
	return g.listCache(kind).Put(v)
}

func (g *Gateway) acquireList(kind string) (func(), error) {
	if g.remote != nil {
		var out shared.Response
		if err := g.remote.Call("cache_acquire", shared.Request{Entry: kind}, &out); err != nil {
			return nil, err
		}
		return g.remote.Release(out.Lease), nil
	}
	return g.listCache(kind).Acquire()
}
