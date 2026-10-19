package runtime

import (
	"net/http"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/rollout"
	"aiagent-gateway/internal/releaseprotocol"
)

func (g *Gateway) initRollout() {
	g.worker = rollout.NewWorker(g.opts.AdminToken, rollout.WorkerBackend{
		Handler:  g.DataHandler(),
		Remote:   g.remote,
		Refresh:  g.refreshShared,
		Ready:    g.healthy.Load,
		Revision: func() uint64 { return g.current.Load().Config.Revision },
	})
	g.resolver = rollout.Resolver{
		Route:         func(key string) *protocol.Route { return g.current.Load().Routes[key] },
		ResourceOwner: g.resources.ResolveOwner,
		SessionOwner:  g.sessionOwner,
	}
}

func (g *Gateway) WorkerHandler(identity releaseprotocol.Identity) http.Handler {
	return g.worker.Handler(identity)
}

func (g *Gateway) ExecuteLocal(w http.ResponseWriter, r *http.Request, done func(releaseprotocol.Result)) {
	g.worker.ExecuteLocal(w, r, done)
}

func (g *Gateway) WorkerIdentity(base releaseprotocol.Identity) releaseprotocol.Identity {
	return g.worker.Identity(base)
}

func (g *Gateway) WorkerControl(base releaseprotocol.Identity) http.Handler {
	return g.worker.Control(base)
}

func (g *Gateway) resolveRollout(request releaseprotocol.Request) releaseprotocol.Resolution {
	return g.resolver.Resolve(request)
}
