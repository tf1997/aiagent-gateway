package runtime

import (
	"fmt"
	"io"
	"net/http"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
)

func (g *Gateway) AdminHandler() http.Handler {
	return configuration.NewHandler(g.opts.AdminToken, configuration.Backend{
		Current:        g.current.Load,
		Healthy:        g.healthy.Load,
		Frozen:         g.configFrozen.Load,
		BeginUpdate:    g.beginConfigUpdate,
		Commit:         g.commitConfig,
		References:     g.references,
		Sessions:       g.sessions,
		Resources:      g.resources.List,
		EndpointDetail: g.endpointDetail,
		Metrics:        g.writeMetrics,
	})
}

func (g *Gateway) beginConfigUpdate() (func(), *configuration.Error) {
	g.mu.Lock()
	if g.configFrozen.Load() {
		g.mu.Unlock()
		return nil, &configuration.Error{Status: 409, Code: "ROLLOUT_IN_PROGRESS"}
	}
	g.lifecycle.Lock()
	if !g.healthy.Load() {
		g.lifecycle.Unlock()
		g.mu.Unlock()
		return nil, &configuration.Error{Status: 503, Code: "PERSISTENCE_UNAVAILABLE"}
	}
	return func() { g.lifecycle.Unlock(); g.mu.Unlock() }, nil
}

// Called with the configuration and lifecycle write locks held. Publish only
// after the backup, runtime journal and configuration have been persisted.
func (g *Gateway) commitConfig(c protocol.Config) *configuration.Error {
	if err := configuration.BackupLegacy(g.opts.DataDir, g.legacyRaw); err != nil {
		return &configuration.Error{Status: 503, Code: "MIGRATION_BACKUP_FAILED"}
	}
	if err := g.persistSessions(); err != nil {
		return &configuration.Error{Status: 503, Code: "STATE_PERSISTENCE_FAILED"}
	}
	renamed, err := g.persist(c)
	if err != nil {
		if renamed {
			g.healthy.Store(false)
		}
		return &configuration.Error{Status: 503, Code: "PERSISTENCE_FAILED"}
	}
	g.resetHealth(g.current.Load(), &c)
	g.current.Store(configuration.Build(c))
	g.legacyRaw = nil
	return nil
}

func (g *Gateway) writeMetrics(w io.Writer) {
	fmt.Fprintf(w, "gateway_requests_total %d\ngateway_failures_total %d\ngateway_inflight %d\ngateway_sessions %d\ngateway_config_revision %d\n", g.requests.Load(), g.failures.Load(), g.inflight.Load(), g.bindings.Load(), g.current.Load().Config.Revision)
	resources, aliases, reservations := g.resources.Sizes()
	fmt.Fprintf(w, "gateway_resources %d\ngateway_resource_aliases %d\ngateway_data_idempotency_reservations %d\n", resources, aliases, reservations)
}

func (g *Gateway) endpointDetail(rt *protocol.Route, ep *protocol.Endpoint) any {
	st := g.state(rt.ID, ep.ID)
	interfaces := map[string]any{}
	for entry := range ep.Interfaces {
		is := st.iface(entry)
		interfaces[entry] = map[string]any{"healthy": is.healthy.Load(), "http_inflight": is.active.Load(), "ready": g.interfaceReady(rt, ep, entry), "capabilities": is.discovery.Load()}
	}
	counts := map[string]int{"nonterminal_tasks": 0, "nonterminal_runs": 0, "unknown_resources": 0, "reserved_resources": 0}
	for _, res := range g.resources.List("", rt.ID, "", ep.ID) {
		if res.Kind == protocol.A2ATask && res.State != protocol.ResourceTerminal {
			counts["nonterminal_tasks"]++
		}
		if res.Kind == protocol.HermesRun && res.State != protocol.ResourceTerminal {
			counts["nonterminal_runs"]++
		}
		if res.State == protocol.ResourceUnknown {
			counts["unknown_resources"]++
		}
		if res.State == protocol.ResourceReserved {
			counts["reserved_resources"]++
		}
	}
	redacted := configuration.PublicEndpoint(ep)
	return struct {
		*protocol.Endpoint
		Runtime map[string]any `json:"runtime"`
	}{&redacted, map[string]any{"http_inflight": st.active.Load(), "resource_references": g.references(rt.ID, ep.ID), "interfaces": interfaces, "resources": counts}}
}
