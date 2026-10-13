package config

import (
	"net/http"
	"strconv"

	"aiagent-gateway/internal/gateway/protocol"
)

func page[T any](items []T, r *http.Request) map[string]any {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := min(offset+limit, len(items))
	out := items[offset:end]
	if out == nil {
		out = []T{}
	}
	return map[string]any{"items": out, "total": len(items), "offset": offset, "limit": limit}
}

func (h *handler) readAdmin(w http.ResponseWriter, r *http.Request, p []string) {
	s := h.backend.Current()
	if len(p) == 3 {
		routes := make([]routeView, 0, len(s.Config.Routes))
		for i := range s.Config.Routes {
			routes = append(routes, publicRoute(&s.Config.Routes[i]))
		}
		protocol.WriteJSON(w, 200, page(routes, r))
		return
	}
	ri := adminRouteIndex(s.Config.Routes, p[3])
	if ri < 0 {
		protocol.Problem(w, 404, "ROUTE_NOT_FOUND")
		return
	}
	rt := &s.Config.Routes[ri]
	if len(p) == 4 {
		w.Header().Set("ETag", Tag(rt.Version))
		protocol.WriteJSON(w, 200, publicRoute(rt))
		return
	}
	if len(p) == 5 && p[4] == "endpoints" {
		endpoints := make([]Endpoint, len(rt.Endpoints))
		for i := range rt.Endpoints {
			endpoints[i] = PublicEndpoint(&rt.Endpoints[i])
		}
		protocol.WriteJSON(w, 200, page(endpoints, r))
		return
	}
	if len(p) == 6 && p[4] == "endpoints" {
		e := endpoint(rt, p[5])
		if e == nil {
			protocol.Problem(w, 404, "ENDPOINT_NOT_FOUND")
			return
		}
		w.Header().Set("ETag", Tag(e.Version))
		protocol.WriteJSON(w, 200, h.backend.EndpointDetail(rt, e))
		return
	}
	if len(p) == 5 && p[4] == "sessions" {
		protocol.WriteJSON(w, 200, page(h.backend.Sessions(rt.ID, r.URL.Query().Get("endpoint_id")), r))
		return
	}
	if len(p) == 5 && (p[4] == "tasks" || p[4] == "runs" || p[4] == "resources") {
		kind := protocol.ResourceKind(r.URL.Query().Get("resource_kind"))
		if p[4] == "tasks" {
			kind = protocol.A2ATask
		}
		if p[4] == "runs" {
			kind = protocol.HermesRun
		}
		if kind != "" && protocol.ResourceInterface(kind) == "" {
			protocol.Problem(w, 400, "INVALID_RESOURCE_KIND")
			return
		}
		resources := h.backend.Resources(r.URL.Query().Get("owner_id"), rt.ID, kind, r.URL.Query().Get("endpoint_id"))
		entry := r.URL.Query().Get("interface")
		if entry != "" && entry != "proxy" && entry != "a2a" {
			protocol.Problem(w, 400, "INVALID_INTERFACE")
			return
		}
		out := []protocol.Resource{}
		for _, res := range resources {
			if entry != "" && res.Interface != entry {
				continue
			}
			out = append(out, res)
		}
		if p[4] == "tasks" {
			tasks := []taskResourceView{}
			for _, res := range out {
				tasks = append(tasks, taskResourceView{res, res.ID, res.Upstream, res.State == protocol.ResourceTerminal})
			}
			protocol.WriteJSON(w, 200, page(tasks, r))
		} else {
			protocol.WriteJSON(w, 200, page(out, r))
		}
		return
	}
	protocol.Problem(w, 404, "NOT_FOUND")
}

// Preserve the existing administrative task field names during migration.
type taskResourceView struct {
	protocol.Resource
	GatewayTaskID  string `json:"gateway_task_id"`
	UpstreamTaskID string `json:"upstream_task_id"`
	Terminal       bool   `json:"terminal"`
}

type consumerView struct {
	ID string `json:"id"`
}

type routeView struct {
	*Route
	Consumers []consumerView `json:"consumers"`
}

func publicRoute(rt *Route) routeView {
	copy := *rt
	copy.Endpoints = make([]Endpoint, len(rt.Endpoints))
	for i := range rt.Endpoints {
		copy.Endpoints[i] = PublicEndpoint(&rt.Endpoints[i])
	}
	v := routeView{Route: &copy, Consumers: make([]consumerView, 0, len(rt.Consumers))}
	for _, c := range rt.Consumers {
		v.Consumers = append(v.Consumers, consumerView{ID: c.ID})
	}
	return v
}

func PublicEndpoint(ep *Endpoint) Endpoint {
	copy := *ep
	copy.Interfaces = make(map[string]EndpointInterface, len(ep.Interfaces))
	for entry, iface := range ep.Interfaces {
		iface.Token = ""
		copy.Interfaces[entry] = iface
	}
	return copy
}
