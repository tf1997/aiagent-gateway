package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func tokenOK(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+want)) == 1
}
func (g *Gateway) AdminHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tokenOK(r.Header.Get("Authorization"), g.opts.AdminToken) {
			problem(w, 401, "UNAUTHORIZED")
			return
		}
		g.admin(w, r)
	})
}
func (g *Gateway) admin(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "gateway_requests_total %d\ngateway_failures_total %d\ngateway_inflight %d\ngateway_sessions %d\ngateway_config_revision %d\n", g.requests.Load(), g.failures.Load(), g.inflight.Load(), g.bindings.Load(), g.current.Load().Config.Revision)
		resources, aliases, reservations := g.resources.sizes()
		fmt.Fprintf(w, "gateway_resources %d\ngateway_resource_aliases %d\ngateway_data_idempotency_reservations %d\n", resources, aliases, reservations)
		return
	}
	if r.URL.Path == "/admin/v1/config/status" && r.Method == "GET" {
		s := g.current.Load()
		var persisted any = s.Config.Revision
		if !g.healthy.Load() {
			persisted = nil
		}
		writeJSON(w, 200, map[string]any{"persisted_version": persisted, "applied_version": s.Config.Revision, "persistence_healthy": g.healthy.Load()})
		return
	}
	if r.URL.Path == "/admin/v1/audit-logs" && r.Method == "GET" {
		writeJSON(w, 200, page(g.current.Load().Config.Audit, r))
		return
	}
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(p) < 3 || p[0] != "admin" || p[1] != "v1" || p[2] != "routes" || len(p) > 6 {
		problem(w, 404, "NOT_FOUND")
		return
	}
	if r.Method == "GET" {
		g.readAdmin(w, r, p)
		return
	}
	if r.Method != "POST" && r.Method != "PATCH" && r.Method != "DELETE" {
		problem(w, 405, "METHOD_NOT_ALLOWED")
		return
	}
	if g.configFrozen.Load() {
		problem(w, 409, "ROLLOUT_IN_PROGRESS")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		problem(w, 413, "BODY_TOO_LARGE")
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.configFrozen.Load() {
		problem(w, 409, "ROLLOUT_IN_PROGRESS")
		return
	}
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	if !g.healthy.Load() {
		problem(w, 503, "PERSISTENCE_UNAVAILABLE")
		return
	}
	c := clone(g.current.Load().Config)
	idem := r.Header.Get("Idempotency-Key")
	if len(idem) > 256 {
		problem(w, 400, "INVALID_IDEMPOTENCY_KEY")
		return
	}
	sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), raw...))
	hash := hex.EncodeToString(sum[:])
	if idem != "" && r.Method == "POST" {
		if receipt, ok := c.Receipts[idem]; ok {
			if receipt.Hash != hash {
				problem(w, 409, "IDEMPOTENCY_CONFLICT")
				return
			}
			w.Header().Set("ETag", receipt.ETag)
			writeJSON(w, 201, receipt.Body)
			return
		}
	}
	var result any
	version := uint64(1)
	status := 200
	ri := -1
	if len(p) >= 4 {
		for i := range c.Routes {
			if c.Routes[i].ID == p[3] {
				ri = i
				break
			}
		}
		if ri < 0 {
			problem(w, 404, "ROUTE_NOT_FOUND")
			return
		}
	}
	reject := func(code int, msg string) { problem(w, code, msg) }
	rejectInput := func(err error) {
		code := 400
		if errors.Is(err, errConfigConflict) {
			code = 409
		}
		reject(code, err.Error())
	}
	switch {
	case len(p) == 3 && r.Method == "POST":
		rt := Route{Enabled: true, Session: SessionPolicy{Enabled: true, AcceptA2AContext: true}, Endpoints: []Endpoint{}}
		if err := decodeRouteInput(raw, &rt); err != nil {
			rejectInput(err)
			return
		}
		if rt.ID != "" || rt.Version != 0 {
			reject(400, "ID_AND_VERSION_READ_ONLY")
			return
		}
		defaults(&rt)
		rt.ID = newID("rt_")
		rt.Version = 1
		for i := range rt.Endpoints {
			if err := initEndpoint(&rt.Endpoints[i]); err != nil {
				reject(400, err.Error())
				return
			}
		}
		c.Routes = append(c.Routes, rt)
		result = rt
		status = 201
	case len(p) == 4 && (r.Method == "PATCH" || r.Method == "DELETE"):
		rt := &c.Routes[ri]
		if !match(w, r, rt.Version) {
			return
		}
		version = rt.Version + 1
		if r.Method == "DELETE" {
			if g.references(rt.ID, "") > 0 {
				reject(409, "ROUTE_IN_USE")
				return
			}
			c.Routes = append(c.Routes[:ri], c.Routes[ri+1:]...)
			result = map[string]bool{"deleted": true}
		} else {
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &fields) != nil || fields == nil {
				reject(400, "INVALID_JSON")
				return
			}
			for _, k := range []string{"endpoints", "id", "version"} {
				if _, ok := fields[k]; ok {
					reject(400, "READ_ONLY_FIELD: "+k)
					return
				}
			}
			old := *rt
			patch, err := routePatchInput(raw, rt)
			if err != nil {
				rejectInput(err)
				return
			}
			if err := merge(patch, rt); err != nil {
				rejectInput(err)
				return
			}
			changed := old.Key != rt.Key || old.AgentType != rt.AgentType
			for _, entry := range old.Expose {
				changed = changed || !exposes(rt, entry)
			}
			if changed && g.references(rt.ID, "") > 0 {
				reject(409, "ROUTE_IN_USE")
				return
			}
			rt.Version = version
			result = *rt
		}
	case len(p) == 5 && p[4] == "endpoints" && r.Method == "POST":
		var ep Endpoint
		input, err := endpointInput(raw, &c.Routes[ri], false)
		if err != nil {
			rejectInput(err)
			return
		}
		if err := decode(input, &ep); err != nil {
			rejectInput(err)
			return
		}
		if err := initEndpoint(&ep); err != nil {
			reject(400, err.Error())
			return
		}
		rt := &c.Routes[ri]
		rt.Endpoints = append(rt.Endpoints, ep)
		rt.Version++
		result = ep
		version = ep.Version
		status = 201
	case len(p) == 6 && p[4] == "endpoints" && (r.Method == "PATCH" || r.Method == "DELETE"):
		rt := &c.Routes[ri]
		ei := -1
		for i := range rt.Endpoints {
			if rt.Endpoints[i].ID == p[5] {
				ei = i
				break
			}
		}
		if ei < 0 {
			reject(404, "ENDPOINT_NOT_FOUND")
			return
		}
		ep := &rt.Endpoints[ei]
		if !match(w, r, ep.Version) {
			return
		}
		version = ep.Version + 1
		if r.Method == "DELETE" {
			if g.references(rt.ID, ep.ID) > 0 {
				reject(409, "ENDPOINT_IN_USE")
				return
			}
			rt.Endpoints = append(rt.Endpoints[:ei], rt.Endpoints[ei+1:]...)
			result = map[string]bool{"deleted": true}
		} else {
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &fields) != nil || fields == nil {
				reject(400, "INVALID_JSON")
				return
			}
			for _, k := range []string{"id", "version", "route_id"} {
				if _, ok := fields[k]; ok {
					reject(400, "READ_ONLY_FIELD: "+k)
					return
				}
			}
			old := ep.Interfaces
			patch, err := endpointInput(raw, rt, true)
			if err != nil {
				rejectInput(err)
				return
			}
			if err := merge(patch, ep); err != nil {
				rejectInput(err)
				return
			}
			changed := false
			for entry, iface := range old {
				next, ok := ep.Interfaces[entry]
				changed = changed || !ok || next.URL != iface.URL || next.ProtocolVersion != iface.ProtocolVersion || next.Tenant != iface.Tenant
			}
			if changed && g.references(rt.ID, ep.ID) > 0 {
				reject(409, "ENDPOINT_IN_USE")
				return
			}
			ep.Version = version
			result = *ep
		}
		rt.Version++
	default:
		reject(405, "METHOD_NOT_ALLOWED")
		return
	}
	if err := validate(&c); err != nil {
		reject(400, err.Error())
		return
	}
	c.Revision++
	c.Audit = append(c.Audit, Audit{At: time.Now().UTC(), Actor: "admin", Action: r.Method, Path: r.URL.Path, Revision: c.Revision})
	if len(c.Audit) > 1000 {
		c.Audit = c.Audit[len(c.Audit)-1000:]
	}
	if idem != "" && r.Method == "POST" {
		b, _ := json.Marshal(result)
		c.Receipts[idem] = Receipt{Hash: hash, Body: b, ETag: tag(version)}
		c.ReceiptOrder = append(c.ReceiptOrder, idem)
		if len(c.ReceiptOrder) > 1000 {
			delete(c.Receipts, c.ReceiptOrder[0])
			c.ReceiptOrder = c.ReceiptOrder[1:]
		}
	}
	if err := g.backupLegacy(); err != nil {
		reject(503, "MIGRATION_BACKUP_FAILED")
		return
	}
	if err := g.persistSessions(); err != nil {
		reject(503, "STATE_PERSISTENCE_FAILED")
		return
	}
	renamed, err := g.persist(c)
	if err != nil {
		if renamed {
			g.healthy.Store(false)
		}
		reject(503, "PERSISTENCE_FAILED")
		return
	}
	g.resetHealth(g.current.Load(), &c)
	g.current.Store(build(c))
	g.legacyRaw = nil
	w.Header().Set("ETag", tag(version))
	w.Header().Set("X-Config-Revision", strconv.FormatUint(c.Revision, 10))
	writeJSON(w, status, result)
}
func initEndpoint(e *Endpoint) error {
	if e.ID != "" || e.Version != 0 {
		return fmt.Errorf("ID_AND_VERSION_READ_ONLY")
	}
	e.ID = newID("ep_")
	e.Version = 1
	if e.Weight == 0 {
		e.Weight = 100
	}
	if e.State == "" {
		e.State = "active"
	}
	return nil
}
func match(w http.ResponseWriter, r *http.Request, v uint64) bool {
	if r.Header.Get("If-Match") == "" {
		problem(w, 428, "IF_MATCH_REQUIRED")
		return false
	}
	if r.Header.Get("If-Match") != tag(v) {
		problem(w, 412, "VERSION_CONFLICT")
		return false
	}
	return true
}
func decode(b []byte, v any) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid trailing JSON")
	}
	return nil
}
func merge(b []byte, v any) error {
	old, _ := json.Marshal(v)
	var dst, patch map[string]any
	if json.Unmarshal(old, &dst) != nil || json.Unmarshal(b, &patch) != nil || patch == nil {
		return fmt.Errorf("invalid patch")
	}
	mergeMap(dst, patch)
	out, _ := json.Marshal(dst)
	fresh := reflect.New(reflect.TypeOf(v).Elem())
	if err := decode(out, fresh.Interface()); err != nil {
		return err
	}
	reflect.ValueOf(v).Elem().Set(fresh.Elem())
	return nil
}
func mergeMap(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		if m, ok := v.(map[string]any); ok {
			base, ok := dst[k].(map[string]any)
			if !ok {
				base = map[string]any{}
			}
			mergeMap(base, m)
			dst[k] = base
		} else {
			dst[k] = v
		}
	}
}
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
func (g *Gateway) readAdmin(w http.ResponseWriter, r *http.Request, p []string) {
	s := g.current.Load()
	if len(p) == 3 {
		writeJSON(w, 200, page(s.Config.Routes, r))
		return
	}
	var rt *Route
	for i := range s.Config.Routes {
		if s.Config.Routes[i].ID == p[3] {
			rt = &s.Config.Routes[i]
		}
	}
	if rt == nil {
		problem(w, 404, "ROUTE_NOT_FOUND")
		return
	}
	if len(p) == 4 {
		w.Header().Set("ETag", tag(rt.Version))
		writeJSON(w, 200, rt)
		return
	}
	if len(p) == 5 && p[4] == "endpoints" {
		writeJSON(w, 200, page(rt.Endpoints, r))
		return
	}
	if len(p) == 6 && p[4] == "endpoints" {
		e := endpoint(rt, p[5])
		if e == nil {
			problem(w, 404, "ENDPOINT_NOT_FOUND")
			return
		}
		w.Header().Set("ETag", tag(e.Version))
		writeJSON(w, 200, g.endpointDetail(rt, e))
		return
	}
	if len(p) == 5 && p[4] == "sessions" {
		writeJSON(w, 200, page(g.sessions(rt.ID, r.URL.Query().Get("endpoint_id")), r))
		return
	}
	if len(p) == 5 && (p[4] == "tasks" || p[4] == "runs" || p[4] == "resources") {
		kind := resourceKind(r.URL.Query().Get("resource_kind"))
		if p[4] == "tasks" {
			kind = a2aTask
		}
		if p[4] == "runs" {
			kind = hermesRun
		}
		if kind != "" && resourceInterface(kind) == "" {
			problem(w, 400, "INVALID_RESOURCE_KIND")
			return
		}
		resources := g.resources.list(r.URL.Query().Get("owner_id"), rt.ID, kind, r.URL.Query().Get("endpoint_id"))
		entry := r.URL.Query().Get("interface")
		if entry != "" && entry != "proxy" && entry != "a2a" {
			problem(w, 400, "INVALID_INTERFACE")
			return
		}
		out := []resource{}
		for _, res := range resources {
			if entry != "" && res.Interface != entry {
				continue
			}
			out = append(out, res)
		}
		if p[4] == "tasks" {
			tasks := []taskResourceView{}
			for _, res := range out {
				tasks = append(tasks, taskResourceView{res, res.ID, res.Upstream, res.State == resourceTerminal})
			}
			writeJSON(w, 200, page(tasks, r))
		} else {
			writeJSON(w, 200, page(out, r))
		}
		return
	}
	problem(w, 404, "NOT_FOUND")
}

// Preserve the existing administrative task field names during migration.
type taskResourceView struct {
	resource
	GatewayTaskID  string `json:"gateway_task_id"`
	UpstreamTaskID string `json:"upstream_task_id"`
	Terminal       bool   `json:"terminal"`
}

func (g *Gateway) endpointDetail(rt *Route, ep *Endpoint) any {
	st := g.state(rt.ID, ep.ID)
	interfaces := map[string]any{}
	for entry := range ep.Interfaces {
		is := st.iface(entry)
		interfaces[entry] = map[string]any{"healthy": is.healthy.Load(), "http_inflight": is.active.Load(), "ready": g.interfaceReady(rt, ep, entry), "capabilities": is.discovery.Load()}
	}
	counts := map[string]int{"nonterminal_tasks": 0, "nonterminal_runs": 0, "unknown_resources": 0, "reserved_resources": 0}
	for _, res := range g.resources.list("", rt.ID, "", ep.ID) {
		if res.Kind == a2aTask && res.State != resourceTerminal {
			counts["nonterminal_tasks"]++
		}
		if res.Kind == hermesRun && res.State != resourceTerminal {
			counts["nonterminal_runs"]++
		}
		if res.State == resourceUnknown {
			counts["unknown_resources"]++
		}
		if res.State == resourceReserved {
			counts["reserved_resources"]++
		}
	}
	return struct {
		*Endpoint
		Runtime map[string]any `json:"runtime"`
	}{ep, map[string]any{"http_inflight": st.active.Load(), "resource_references": g.references(rt.ID, ep.ID), "interfaces": interfaces, "resources": counts}}
}
