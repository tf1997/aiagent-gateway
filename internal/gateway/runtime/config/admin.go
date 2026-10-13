package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

func NewHandler(token string, backend Backend) http.Handler {
	h := &handler{token: token, backend: backend}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !TokenOK(r.Header.Get("Authorization"), h.token) {
			protocol.Problem(w, 401, "UNAUTHORIZED")
			return
		}
		h.admin(w, r)
	})
}

func (h *handler) admin(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		h.backend.Metrics(w)
		return
	}
	if r.URL.Path == "/admin/v1/config/status" && r.Method == "GET" {
		s := h.backend.Current()
		var persisted any = s.Config.Revision
		if !h.backend.Healthy() {
			persisted = nil
		}
		protocol.WriteJSON(w, 200, map[string]any{"persisted_version": persisted, "applied_version": s.Config.Revision, "persistence_healthy": h.backend.Healthy()})
		return
	}
	if r.URL.Path == "/admin/v1/audit-logs" && r.Method == "GET" {
		protocol.WriteJSON(w, 200, page(h.backend.Current().Config.Audit, r))
		return
	}
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(p) < 3 || p[0] != "admin" || p[1] != "v1" || p[2] != "routes" || len(p) > 6 {
		protocol.Problem(w, 404, "NOT_FOUND")
		return
	}
	if r.Method == "GET" {
		h.readAdmin(w, r, p)
		return
	}
	if r.Method != "POST" && r.Method != "PATCH" && r.Method != "DELETE" {
		protocol.Problem(w, 405, "METHOD_NOT_ALLOWED")
		return
	}
	if h.backend.Frozen() {
		protocol.Problem(w, 409, "ROLLOUT_IN_PROGRESS")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		protocol.Problem(w, 413, "BODY_TOO_LARGE")
		return
	}
	unlock, failure := h.backend.BeginUpdate()
	if failure != nil {
		protocol.Problem(w, failure.Status, failure.Code)
		return
	}
	defer unlock()
	c := Clone(h.backend.Current().Config)
	idem := r.Header.Get("Idempotency-Key")
	if len(idem) > 256 {
		protocol.Problem(w, 400, "INVALID_IDEMPOTENCY_KEY")
		return
	}
	sum := sha256.Sum256(append([]byte(r.Method+" "+r.URL.Path+"\n"), raw...))
	hash := hex.EncodeToString(sum[:])
	if idem != "" && r.Method == "POST" {
		if receipt, ok := c.Receipts[idem]; ok {
			if receipt.Hash != hash {
				protocol.Problem(w, 409, "IDEMPOTENCY_CONFLICT")
				return
			}
			w.Header().Set("ETag", receipt.ETag)
			status := http.StatusCreated
			// An upsert update returns an existing route at version >= 2.
			// Derive the status from the saved ETag to keep the on-disk
			// receipt format readable by older workers during a rollout.
			if len(p) == 3 && receipt.ETag != Tag(1) {
				status = http.StatusOK
			}
			protocol.WriteJSON(w, status, receipt.Body)
			return
		}
	}
	var result any
	version := uint64(1)
	status := 200
	ri := -1
	if len(p) >= 4 {
		ri = adminRouteIndex(c.Routes, p[3])
		if ri < 0 {
			protocol.Problem(w, 404, "ROUTE_NOT_FOUND")
			return
		}
	}
	reject := func(code int, msg string) { protocol.Problem(w, code, msg) }
	rejectInput := func(err error) {
		code := 400
		if errors.Is(err, errConfigConflict) {
			code = 409
		}
		reject(code, err.Error())
	}
	switch {
	case len(p) == 3 && r.Method == "POST":
		fields, err := object(raw)
		if err != nil {
			rejectInput(err)
			return
		}
		var key string
		if json.Unmarshal(fields["key"], &key) != nil || !validKey.MatchString(key) {
			reject(400, "INVALID_ROUTE_KEY")
			return
		}
		if has(fields, "id", "version") {
			reject(400, "ID_AND_VERSION_READ_ONLY")
			return
		}
		for i := range c.Routes {
			if c.Routes[i].Key == key {
				ri = i
				break
			}
		}
		if ri >= 0 {
			if r.Header.Get("If-Match") != "" && !match(w, r, c.Routes[ri].Version) {
				return
			}
			rt, err := h.upsertRoute(c.Routes[ri], fields)
			if err != nil {
				rejectInput(err)
				return
			}
			c.Routes[ri] = rt
			result, version = rt, rt.Version
			break
		}
		if r.Header.Get("If-Match") != "" {
			reject(412, "VERSION_CONFLICT")
			return
		}
		rt := Route{Enabled: true, Session: SessionPolicy{Enabled: true, AcceptA2AContext: true}, Endpoints: []Endpoint{}}
		consumers, _, err := h.consumerInput(fields, nil)
		if err != nil {
			rejectInput(err)
			return
		}
		input, _ := json.Marshal(fields)
		if err := decodeRouteInput(input, &rt); err != nil {
			rejectInput(err)
			return
		}
		rt.Consumers = consumers
		if rt.ID != "" || rt.Version != 0 {
			reject(400, "ID_AND_VERSION_READ_ONLY")
			return
		}
		Defaults(&rt)
		rt.ID = protocol.NewID("rt_")
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
		// Key-based deletion is unconditional unless the caller supplies an ETah.
		if (r.Method == "PATCH" || p[3] != rt.Key || r.Header.Get("If-Match") != "") && !match(w, r, rt.Version) {
			return
		}
		version = rt.Version + 1
		if r.Method == "DELETE" {
			if h.backend.References(rt.ID, "") > 0 {
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
			consumers, supplied, err := h.consumerInput(fields, old.Consumers)
			if err != nil {
				rejectInput(err)
				return
			}
			input, _ := json.Marshal(fields)
			patch, err := routePatchInput(input, rt)
			if err != nil {
				rejectInput(err)
				return
			}
			if err := merge(patch, rt); err != nil {
				rejectInput(err)
				return
			}
			if supplied {
				rt.Consumers = consumers
			}
			changed := old.Key != rt.Key || old.AgentType != rt.AgentType
			for _, entry := range old.Expose {
				changed = changed || !protocol.Exposes(rt, entry)
			}
			if changed && h.backend.References(rt.ID, "") > 0 {
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
			if h.backend.References(rt.ID, ep.ID) > 0 {
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
			if changed && h.backend.References(rt.ID, ep.ID) > 0 {
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
	if err := Validate(&c); err != nil {
		reject(400, err.Error())
		return
	}
	if rt, ok := result.(Route); ok {
		result = publicRoute(&rt)
	} else if ep, ok := result.(Endpoint); ok {
		result = PublicEndpoint(&ep)
	}
	c.Revision++
	c.Audit = append(c.Audit, Audit{At: time.Now().UTC(), Actor: "admin", Action: r.Method, Path: r.URL.Path, Revision: c.Revision})
	if len(c.Audit) > 1000 {
		c.Audit = c.Audit[len(c.Audit)-1000:]
	}
	if idem != "" && r.Method == "POST" {
		b, _ := json.Marshal(result)
		c.Receipts[idem] = Receipt{Hash: hash, Body: b, ETag: Tag(version)}
		c.ReceiptOrder = append(c.ReceiptOrder, idem)
		if len(c.ReceiptOrder) > 1000 {
			delete(c.Receipts, c.ReceiptOrder[0])
			c.ReceiptOrder = c.ReceiptOrder[1:]
		}
	}
	if failure := h.backend.Commit(c); failure != nil {
		reject(failure.Status, failure.Code)
		return
	}
	w.Header().Set("ETag", Tag(version))
	w.Header().Set("X-Config-Revision", strconv.FormatUint(c.Revision, 10))
	protocol.WriteJSON(w, status, result)
}

func initEndpoint(e *Endpoint) error {
	if e.ID != "" || e.Version != 0 {
		return fmt.Errorf("ID_AND_VERSION_READ_ONLY")
	}
	e.ID = protocol.NewID("ep_")
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
		protocol.Problem(w, 428, "IF_MATCH_REQUIRED")
		return false
	}
	if r.Header.Get("If-Match") != Tag(v) {
		protocol.Problem(w, 412, "VERSION_CONFLICT")
		return false
	}
	return true
}
