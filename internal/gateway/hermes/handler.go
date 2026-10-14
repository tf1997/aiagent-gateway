// Package hermes adapts the Hermes HTTP API to gateway-owned resources.
package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

type Handler struct{ *protocol.Backend }

func New(backend *protocol.Backend) *Handler { return &Handler{backend} }

type hermesOperation struct{ action, id, suffix string }

func hermesPath(method, path string) (hermesOperation, bool) {
	p := strings.Split(strings.TrimPrefix(path, "/"), "/")
	op := hermesOperation{}
	if path == "/v1/capabilities" && method == "GET" {
		op.action = "capabilities"
	}
	if path == "/v1/models" && method == "GET" {
		op.action = "models"
	}
	if path == "/v1/chat/completions" && method == "POST" {
		op.action = "completion"
	}
	if path == "/v1/responses" && method == "POST" {
		op.action = "response"
	}
	if path == "/v1/runs" && method == "POST" {
		op.action = "run.create"
	}
	if path == "/api/sessions" {
		if method == "POST" {
			op.action = "session.create"
		}
		if method == "GET" {
			op.action = "session.list"
		}
	}
	if len(p) >= 3 && p[0] == "api" && p[1] == "sessions" && protocol.ValidResourceID(p[2]) {
		op.id = p[2]
		if len(p) == 3 {
			switch method {
			case "GET":
				op.action = "session.get"
			case "PATCH":
				op.action = "session.patch"
			case "DELETE":
				op.action = "session.delete"
			}
		} else {
			op.suffix = strings.Join(p[3:], "/")
			if method == "GET" && op.suffix == "messages" {
				op.action = "session.messages"
			}
			if method == "POST" {
				switch op.suffix {
				case "fork", "chat", "chat/stream", "model":
					op.action = "session." + op.suffix
				}
			}
		}
	}
	if len(p) >= 3 && p[0] == "v1" && p[1] == "runs" && protocol.ValidResourceID(p[2]) {
		op.id = p[2]
		if len(p) == 3 && method == "GET" {
			op.action = "run.get"
		}
		if len(p) == 4 {
			op.suffix = p[3]
			if method == "GET" && p[3] == "events" {
				op.action = "run.events"
			}
			if method == "POST" {
				switch p[3] {
				case "stop", "steer", "approval":
					op.action = "run." + p[3]
				}
			}
		}
	}
	return op, op.action != ""
}

func hermesError(w http.ResponseWriter, err error) {
	code := 503
	switch {
	case errors.Is(err, protocol.ErrResourceNotFound):
		code = 404
	case errors.Is(err, protocol.ErrResourceConflict), errors.Is(err, protocol.ErrResourceInUse):
		code = 409
	case errors.Is(err, protocol.ErrResourceInvalid):
		code = 400
	}
	protocol.Problem(w, code, err.Error())
}

func oneHeader(h http.Header, name string) (string, error) {
	values := h.Values(name)
	if len(values) > 1 {
		return "", protocol.ErrResourceInvalid
	}
	if len(values) == 0 {
		return "", nil
	}
	if len(values[0]) > 256 || strings.ContainsAny(values[0], "\r\n\x00") {
		return "", protocol.ErrResourceInvalid
	}
	return values[0], nil
}

func idField(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok || !protocol.ValidResourceID(s) {
		return "", protocol.ErrResourceInvalid
	}
	return s, nil
}

type hermesRequest struct {
	g                  *Handler
	caller             protocol.Caller
	rt                 *protocol.Route
	op                 hermesOperation
	session, execution protocol.Resource
	body               map[string]any
	path, query        string
	headers            http.Header
	lease              *protocol.Lease
	terminal           bool
	conversation       string
	fingerprint        string
	created            []protocol.Resource
	sent               bool
	executionNew       bool
}

func (g *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, c protocol.Caller, rt *protocol.Route, path string) {
	op, ok := hermesPath(r.Method, path)
	if !ok {
		protocol.Problem(w, 501, "HERMES_OPERATION_NOT_SUPPORTED")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(rt.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	r = r.WithContext(ctx)
	h := &hermesRequest{g: g, caller: c, rt: rt, op: op, path: path, query: r.URL.RawQuery, headers: make(http.Header)}
	if r.Method == "POST" || r.Method == "PATCH" {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if err != nil {
			protocol.Problem(w, 413, "REQUEST_TOO_LARGE")
			return
		}
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		h.body, err = protocol.JSONObject(raw)
		if err != nil {
			protocol.Problem(w, 400, "INVALID_JSON")
			return
		}
	}
	rawFingerprint, _ := json.Marshal(h.body)
	h.fingerprint = protocol.DataFingerprint(r.Method, path, rawFingerprint, r.Header, "X-Hermes-Session-Id", rt.Session.Header, "X-Hermes-Session-Key")
	// Discovery never expands the supported path table or route exposure.
	g.DiscoverHermes(ctx, rt)
	if op.action == "session.list" {
		g.listHermesSessions(w, r, c, rt)
		return
	}
	defer h.abandonPrepared()
	if err := h.prepare(r); err != nil {
		hermesError(w, err)
		return
	}
	defer h.lease.Release()
	if op.action == "capabilities" {
		h.capabilities(w)
		return
	}
	h.forward(w, r)
}

func (h *hermesRequest) lookup(kind protocol.ResourceKind, id string) (protocol.Resource, error) {
	if !protocol.ValidResourceID(id) {
		return protocol.Resource{}, protocol.ErrResourceInvalid
	}
	res, err := h.g.Resources.Lookup(protocol.ResourceKey{Owner: h.caller.ID, Route: h.rt.ID, Kind: kind, ID: id})
	if err == nil && kind == protocol.HermesSession && res.State == protocol.ResourceTerminal {
		return protocol.Resource{}, protocol.ErrResourceNotFound
	}
	return res, err
}

func (h *hermesRequest) sessionHint(r *http.Request, id string) error {
	refs := []string{}
	if id != "" {
		refs = append(refs, id)
	}
	names := []string{"X-Hermes-Session-Id"}
	if !strings.EqualFold(h.rt.Session.Header, "X-Hermes-Session-Id") {
		names = append(names, h.rt.Session.Header)
	}
	for _, name := range names {
		v, err := oneHeader(r.Header, name)
		if err != nil {
			return err
		}
		if v != "" {
			refs = append(refs, v)
		}
	}
	for _, ref := range refs {
		s, err := h.lookup(protocol.HermesSession, ref)
		if err != nil {
			return err
		}
		if h.session.ID != "" && h.session.ID != s.ID {
			return protocol.ErrResourceConflict
		}
		h.session = s
	}
	return nil
}

func (h *hermesRequest) reserve(kind protocol.ResourceKind, alias, idem, operation string) (protocol.CreationReservation, error) {
	spec := protocol.ResourceCreation{Kind: kind, Alias: alias, Operation: operation, IdempotencyKey: idem, Fingerprint: h.fingerprint}
	if h.session.ID != "" {
		spec.ParentKind = protocol.HermesSession
		spec.ParentID = h.session.ID
	}
	reservation, err := h.g.ReserveResource(h.caller.ID, h.rt, spec)
	if err == nil && reservation.First {
		h.created = append(h.created, reservation.Resource)
	}
	return reservation, err
}

func (h *hermesRequest) prepare(r *http.Request) error {
	key, err := oneHeader(r.Header, "X-Hermes-Session-Key")
	if err != nil {
		return err
	}
	idem, err := oneHeader(r.Header, "Idempotency-Key")
	if err != nil {
		return err
	}
	if idem != "" && h.op.action != "run.create" {
		return protocol.ErrResourceInvalid
	}
	for _, name := range []string{"Accept", "Last-Event-ID"} {
		h.headers[name] = r.Header.Values(name)
	}
	if key != "" {
		h.headers.Set("X-Hermes-Session-Key", protocol.NamespacedIdempotency(h.caller.ID, h.rt.ID, "memory", key))
	}
	// Body references must agree with path/header references, never override them.
	sid, err := idField(h.body, "session_id")
	if err != nil {
		return err
	}
	if h.op.action != "response" {
		for _, field := range []string{"conversation", "previous_response_id"} {
			if h.body[field] != nil {
				return protocol.ErrResourceConflict
			}
		}
	}
	switch h.op.action {
	case "models", "capabilities":
		h.lease, err = h.g.Acquire(h.caller.ID, h.rt.Key, "", "", "proxy", h.rt)
		return err
	case "session.create", "session.fork":
		alias, e := idField(h.body, "id")
		if e != nil {
			return e
		}
		if sid != "" {
			if alias != "" && alias != sid {
				return protocol.ErrResourceConflict
			}
			alias = sid
		}
		if h.op.action == "session.fork" {
			if err = h.sessionHint(r, h.op.id); err != nil {
				return err
			}
			h.path = "/api/sessions/" + h.session.Upstream + "/fork"
		} else {
			for _, name := range []string{"X-Hermes-Session-Id", h.rt.Session.Header} {
				v, e := oneHeader(r.Header, name)
				if e != nil {
					return e
				}
				if v != "" {
					if alias != "" && alias != v {
						return protocol.ErrResourceConflict
					}
					alias = v
				}
			}
		}
		reservation, e := h.reserve(protocol.HermesSession, alias, "", h.op.action)
		if e != nil {
			return e
		}
		if !reservation.First {
			return protocol.ErrResourceConflict
		}
		h.session = reservation.Resource
		h.body["id"] = h.session.Upstream
		delete(h.body, "session_id")
	case "response":
		if sid != "" {
			return protocol.ErrResourceInvalid
		}
		previous, e := idField(h.body, "previous_response_id")
		if e != nil {
			return e
		}
		conversation, e := idField(h.body, "conversation")
		if e != nil {
			return e
		}
		if previous != "" && conversation != "" {
			return protocol.ErrResourceConflict
		}
		h.conversation = conversation
		if previous != "" {
			prev, e := h.lookup(protocol.HermesResponse, previous)
			if e != nil {
				return e
			}
			h.session, e = h.lookup(protocol.HermesSession, prev.ParentID)
			if e != nil {
				return e
			}
			if prev.Upstream == "" {
				return protocol.ErrResourceInUse
			}
			h.body["previous_response_id"] = prev.Upstream
		} else {
			alias := ""
			if conversation != "" {
				alias = protocol.NamespacedIdempotency(h.caller.ID, h.rt.ID, "conversation", conversation)
			}
			if alias != "" {
				h.session, err = h.lookup(protocol.HermesSession, alias)
				if err != nil && !errors.Is(err, protocol.ErrResourceNotFound) {
					return err
				}
			}
			if h.session.ID == "" {
				// Conversation identity is independent of the individual turn's prompt.
				reservation, e := h.g.ReserveResource(h.caller.ID, h.rt, protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: alias, Operation: "response.conversation", Fingerprint: "response.conversation"})
				if e != nil {
					return e
				}
				h.session = reservation.Resource
				if reservation.First {
					h.created = append(h.created, reservation.Resource)
				}
				if err = h.g.Resources.Update(h.session, protocol.ResourceLive); err != nil {
					return err
				}
			}
			if conversation != "" {
				h.body["conversation"] = protocol.NamespacedIdempotency(h.caller.ID, h.rt.ID, "response.conversation", h.session.ID)
			}
		}
		if err = h.sessionHint(r, ""); err != nil {
			return err
		}
		reservation, e := h.reserve(protocol.HermesResponse, "", "", "response")
		if e != nil {
			return e
		}
		h.execution = reservation.Resource
		h.executionNew = reservation.First
	default:
		if strings.HasPrefix(h.op.action, "run.") && h.op.action != "run.create" {
			h.execution, err = h.lookup(protocol.HermesRun, h.op.id)
			if err != nil {
				return err
			}
			h.session, err = h.lookup(protocol.HermesSession, h.execution.ParentID)
			if err != nil {
				return err
			}
			if h.execution.Upstream == "" {
				return protocol.ErrResourceInUse
			}
			h.path = "/v1/runs/" + h.execution.Upstream
			if h.op.suffix != "" {
				h.path += "/" + h.op.suffix
			}
		} else if h.op.id != "" {
			if err = h.sessionHint(r, h.op.id); err != nil {
				return err
			}
		}
		if err = h.sessionHint(r, sid); err != nil {
			return err
		}
		if h.session.ID == "" {
			return protocol.ErrResourceInvalid
		}
		if strings.HasPrefix(h.op.action, "session.") {
			h.path = "/api/sessions/" + h.session.Upstream
			if h.op.suffix != "" {
				h.path += "/" + h.op.suffix
			}
		}
		if sid != "" {
			h.body["session_id"] = h.session.Upstream
		}
		if h.op.action == "run.create" || h.op.action == "session.chat" || h.op.action == "session.chat/stream" || h.op.action == "completion" {
			if h.op.action == "run.create" && sid == "" {
				return protocol.ErrResourceInvalid
			}
			reservation, e := h.reserve(protocol.HermesRun, "", idem, h.op.action)
			if e != nil {
				return e
			}
			h.execution = reservation.Resource
			h.executionNew = reservation.First
			if idem != "" {
				h.headers.Set("Idempotency-Key", reservation.UpstreamIdempotencyKey)
			}
		}
	}
	if rawRun, e := idField(h.body, "run_id"); e != nil {
		return e
	} else if rawRun != "" {
		if rawRun != h.execution.ID && rawRun != h.op.id {
			return protocol.ErrResourceConflict
		}
		h.body["run_id"] = h.execution.Upstream
	}
	if h.headers.Get("X-Hermes-Session-Key") == "" {
		h.headers.Set("X-Hermes-Session-Key", protocol.NamespacedIdempotency(h.caller.ID, h.rt.ID, "memory", h.session.ID))
	}
	// Responses selects its chain using the body, not this header.
	if h.op.action != "response" {
		h.headers.Set("X-Hermes-Session-Id", h.session.Upstream)
	}
	kind, id := protocol.HermesSession, h.session.ID
	if h.execution.ID != "" {
		kind, id = h.execution.Kind, h.execution.ID
	}
	h.lease, _, err = h.g.AcquireResource(h.caller.ID, h.rt, kind, id)
	return err
}

func (h *hermesRequest) forward(w http.ResponseWriter, r *http.Request) {
	raw, _ := json.Marshal(h.body)
	var input io.Reader
	if h.body != nil {
		input = bytes.NewReader(raw)
	}
	target, err := protocol.UpstreamURL(h.lease.Interface.URL, h.path, h.query)
	if err != nil {
		hermesError(w, protocol.ErrResourceInvalid)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, input)
	if err != nil {
		hermesError(w, protocol.ErrResourceInvalid)
		return
	}
	req.Header = h.headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	h.g.UpstreamHeaders(req.Header, h.caller, h.lease.Interface)
	if h.execution.ID != "" && r.Method == "POST" {
		if err := h.g.Resources.Update(h.execution, protocol.ResourceUnknown); err != nil {
			hermesError(w, err)
			return
		}
	}
	if h.op.action == "session.create" || h.op.action == "session.fork" {
		if err := h.g.Resources.Update(h.session, protocol.ResourceUnknown); err != nil {
			hermesError(w, err)
			return
		}
	}
	client := &http.Client{Transport: h.g.Transport(h.rt.Timeout.ResponseHeaderMS), CheckRedirect: protocol.NoRedirect}
	h.sent = true
	res, err := client.Do(req)
	if err != nil {
		h.g.RecordFailure()
		code := 502
		if r.Context().Err() == context.DeadlineExceeded {
			code = 504
		}
		protocol.UpstreamProblem(w, code, "HERMES_UPSTREAM_UNAVAILABLE", protocol.DescribeUpstreamError(0, nil, err, h.lease.Interface.Token))
		return
	}
	defer res.Body.Close()
	idle := time.Duration(h.rt.Timeout.StreamIdleMS) * time.Millisecond
	// Apply the read timeout to error bodies as well as successful responses.
	body := protocol.NewIdleBody(res.Body, func() { res.Body.Close() }, idle)
	defer body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if res.StatusCode < 500 && r.Method == "POST" && h.executionNew {
			h.g.Resources.Update(h.execution, protocol.ResourceTerminal)
		}
		code := res.StatusCode
		if code >= 300 && code < 400 {
			code = 502
		}
		protocol.UpstreamProblem(w, code, "HERMES_UPSTREAM_ERROR", protocol.ReadUpstreamError(res.StatusCode, body, h.lease.Interface.Token))
		return
	}
	if strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		if err = h.observeHeader(res.Header); err != nil {
			hermesError(w, err)
			return
		}
		h.responseHeaders(w)
		h.stream(w, body, idle)
		return
	}
	raw, err = io.ReadAll(io.LimitReader(body, (4<<20)+1))
	if err != nil || len(raw) > 4<<20 {
		protocol.Problem(w, 502, "INVALID_HERMES_RESPONSE")
		return
	}
	value, err := protocol.JSONObject(raw)
	if err == nil {
		err = h.observe(value, "", res.Header)
	}
	if err != nil {
		protocol.Problem(w, 502, "INVALID_HERMES_RESPONSE")
		return
	}
	if h.op.action == "session.delete" {
		h.g.Resources.Update(h.session, protocol.ResourceTerminal)
	}
	h.responseHeaders(w)
	protocol.WriteJSON(w, res.StatusCode, value)
}

func (h *hermesRequest) responseHeaders(w http.ResponseWriter) {
	if h.session.ID != "" {
		w.Header().Set("X-Hermes-Session-Id", h.session.ID)
	}
}

func (h *hermesRequest) observeHeader(headers http.Header) error {
	if h.session.ID == "" {
		return nil
	}
	return h.g.Resources.ObserveHermes(h.session, h.execution, nil, headers.Get("X-Hermes-Session-Id"), "", "")
}

func hermesState(s string) protocol.ResourceState {
	switch s {
	case "completed", "failed", "cancelled", "canceled", "interrupted", "incomplete", "rejected":
		return protocol.ResourceTerminal
	case "waiting_for_approval", "input_required", "auth_required":
		return protocol.ResourceWaiting
	case "queued", "submitted", "running", "in_progress", "stopping":
		return protocol.ResourceLive
	}
	return protocol.ResourceUnknown
}

func (h *hermesRequest) capabilities(w http.ResponseWriter) {
	snapshot := h.lease.Capabilities()
	features := map[string]any{}
	if snapshot != nil && snapshot.Interface == h.lease.Interface {
		for k, v := range snapshot.Features {
			features[k] = v
		}
	}
	features["session_continuity_header"] = "X-Hermes-Session-Id"
	features["session_key_header"] = "X-Hermes-Session-Key"
	features["gateway_resource_ownership"] = true
	features["gateway_restart_recovery"] = false
	protocol.WriteJSON(w, 200, map[string]any{"object": "hermes.api_server.capabilities", "platform": "hermes-agent", "features": features, "endpoints": hermesEndpoints()})
}

func hermesEndpoints() map[string]any {
	endpoints := map[string]any{}
	for _, item := range []struct{ name, method, path string }{
		{"sessions", "GET", "/api/sessions"}, {"session_create", "POST", "/api/sessions"}, {"session", "GET", "/api/sessions/{session_id}"}, {"session_update", "PATCH", "/api/sessions/{session_id}"}, {"session_delete", "DELETE", "/api/sessions/{session_id}"}, {"session_messages", "GET", "/api/sessions/{session_id}/messages"}, {"session_fork", "POST", "/api/sessions/{session_id}/fork"}, {"session_chat", "POST", "/api/sessions/{session_id}/chat"}, {"session_chat_stream", "POST", "/api/sessions/{session_id}/chat/stream"},
		{"models", "GET", "/v1/models"}, {"chat_completions", "POST", "/v1/chat/completions"}, {"responses", "POST", "/v1/responses"}, {"runs", "POST", "/v1/runs"}, {"run_status", "GET", "/v1/runs/{run_id}"}, {"run_events", "GET", "/v1/runs/{run_id}/events"}, {"run_stop", "POST", "/v1/runs/{run_id}/stop"}, {"run_steer", "POST", "/v1/runs/{run_id}/steer"}, {"run_approval", "POST", "/v1/runs/{run_id}/approval"},
	} {
		endpoints[item.name] = map[string]string{"method": item.method, "path": item.path}
	}
	return endpoints
}

func (h *hermesRequest) abandonPrepared() {
	if !h.sent {
		_ = h.g.Resources.AbandonPrepared(h.created, h.op.action == "response")
	}
}
