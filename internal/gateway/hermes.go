package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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
	if len(p) >= 3 && p[0] == "api" && p[1] == "sessions" && validResourceID(p[2]) {
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
	if len(p) >= 3 && p[0] == "v1" && p[1] == "runs" && validResourceID(p[2]) {
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

func hermesURL(base, path, query string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	if u.RawQuery != "" && query != "" {
		u.RawQuery += "&" + query
	} else if query != "" {
		u.RawQuery = query
	}
	return u.String(), nil
}
func hermesJSON(raw []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errResourceInvalid
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return nil, errResourceInvalid
	}
	return v, nil
}
func hermesError(w http.ResponseWriter, err error) {
	code := 503
	switch {
	case errors.Is(err, errResourceNotFound):
		code = 404
	case errors.Is(err, errResourceConflict), errors.Is(err, errResourceInUse):
		code = 409
	case errors.Is(err, errResourceInvalid):
		code = 400
	}
	problem(w, code, err.Error())
}
func oneHeader(h http.Header, name string) (string, error) {
	values := h.Values(name)
	if len(values) > 1 {
		return "", errResourceInvalid
	}
	if len(values) == 0 {
		return "", nil
	}
	if len(values[0]) > 256 || strings.ContainsAny(values[0], "\r\n\x00") {
		return "", errResourceInvalid
	}
	return values[0], nil
}
func idField(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok || !validResourceID(s) {
		return "", errResourceInvalid
	}
	return s, nil
}

type hermesRequest struct {
	g                  *Gateway
	caller             Caller
	rt                 *Route
	op                 hermesOperation
	session, execution resource
	body               map[string]any
	path, query        string
	headers            http.Header
	lease              *lease
	terminal           bool
	conversation       string
	fingerprint        string
	created            []resource
	sent               bool
	executionNew       bool
}

func (g *Gateway) hermes(w http.ResponseWriter, r *http.Request, c Caller, rt *Route, path string) {
	op, ok := hermesPath(r.Method, path)
	if !ok {
		problem(w, 501, "HERMES_OPERATION_NOT_SUPPORTED")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(rt.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	r = r.WithContext(ctx)
	h := &hermesRequest{g: g, caller: c, rt: rt, op: op, path: path, query: r.URL.RawQuery, headers: make(http.Header)}
	if r.Method == "POST" || r.Method == "PATCH" {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if err != nil {
			problem(w, 413, "REQUEST_TOO_LARGE")
			return
		}
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		h.body, err = hermesJSON(raw)
		if err != nil {
			problem(w, 400, "INVALID_JSON")
			return
		}
	}
	rawFingerprint, _ := json.Marshal(h.body)
	h.fingerprint = dataFingerprint(r.Method, path, rawFingerprint, r.Header, "X-Hermes-Session-Id", rt.Session.Header, "X-Hermes-Session-Key")
	// Discovery never expands the supported path table or route exposure.
	g.discoverHermes(ctx, rt)
	if op.action == "session.list" {
		g.listHermesSessions(w, r, c, rt)
		return
	}
	defer h.abandonPrepared()
	if err := h.prepare(r); err != nil {
		hermesError(w, err)
		return
	}
	defer h.lease.release()
	if op.action == "capabilities" {
		h.capabilities(w)
		return
	}
	h.forward(w, r)
}

func (h *hermesRequest) lookup(kind resourceKind, id string) (resource, error) {
	if !validResourceID(id) {
		return resource{}, errResourceInvalid
	}
	res, err := h.g.resources.lookup(resourceKey{h.caller.ID, h.rt.ID, kind, id})
	if err == nil && kind == hermesSession && res.State == resourceTerminal {
		return resource{}, errResourceNotFound
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
		s, err := h.lookup(hermesSession, ref)
		if err != nil {
			return err
		}
		if h.session.ID != "" && h.session.ID != s.ID {
			return errResourceConflict
		}
		h.session = s
	}
	return nil
}
func (h *hermesRequest) reserve(kind resourceKind, alias, idem, operation string) (creationReservation, error) {
	spec := resourceCreation{Kind: kind, Alias: alias, Operation: operation, IdempotencyKey: idem, Fingerprint: h.fingerprint}
	if h.session.ID != "" {
		spec.ParentKind = hermesSession
		spec.ParentID = h.session.ID
	}
	reservation, err := h.g.reserveResource(h.caller.ID, h.rt, spec)
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
		return errResourceInvalid
	}
	for _, name := range []string{"Accept", "Last-Event-ID"} {
		h.headers[name] = r.Header.Values(name)
	}
	if key != "" {
		h.headers.Set("X-Hermes-Session-Key", namespacedIdempotency(h.caller.ID, h.rt.ID, "memory", key))
	}
	// Body references must agree with path/header references, never override them.
	sid, err := idField(h.body, "session_id")
	if err != nil {
		return err
	}
	if h.op.action != "response" {
		for _, field := range []string{"conversation", "previous_response_id"} {
			if h.body[field] != nil {
				return errResourceConflict
			}
		}
	}
	switch h.op.action {
	case "models", "capabilities":
		h.lease, err = h.g.acquire(h.caller.ID, h.rt.Key, "", "", "proxy", h.rt)
		return err
	case "session.create", "session.fork":
		alias, e := idField(h.body, "id")
		if e != nil {
			return e
		}
		if sid != "" {
			if alias != "" && alias != sid {
				return errResourceConflict
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
						return errResourceConflict
					}
					alias = v
				}
			}
		}
		reservation, e := h.reserve(hermesSession, alias, "", h.op.action)
		if e != nil {
			return e
		}
		if !reservation.First {
			return errResourceConflict
		}
		h.session = reservation.Resource
		h.body["id"] = h.session.Upstream
		delete(h.body, "session_id")
	case "response":
		if sid != "" {
			return errResourceInvalid
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
			return errResourceConflict
		}
		h.conversation = conversation
		if previous != "" {
			prev, e := h.lookup(hermesResponse, previous)
			if e != nil {
				return e
			}
			h.session, e = h.lookup(hermesSession, prev.ParentID)
			if e != nil {
				return e
			}
			if prev.Upstream == "" {
				return errResourceInUse
			}
			h.body["previous_response_id"] = prev.Upstream
		} else {
			alias := ""
			if conversation != "" {
				alias = namespacedIdempotency(h.caller.ID, h.rt.ID, "conversation", conversation)
			}
			if alias != "" {
				h.session, err = h.lookup(hermesSession, alias)
				if err != nil && !errors.Is(err, errResourceNotFound) {
					return err
				}
			}
			if h.session.ID == "" {
				// Conversation identity is independent of the individual turn's prompt.
				reservation, e := h.g.reserveResource(h.caller.ID, h.rt, resourceCreation{Kind: hermesSession, Alias: alias, Operation: "response.conversation", Fingerprint: "response.conversation"})
				if e != nil {
					return e
				}
				h.session = reservation.Resource
				if reservation.First {
					h.created = append(h.created, reservation.Resource)
				}
				if err = h.g.resources.update(h.session, resourceLive); err != nil {
					return err
				}
			}
			if conversation != "" {
				h.body["conversation"] = namespacedIdempotency(h.caller.ID, h.rt.ID, "response.conversation", h.session.ID)
			}
		}
		if err = h.sessionHint(r, ""); err != nil {
			return err
		}
		reservation, e := h.reserve(hermesResponse, "", "", "response")
		if e != nil {
			return e
		}
		h.execution = reservation.Resource
		h.executionNew = reservation.First
	default:
		if strings.HasPrefix(h.op.action, "run.") && h.op.action != "run.create" {
			h.execution, err = h.lookup(hermesRun, h.op.id)
			if err != nil {
				return err
			}
			h.session, err = h.lookup(hermesSession, h.execution.ParentID)
			if err != nil {
				return err
			}
			if h.execution.Upstream == "" {
				return errResourceInUse
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
			return errResourceInvalid
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
				return errResourceInvalid
			}
			reservation, e := h.reserve(hermesRun, "", idem, h.op.action)
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
			return errResourceConflict
		}
		h.body["run_id"] = h.execution.Upstream
	}
	if h.headers.Get("X-Hermes-Session-Key") == "" {
		h.headers.Set("X-Hermes-Session-Key", namespacedIdempotency(h.caller.ID, h.rt.ID, "memory", h.session.ID))
	}
	// Responses selects its chain using the body, not this header.
	if h.op.action != "response" {
		h.headers.Set("X-Hermes-Session-Id", h.session.Upstream)
	}
	kind, id := hermesSession, h.session.ID
	if h.execution.ID != "" {
		kind, id = h.execution.Kind, h.execution.ID
	}
	h.lease, _, err = h.g.acquireResource(h.caller.ID, h.rt, kind, id)
	return err
}

func (h *hermesRequest) forward(w http.ResponseWriter, r *http.Request) {
	raw, _ := json.Marshal(h.body)
	var input io.Reader
	if h.body != nil {
		input = bytes.NewReader(raw)
	}
	target, err := hermesURL(h.lease.iface.URL, h.path, h.query)
	if err != nil {
		hermesError(w, errResourceInvalid)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, input)
	if err != nil {
		hermesError(w, errResourceInvalid)
		return
	}
	req.Header = h.headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	upstreamHeaders(req.Header, h.caller, h.lease.iface)
	if h.execution.ID != "" && r.Method == "POST" {
		if err := h.g.resources.update(h.execution, resourceUnknown); err != nil {
			hermesError(w, err)
			return
		}
	}
	if h.op.action == "session.create" || h.op.action == "session.fork" {
		if err := h.g.resources.update(h.session, resourceUnknown); err != nil {
			hermesError(w, err)
			return
		}
	}
	client := &http.Client{Transport: h.g.transport(h.rt.Timeout.ResponseHeaderMS), CheckRedirect: noRedirect}
	h.sent = true
	res, err := client.Do(req)
	if err != nil {
		h.g.failures.Add(1)
		code := 502
		if r.Context().Err() == context.DeadlineExceeded {
			code = 504
		}
		problem(w, code, "HERMES_UPSTREAM_UNAVAILABLE")
		return
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if res.StatusCode < 500 && r.Method == "POST" && h.executionNew {
			h.g.resources.update(h.execution, resourceTerminal)
		}
		// Error text can contain upstream IDs or provider credentials.
		code := res.StatusCode
		if code >= 300 && code < 400 {
			code = 502
		}
		problem(w, code, "HERMES_UPSTREAM_ERROR")
		return
	}
	idle := time.Duration(h.rt.Timeout.StreamIdleMS) * time.Millisecond
	// The request context cancellation interrupts both reads and the model call.
	// Close also releases the transport body if a stream is abandoned.
	body := newIdleBody(res.Body, func() { res.Body.Close() }, idle)
	defer body.Close()
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
		problem(w, 502, "INVALID_HERMES_RESPONSE")
		return
	}
	value, err := hermesJSON(raw)
	if err == nil {
		err = h.observe(value, "", res.Header)
	}
	if err != nil {
		problem(w, 502, "INVALID_HERMES_RESPONSE")
		return
	}
	if h.op.action == "session.delete" {
		h.g.resources.update(h.session, resourceTerminal)
	}
	h.responseHeaders(w)
	writeJSON(w, res.StatusCode, value)
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
	return h.g.resources.observeHermes(h.session, h.execution, nil, headers.Get("X-Hermes-Session-Id"), "", "")
}

func hermesState(s string) resourceState {
	switch s {
	case "completed", "failed", "cancelled", "canceled", "interrupted", "incomplete", "rejected":
		return resourceTerminal
	case "waiting_for_approval", "input_required", "auth_required":
		return resourceWaiting
	case "queued", "submitted", "running", "in_progress", "stopping":
		return resourceLive
	}
	return resourceUnknown
}
func (h *hermesRequest) capabilities(w http.ResponseWriter) {
	snapshot := h.lease.state.proxy.discovery.Load()
	features := map[string]any{}
	for k, v := range snapshot.Features {
		features[k] = v
	}
	features["session_continuity_header"] = "X-Hermes-Session-Id"
	features["session_key_header"] = "X-Hermes-Session-Key"
	features["gateway_resource_ownership"] = true
	features["gateway_restart_recovery"] = false
	writeJSON(w, 200, map[string]any{"object": "hermes.api_server.capabilities", "platform": "hermes-agent", "features": features, "endpoints": hermesEndpoints()})
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
