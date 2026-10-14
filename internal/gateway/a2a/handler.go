// Package a2a handles A2A 1.0 requests, resource mapping and event streams.
package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

type Handler struct{ *protocol.Backend }

func New(backend *protocol.Backend) *Handler { return &Handler{backend} }

type RPC struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  map[string]any  `json:"params"`
}

func RPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	writeRPCError(w, id, map[string]any{"code": code, "message": msg})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, detail map[string]any) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(id))
	decoder.UseNumber()
	_ = decoder.Decode(&value)
	switch value.(type) {
	case string, json.Number:
	default:
		id = json.RawMessage("null")
	}
	protocol.WriteJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "error": detail})
}

func (g *Handler) Card(w http.ResponseWriter, r *http.Request, rt *protocol.Route) {
	if !protocol.Exposes(rt, "a2a") {
		protocol.Problem(w, 404, "NOT_FOUND")
		return
	}
	if g.PublicURL() == "" {
		protocol.Problem(w, 503, "PUBLIC_URL_REQUIRED")
		return
	}

	g.DiscoverA2A(r.Context(), rt)
	card := map[string]any{"name": rt.Key, "description": rt.Key, "version": "1.0.0", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}}
	if len(rt.Card) > 0 {
		var source map[string]any
		_ = json.Unmarshal(rt.Card, &source)
		for _, field := range []string{"name", "description", "version", "skills", "defaultInputModes", "defaultOutputModes", "provider", "documentationUrl", "iconUrl"} {
			if value, ok := source[field]; ok {
				card[field] = value
			}
		}
	}
	card["supportedInterfaces"] = []any{map[string]any{"url": strings.TrimRight(g.PublicURL(), "/") + "/agents/" + rt.Key + "/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}}
	card["capabilities"] = map[string]any{"streaming": supportsStreaming(rt), "pushNotifications": false, "extendedAgentCard": false}
	card["securitySchemes"] = map[string]any{"gatewayBearer": map[string]any{"httpAuthSecurityScheme": map[string]any{"scheme": "Bearer"}}}
	card["securityRequirements"] = []any{map[string]any{"schemes": map[string]any{"gatewayBearer": map[string]any{"list": []string{}}}}}
	for _, field := range []string{"protocolVersion", "url", "preferredTransport", "security", "additionalInterfaces", "signatures", "supportsAuthenticatedExtendedCard"} {
		delete(card, field)
	}
	// Skills are descriptive; upstream authentication declarations do not apply to the gateway.
	if skills, ok := card["skills"].([]any); ok {
		for _, skill := range skills {
			if m, ok := skill.(map[string]any); ok {
				delete(m, "security")
				delete(m, "securityRequirements")
			}
		}
	}
	protocol.WriteJSON(w, 200, card)
}

func supportsStreaming(rt *protocol.Route) bool {
	var card struct {
		Capabilities struct {
			Streaming bool `json:"streaming"`
		} `json:"capabilities"`
	}
	_ = json.Unmarshal(rt.Card, &card)
	return card.Capabilities.Streaming
}

func (g *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, c protocol.Caller, rt *protocol.Route) {
	if r.Method != "POST" {
		protocol.Problem(w, 405, "METHOD_NOT_ALLOWED")
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		RPCError(w, nil, -32600, "Request too large")
		return
	}
	var q RPC
	if !json.Valid(raw) {
		RPCError(w, nil, -32700, "Invalid JSON payload")
		return
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if d.Decode(&q) != nil || q.JSONRPC != "2.0" || q.Params == nil || len(q.ID) == 0 {
		RPCError(w, q.ID, -32600, "Invalid request")
		return
	}
	var requestID any
	dec := json.NewDecoder(bytes.NewReader(q.ID))
	dec.UseNumber()
	_ = dec.Decode(&requestID)
	switch requestID.(type) {
	case string, json.Number:
	default:
		RPCError(w, nil, -32600, "Invalid request ID")
		return
	}
	if r.Header.Get("A2A-Version") != "1.0" {
		RPCError(w, q.ID, -32009, "Only A2A 1.0 is supported; send A2A-Version: 1.0")
		return
	}
	switch q.Method {
	case "SendMessage", "SendStreamingMessage", "GetTask", "ListTasks", "CancelTask", "SubscribeToTask":
	case "CreateTaskPushNotificationConfig", "GetTaskPushNotificationConfig", "ListTaskPushNotificationConfigs", "DeleteTaskPushNotificationConfig":
		RPCError(w, q.ID, -32003, "Push notifications not supported")
		return
	case "GetExtendedAgentCard":
		RPCError(w, q.ID, -32007, "Extended agent card not configured")
		return
	default:
		RPCError(w, q.ID, -32601, "Method not found")
		return
	}
	if isStreaming(q.Method) && !supportsStreaming(rt) {
		RPCError(w, q.ID, -32004, "Streaming not supported")
		return
	}
	if err := validateParams(q); err != nil {
		RPCError(w, q.ID, -32602, err.Error())
		return
	}
	g.DiscoverA2A(r.Context(), rt)
	if q.Method == "ListTasks" {
		g.listA2ATasks(w, r, c, rt, q)
		return
	}
	sid, err := protocol.SessionID(r, rt)
	if err != nil {
		RPCError(w, q.ID, -32602, "Invalid session ID")
		return
	}
	forced := ""
	taskID := ""
	contextID := ""
	var msg map[string]any
	if isSend(q.Method) {
		msg = q.Params["message"].(map[string]any)
		taskID, _ = msg["taskId"].(string)
		contextID, _ = msg["contextId"].(string)
		if contextID != "" && !rt.Session.AcceptA2AContext {
			RPCError(w, q.ID, -32602, "Context routing disabled")
			return
		}
		if cfg, ok := q.Params["configuration"].(map[string]any); ok {
			if cfg["taskPushNotificationConfig"] != nil {
				RPCError(w, q.ID, -32003, "Push notifications not supported")
				return
			}
		}
	} else {
		taskID, _ = q.Params["id"].(string)
	}
	var boundTask protocol.Resource
	releaseRefs := []func(){}
	defer func() {
		for _, release := range releaseRefs {
			release()
		}
	}()
	pin := func(kind protocol.ResourceKind, id string) (protocol.Resource, bool) {
		res, release, err := g.Resources.Pin(protocol.ResourceKey{Owner: c.ID, Route: rt.ID, Kind: kind, ID: id})
		if err != nil {
			return protocol.Resource{}, false
		}
		releaseRefs = append(releaseRefs, release)
		return res, true
	}
	valid := true
	code := -32602
	why := "Context cannot be recovered"
	adopt := func(ep, session string) {
		if forced != "" && forced != ep {
			valid = false
			why = "Conflicting bindings"
		}
		if sid != "" && sid != session {
			valid = false
			why = "Conflicting session"
		}
		forced = ep
		sid = session
	}
	if taskID != "" {
		t, ok := pin(protocol.A2ATask, taskID)
		if !ok {
			valid = false
			code = -32001
			why = "Task not found"
		} else {
			boundTask = t
			if t.State == protocol.ResourceTerminal && (isSend(q.Method) || q.Method == "SubscribeToTask") {
				RPCError(w, q.ID, -32004, "Task is terminal")
				return
			}
			if t.State == protocol.ResourceTerminal && q.Method == "CancelTask" {
				RPCError(w, q.ID, -32002, "Task is not cancelable")
				return
			}
			adopt(t.EndpointID, t.SessionID)
			if msg != nil {
				msg["taskId"] = t.Upstream
			} else {
				q.Params["id"] = t.Upstream
			}
		}
	}
	if contextID != "" {
		b, ok := pin(protocol.A2AContext, contextID)
		if !ok {
			valid = false
		} else {
			if boundTask.ID != "" && boundTask.ParentID != b.ID {
				RPCError(w, q.ID, -32602, "Task and context do not match")
				return
			}
			adopt(b.EndpointID, b.SessionID)
			msg["contextId"] = b.Upstream
		}
	}
	if msg != nil {
		if refs, ok := msg["referenceTaskIds"].([]any); ok {
			for i, v := range refs {
				id, _ := v.(string)
				t, ok := pin(protocol.A2ATask, id)
				if !ok {
					valid = false
					code = -32001
					why = "Referenced task not found"
					break
				}
				if forced != "" && forced != t.EndpointID {
					valid = false
					why = "Referenced task belongs to another endpoint"
					break
				}
				forced = t.EndpointID
				refs[i] = t.Upstream
			}
		}
	}
	if !valid {
		RPCError(w, q.ID, code, why)
		return
	}
	if sid == "" {
		sid = protocol.NewID("as_")
	}
	l, err := g.Acquire(c.ID, rt.Key, sid, forced, "a2a", rt)
	if err != nil {
		RPCError(w, q.ID, -32603, err.Error())
		return
	}
	defer func() { l.Release() }()
	if rt.AgentType == "hermes" && q.Method == "SendMessage" {
		cfg, _ := q.Params["configuration"].(map[string]any)
		if cfg["returnImmediately"] == true {
			g.hermesImmediate(w, r, c, l, q, sid)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(l.Route.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	var seed []byte
	if rt.AgentType == "hermes" && q.Method == "SubscribeToTask" {
		seed, err = g.hermesSubscriptionSeed(ctx, q, c, l)
		if err != nil {
			writeUpstreamRPCError(w, q.ID, "Cannot read subscribed task", err)
			return
		}
	}
	req, err := g.UpstreamRequest(ctx, q, c, l.Interface)
	if err != nil {
		RPCError(w, q.ID, -32603, "Invalid upstream")
		return
	}
	res, err := g.Transport(l.Route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		g.RecordFailure()
		writeUpstreamRPCError(w, q.ID, "Upstream unavailable", protocol.DescribeUpstreamError(0, nil, err, l.Interface.Token))
		return
	}
	defer res.Body.Close()
	idle := time.Duration(l.Route.Timeout.StreamIdleMS) * time.Millisecond
	body := protocol.NewIdleBody(res.Body, cancel, idle)
	defer body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		raw, err := io.ReadAll(io.LimitReader(body, (2<<20)+1))
		writeUpstreamRPCError(w, q.ID, "Upstream HTTP failure", upstreamHTTPError(res.StatusCode, raw, err, q.ID, l.Interface.Token))
		return
	}
	stream := &a2aStreamState{subscribe: q.Method == "SubscribeToTask", seed: seed}
	transform := func(b []byte) ([]byte, error) {
		env, err := decodeEnvelope(b, q.ID, l.Interface.Token)
		if err != nil {
			return nil, err
		}
		if env["error"] != nil {
			stream.done = true
			return json.Marshal(env)
		}
		result := env["result"].(map[string]any)
		node, kind, err := decodeResult(result, q.Method)
		if err != nil {
			return nil, err
		}
		if err = checkIdentity(node, kind, q); err != nil {
			return nil, err
		}
		if q.Method == "SendMessage" && kind == "task" {
			cfg, _ := q.Params["configuration"].(map[string]any)
			status, _ := node["status"].(map[string]any)
			state := TaskResourceState(protocol.StringField(status, "state"))
			if cfg["returnImmediately"] != true && state != protocol.ResourceTerminal && state != protocol.ResourceWaiting {
				return nil, fmt.Errorf("blocking send returned an unfinished task")
			}
		}
		if isStreaming(q.Method) {
			if err = stream.accept(node, kind); err != nil {
				return nil, err
			}
		}
		if err = g.MapObject(node, kind, c.ID, l.Route.ID, l.Endpoint.ID, sid); err != nil {
			return nil, err
		}
		trimHistory(node, q)
		return json.Marshal(env)
	}
	media, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if media == "text/event-stream" {
		if !isStreaming(q.Method) {
			RPCError(w, q.ID, -32006, "Unexpected stream")
			return
		}
		g.forwardA2AStream(w, body, q.ID, idle, stream, transform)
		return
	}
	raw, err = io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		RPCError(w, q.ID, -32006, "Invalid upstream response")
		return
	}
	if isStreaming(q.Method) {
		env, err := decodeEnvelope(raw, q.ID, l.Interface.Token)
		if err != nil || env["error"] == nil {
			RPCError(w, q.ID, -32006, "Expected SSE stream")
			return
		}
	}
	out, err := transform(raw)
	if err != nil {
		RPCError(w, q.ID, -32006, "Invalid upstream response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

func (g *Handler) UpstreamRequest(ctx context.Context, q RPC, c protocol.Caller, iface protocol.EndpointInterface) (*http.Request, error) {
	// Public routes have no tenant. Upstream tenant is deployment configuration only.
	params := make(map[string]any, len(q.Params)+1)
	for k, v := range q.Params {
		params[k] = v
	}
	delete(params, "tenant")
	if iface.Tenant != "" {
		params["tenant"] = iface.Tenant
	}
	q.Params = params
	payload, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", iface.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("A2A-Version", "1.0")
	g.UpstreamHeaders(req.Header, c, iface)
	return req, nil
}
