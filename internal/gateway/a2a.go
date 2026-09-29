package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type upstreamKey struct{ Owner, Route, Endpoint, ID string }

type taskBinding struct {
	ID         string    `json:"gateway_task_id"`
	Upstream   string    `json:"upstream_task_id"`
	Owner      string    `json:"owner_id"`
	RouteID    string    `json:"route_id"`
	EndpointID string    `json:"endpoint_id"`
	SessionID  string    `json:"session_id"`
	Terminal   bool      `json:"terminal"`
	Seen       time.Time `json:"last_seen_at"`
}
type contextBinding struct {
	ID, Upstream, Owner, RouteID, EndpointID, SessionID string
	Seen                                                time.Time
}
type rpc struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  map[string]any  `json:"params"`
}

func rpcError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	writeJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}
func (g *Gateway) card(w http.ResponseWriter, r *http.Request, rt *Route) {
	if rt.Protocol != "a2a" {
		problem(w, 404, "NOT_FOUND")
		return
	}
	if g.opts.PublicURL == "" {
		problem(w, 503, "PUBLIC_URL_REQUIRED")
		return
	}
	var card map[string]any
	json.Unmarshal(rt.Card, &card)
	card["protocolVersion"] = "0.3.0"
	card["url"] = strings.TrimRight(g.opts.PublicURL, "/") + "/a2a/" + rt.Key
	card["preferredTransport"] = "JSONRPC"
	card["capabilities"] = map[string]any{"streaming": supportsStreaming(rt), "pushNotifications": false, "stateTransitionHistory": false}
	card["securitySchemes"] = map[string]any{"gatewayBearer": map[string]any{"type": "http", "scheme": "bearer"}}
	card["security"] = []any{map[string]any{"gatewayBearer": []string{}}}
	delete(card, "additionalInterfaces")
	delete(card, "signatures")
	card["supportsAuthenticatedExtendedCard"] = false
	writeJSON(w, 200, card)
}
func supportsStreaming(rt *Route) bool {
	var card struct {
		Capabilities struct {
			Streaming bool `json:"streaming"`
		} `json:"capabilities"`
	}
	_ = json.Unmarshal(rt.Card, &card)
	return card.Capabilities.Streaming
}
func (g *Gateway) a2a(w http.ResponseWriter, r *http.Request, c Caller, rt *Route) {
	if r.Method != "POST" {
		problem(w, 405, "METHOD_NOT_ALLOWED")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		rpcError(w, nil, -32600, "Request too large")
		return
	}
	var q rpc
	if decode(raw, &q) != nil || q.JSONRPC != "2.0" || q.Params == nil || len(q.ID) == 0 {
		rpcError(w, q.ID, -32600, "Invalid request")
		return
	}
	if q.Method != "message/send" && q.Method != "message/stream" && q.Method != "tasks/get" && q.Method != "tasks/cancel" && q.Method != "tasks/resubscribe" {
		rpcError(w, q.ID, -32601, "Method not supported")
		return
	}
	if (q.Method == "message/stream" || q.Method == "tasks/resubscribe") && !supportsStreaming(rt) {
		rpcError(w, q.ID, -32004, "Streaming not supported by this route")
		return
	}
	var requestID any
	dec := json.NewDecoder(bytes.NewReader(q.ID))
	dec.UseNumber()
	_ = dec.Decode(&requestID)
	switch requestID.(type) {
	case string, json.Number:
	default:
		rpcError(w, nil, -32600, "Invalid request ID")
		return
	}
	sid, err := sessionID(r, rt)
	if err != nil {
		rpcError(w, q.ID, -32602, "Invalid session ID")
		return
	}
	forced := ""
	taskID := ""
	contextID := ""
	var msg map[string]any
	if strings.HasPrefix(q.Method, "message/") {
		msg, _ = q.Params["message"].(map[string]any)
		messageID, _ := msg["messageId"].(string)
		if msg == nil || msg["kind"] != "message" || messageID == "" {
			rpcError(w, q.ID, -32602, "Message required")
			return
		}
		if role, _ := msg["role"].(string); role != "user" && role != "agent" {
			rpcError(w, q.ID, -32602, "Invalid message role")
			return
		}
		if _, ok := msg["parts"].([]any); !ok {
			rpcError(w, q.ID, -32602, "Message parts required")
			return
		}
		taskID, _ = msg["taskId"].(string)
		contextID, _ = msg["contextId"].(string)
		if contextID != "" && !rt.Session.AcceptA2AContext {
			rpcError(w, q.ID, -32602, "Context routing disabled")
			return
		}
		if cfg, ok := q.Params["configuration"].(map[string]any); ok {
			if _, exists := cfg["pushNotificationConfig"]; exists {
				rpcError(w, q.ID, -32003, "Push notifications not supported")
				return
			}
		}
	} else {
		taskID, _ = q.Params["id"].(string)
		if taskID == "" {
			rpcError(w, q.ID, -32602, "Task ID required")
			return
		}
	}
	g.a2aMu.Lock()
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
		t := g.tasks[taskID]
		if t == nil || t.Owner != c.ID || t.RouteID != rt.ID {
			valid = false
			code = -32001
			why = "Task not found"
		} else {
			adopt(t.EndpointID, t.SessionID)
			t.Seen = time.Now()
			if msg != nil {
				msg["taskId"] = t.Upstream
			} else {
				q.Params["id"] = t.Upstream
			}
		}
	}
	if contextID != "" {
		b := g.contexts[contextID]
		if b == nil || b.Owner != c.ID || b.RouteID != rt.ID {
			valid = false
		} else {
			adopt(b.EndpointID, b.SessionID)
			b.Seen = time.Now()
			msg["contextId"] = b.Upstream
		}
	}
	if msg != nil {
		if refs, ok := msg["referenceTaskIds"].([]any); ok {
			for i, v := range refs {
				id, _ := v.(string)
				t := g.tasks[id]
				if t == nil || t.Owner != c.ID || t.RouteID != rt.ID {
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
	g.a2aMu.Unlock()
	if !valid {
		rpcError(w, q.ID, code, why)
		return
	}
	if sid == "" {
		sid = newID("as_")
	}
	l, err := g.acquire(c.ID, rt.Key, sid, forced)
	if err != nil {
		rpcError(w, q.ID, -32603, err.Error())
		return
	}
	defer l.release()
	payload, _ := json.Marshal(q)
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(l.route.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", l.ep.URL, bytes.NewReader(payload))
	if err != nil {
		rpcError(w, q.ID, -32603, "Invalid upstream")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	upstreamHeaders(req.Header, c, l.ep)
	res, err := g.transport(l.route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		g.failures.Add(1)
		rpcError(w, q.ID, -32603, "Upstream unavailable")
		return
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		rpcError(w, q.ID, -32603, "Upstream HTTP failure")
		return
	}
	idle := time.Duration(l.route.Timeout.StreamIdleMS) * time.Millisecond
	body := newIdleBody(res.Body, cancel, idle)
	defer body.Close()
	transform := func(b []byte) ([]byte, error) {
		var env map[string]any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		if d.Decode(&env) != nil || env["jsonrpc"] != "2.0" {
			return nil, fmt.Errorf("invalid upstream JSON-RPC response")
		}
		result, ok := env["result"].(map[string]any)
		if ok {
			g.a2aMu.Lock()
			err := g.mapResult(result, c.ID, l.route.ID, l.ep.ID, sid)
			g.a2aMu.Unlock()
			if err != nil {
				return nil, err
			}
		} else if env["error"] == nil {
			return nil, fmt.Errorf("missing result")
		}
		env["id"] = q.ID
		return json.Marshal(env)
	}
	if strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		if q.Method != "message/stream" && q.Method != "tasks/resubscribe" {
			rpcError(w, q.ID, -32603, "Unexpected stream")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		var data []string
		size := 0
		streamError := func(message string) {
			payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "error": map[string]any{"code": -32603, "message": message}})
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(idle))
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			_ = http.NewResponseController(w).Flush()
		}
		send := func() bool {
			if len(data) == 0 {
				return true
			}
			b, err := transform([]byte(strings.Join(data, "\n")))
			data = nil
			size = 0
			if err != nil {
				streamError(err.Error())
				return false
			}
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(idle))
			if _, err = fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return false
			}
			return http.NewResponseController(w).Flush() == nil
		}
		defer http.NewResponseController(w).SetWriteDeadline(time.Time{})
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if !send() {
					g.failures.Add(1)
					return
				}
			} else if strings.HasPrefix(line, "data:") {
				part := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
				size += len(part)
				if size > 2<<20 {
					streamError("Upstream event too large")
					g.failures.Add(1)
					return
				}
				data = append(data, part)
			} else if strings.HasPrefix(line, ":") {
				_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(idle))
				if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
					return
				}
				_ = http.NewResponseController(w).Flush()
			}
		}
		if scanner.Err() != nil {
			if ctx.Err() == nil {
				streamError("Upstream stream interrupted")
			}
			g.failures.Add(1)
		}
		return
	}
	raw, err = io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		rpcError(w, q.ID, -32603, "Invalid upstream response")
		return
	}
	out, err := transform(raw)
	if err != nil {
		rpcError(w, q.ID, -32603, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

// Only traverse protocol-owned objects, never arbitrary data parts or metadata.
func (g *Gateway) mapResult(m map[string]any, owner, route, ep, sid string) error {
	kind, _ := m["kind"].(string)
	if kind != "message" && kind != "task" && kind != "status-update" && kind != "artifact-update" {
		return fmt.Errorf("unsupported result kind")
	}
	ctxID, _ := m["contextId"].(string)
	if ctxID != "" {
		key := upstreamKey{owner, route, ep, ctxID}
		b := g.contexts[g.contextReverse[key]]
		if b == nil {
			if len(g.contexts) >= g.opts.MaxSessions {
				return fmt.Errorf("context capacity exceeded")
			}
			b = &contextBinding{ID: newID("ctx_"), Upstream: ctxID, Owner: owner, RouteID: route, EndpointID: ep, SessionID: sid}
			g.contexts[b.ID] = b
			g.contextReverse[key] = b.ID
		}
		if b.SessionID != sid {
			return fmt.Errorf("upstream context belongs to another session")
		}
		b.Seen = time.Now()
		m["contextId"] = b.ID
	}
	taskField := "taskId"
	if kind == "task" {
		taskField = "id"
	}
	tid, _ := m[taskField].(string)
	if tid != "" {
		key := upstreamKey{owner, route, ep, tid}
		t := g.tasks[g.taskReverse[key]]
		if t == nil {
			if len(g.tasks) >= g.opts.MaxTasks {
				return fmt.Errorf("task capacity exceeded")
			}
			t = &taskBinding{ID: newID("task_"), Upstream: tid, Owner: owner, RouteID: route, EndpointID: ep, SessionID: sid}
			g.tasks[t.ID] = t
			g.taskReverse[key] = t.ID
		}
		if t.SessionID != sid {
			return fmt.Errorf("upstream task belongs to another session")
		}
		t.Seen = time.Now()
		m[taskField] = t.ID
		if status, ok := m["status"].(map[string]any); ok {
			state, _ := status["state"].(string)
			t.Terminal = state == "completed" || state == "failed" || state == "canceled" || state == "rejected"
		}
	}
	if status, ok := m["status"].(map[string]any); ok {
		if message, ok := status["message"].(map[string]any); ok {
			if err := g.mapResult(message, owner, route, ep, sid); err != nil {
				return err
			}
		}
	}
	if history, ok := m["history"].([]any); ok {
		for _, v := range history {
			if msg, ok := v.(map[string]any); ok {
				if err := g.mapResult(msg, owner, route, ep, sid); err != nil {
					return err
				}
			}
		}
	}
	if refs, ok := m["referenceTaskIds"].([]any); ok {
		for i, v := range refs {
			up, _ := v.(string)
			found := false
			for _, t := range g.tasks {
				if t.Owner == owner && t.RouteID == route && t.EndpointID == ep && t.Upstream == up {
					refs[i] = t.ID
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unmapped referenced task")
			}
		}
	}
	return nil
}
func (g *Gateway) cleanupA2A() {
	g.a2aMu.Lock()
	defer g.a2aMu.Unlock()
	now := time.Now()
	for id, t := range g.tasks {
		if t.Terminal && now.Sub(t.Seen) > 24*time.Hour {
			delete(g.taskReverse, upstreamKey{t.Owner, t.RouteID, t.EndpointID, t.Upstream})
			delete(g.tasks, id)
		}
	}
	for id, c := range g.contexts {
		if now.Sub(c.Seen) <= 24*time.Hour {
			continue
		}
		used := false
		for _, t := range g.tasks {
			if t.RouteID == c.RouteID && t.Owner == c.Owner && t.SessionID == c.SessionID {
				used = true
				break
			}
		}
		if !used && g.state(c.RouteID, c.EndpointID).active.Load() == 0 {
			delete(g.contextReverse, upstreamKey{c.Owner, c.RouteID, c.EndpointID, c.Upstream})
			delete(g.contexts, id)
		}
	}
}
