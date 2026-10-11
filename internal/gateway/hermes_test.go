package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type hermesFake struct {
	mu            sync.Mutex
	sessions      map[string]map[string]any
	runs          map[string]map[string]any
	idem          map[string]string
	keys          []string
	calls         int
	next          int
	loseRun       bool
	responses     map[string]string
	conversations map[string]string
}

func newHermesFake() *hermesFake {
	return &hermesFake{sessions: map[string]map[string]any{}, runs: map[string]map[string]any{}, idem: map[string]string{}, responses: map[string]string{}, conversations: map[string]string{}}
}
func (f *hermesFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	path := strings.TrimPrefix(r.URL.Path, "/p/alice")
	if path == "/v1/capabilities" {
		features := map[string]any{"runs_idempotency": map[string]any{"supported": true, "durable": true}, "jobs_admin": true}
		for _, key := range hermesFeatures {
			features[key] = true
		}
		writeJSON(w, 200, map[string]any{"features": features})
		return
	}
	var body map[string]any
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&body)
	}

	if path == "/v1/chat/completions" {
		sid := r.Header.Get("X-Hermes-Session-Id")
		if f.sessions[sid] == nil {
			writeJSON(w, 404, map[string]any{})
			return
		}
		f.next++
		rid := fmt.Sprintf("chatcmpl-%d", f.next)
		w.Header().Set("X-Hermes-Session-Id", sid)
		value := map[string]any{"id": rid, "object": "chat.completion", "choices": []any{map[string]any{"message": map[string]any{"content": "hello"}, "finish_reason": "stop"}}}
		if body["stream"] == true {
			value["object"] = "chat.completion.chunk"
			w.Header().Set("Content-Type", "text/event-stream")
			raw, _ := json.Marshal(value)
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
			return
		}
		writeJSON(w, 200, value)
		return
	}
	if path == "/v1/responses" {
		previous := stringField(body, "previous_response_id")
		conversation := stringField(body, "conversation")
		sid := f.responses[previous]
		if sid == "" {
			sid = f.conversations[conversation]
		}
		f.next++
		rid := fmt.Sprintf("resp-%d", f.next)
		if sid == "" {
			sid = fmt.Sprintf("response-session-%d", f.next)
			f.sessions[sid] = map[string]any{"id": sid, "source": "api_server"}
		}
		f.responses[rid] = sid
		if conversation != "" {
			f.conversations[conversation] = sid
		}
		w.Header().Set("X-Hermes-Session-Id", sid)
		value := map[string]any{"id": rid, "object": "response", "status": "completed", "conversation": conversation, "output": []any{map[string]any{"id": "business-id"}}}
		if previous != "" {
			value["previous_response_id"] = previous
		}
		if body["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			raw, _ := json.Marshal(map[string]any{"type": "response.completed", "response": value})
			fmt.Fprintf(w, "event: response.completed\ndata: %s\n\n", raw)
			return
		}
		writeJSON(w, 200, value)
		return
	}
	if path == "/api/sessions" && r.Method == "POST" || strings.HasSuffix(path, "/fork") {
		id := stringField(body, "id")
		if !strings.HasPrefix(id, "hgs_") {
			writeJSON(w, 400, map[string]string{"error": "unisolated"})
			return
		}
		if f.sessions[id] != nil {
			writeJSON(w, 409, map[string]string{"error": "exists"})
			return
		}
		s := map[string]any{"id": id, "source": "api_server", "title": body["title"], "metadata": map[string]any{"session_id": "business-id"}}
		if strings.HasSuffix(path, "/fork") {
			s["parent_session_id"] = strings.Split(path, "/")[3]
		}
		f.sessions[id] = s
		writeJSON(w, 201, map[string]any{"session": s})
		return
	}
	if strings.HasPrefix(path, "/api/sessions/") {
		parts := strings.Split(path, "/")
		id := parts[3]
		s := f.sessions[id]
		if s == nil {
			writeJSON(w, 404, map[string]any{})
			return
		}
		if len(parts) == 4 {
			if r.Method == "DELETE" {
				delete(f.sessions, id)
				writeJSON(w, 200, map[string]any{"id": id, "deleted": true})
				return
			}
			if r.Method == "PATCH" {
				s["title"] = body["title"]
			}
			writeJSON(w, 200, map[string]any{"session": s})
			return
		}
		if parts[4] == "messages" {
			child := id + "-compressed"
			f.sessions[child] = map[string]any{"id": child, "source": "api_server"}
			writeJSON(w, 200, map[string]any{"session_id": child, "data": []any{map[string]any{"session_id": id, "role": "user", "content": "business-id"}}})
			return
		}
		if parts[4] == "chat" {
			f.next++
			rid := fmt.Sprintf("native-%d", f.next)
			f.keys = append(f.keys, r.Header.Get("X-Hermes-Session-Key"))
			f.runs[rid] = map[string]any{"run_id": rid, "session_id": id, "status": "completed"}
			if len(parts) == 6 {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, ": real heartbeat\r\n\r\nevent: run.started\r\nid: 12\r\ndata: {\"session_id\":%q,\"run_id\":%q}\r\n\r\n", id, rid)
				fmt.Fprintf(w, "event: run.completed\ndata: {\"session_id\":%q,\"run_id\":%q,\"content\":\"business-id\"}\n\n", id, rid)
				fmt.Fprintf(w, "event: done\ndata: {}\n\n")
				return
			}
			w.Header().Set("X-Hermes-Session-Id", id)
			writeJSON(w, 200, map[string]any{"session_id": id, "message": map[string]any{"content": "business-id"}})
			return
		}
	}
	if path == "/v1/runs" && r.Method == "POST" {
		key := r.Header.Get("Idempotency-Key")
		rid := f.idem[key]
		if rid == "" || key == "" {
			f.next++
			rid = fmt.Sprintf("upstream-run-%d", f.next)
			f.idem[key] = rid
			f.runs[rid] = map[string]any{"run_id": rid, "session_id": body["session_id"], "status": "running"}
		}
		if f.loseRun {
			f.loseRun = false
			writeJSON(w, 502, map[string]any{"error": "lost"})
			return
		}
		writeJSON(w, 202, map[string]any{"run_id": rid, "status": f.runs[rid]["status"]})
		return
	}
	if strings.HasPrefix(path, "/v1/runs/") {
		parts := strings.Split(path, "/")
		run := f.runs[parts[3]]
		if run == nil {
			writeJSON(w, 404, map[string]any{})
			return
		}
		if len(parts) == 5 {
			switch parts[4] {
			case "stop":
				run["status"] = "stopping"
			case "approval":
				run["status"] = "running"
			case "events":
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "id: 21\nevent: run.completed\ndata: {\"run_id\":%q,\"session_id\":%q,\"status\":\"completed\"}\n\n", run["run_id"], run["session_id"])
				return
			}
		}
		writeJSON(w, 200, run)
		return
	}
	writeJSON(w, 501, map[string]any{"error": "unsupported"})
}
func createHermesRoute(t *testing.T, g *Gateway, urls ...string) Route {
	t.Helper()
	endpoints := []any{}
	for _, u := range urls {
		endpoints = append(endpoints, map[string]any{"interfaces": map[string]any{"proxy": map[string]any{"url": u}}})
	}
	body, _ := json.Marshal(map[string]any{"consumers": testConsumers(), "key": "test", "agent_type": "hermes", "expose": []string{"proxy"}, "endpoints": endpoints})
	w := admin(t, g, "POST", "/admin/v1/routes", string(body), "", "")
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var rt Route
	json.Unmarshal(w.Body.Bytes(), &rt)
	return rt
}
func hermesCall(g *Gateway, method, path, body, caller string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/agents/test"+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+caller+"-secret")
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, req)
	return w
}
func hermesCreated(t *testing.T, g *Gateway, caller, alias string) string {
	t.Helper()
	w := hermesCall(g, "POST", "/api/sessions", `{"id":"`+alias+`"}`, caller)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	v, _ := hermesJSON(w.Body.Bytes())
	return stringField(v["session"].(map[string]any), "id")
}
func TestHermesSessionsOwnershipLineageAndDrain(t *testing.T) {
	a, b := newHermesFake(), newHermesFake()
	sa, sb := newTestServer(a), newTestServer(b)
	defer sa.Close()
	defer sb.Close()
	g := setup(t)
	createHermesRoute(t, g, sa.URL+"/p/alice", sb.URL+"/p/alice")
	alice := hermesCreated(t, g, "alice", "same")
	bob := hermesCreated(t, g, "bob", "same")
	if alice == bob {
		t.Fatal("session ownership shared")
	}
	if w := hermesCall(g, "GET", "/api/sessions/"+alice, "", "bob"); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(g, "POST", "/api/sessions/"+alice+"/chat", `{"session_id":"unknown","input":"hi"}`, "alice"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	other := hermesCreated(t, g, "alice", "other")
	if w := hermesCall(g, "POST", "/api/sessions/"+alice+"/chat", `{"input":"hi"}`, "alice", "X-Hermes-Session-Id", other); w.Code != 409 {
		t.Fatal(w.Code)
	}
	history := hermesCall(g, "GET", "/api/sessions/"+alice+"/messages", "", "alice")
	if history.Code != 200 || strings.Contains(history.Body.String(), "hgs_") || !strings.Contains(history.Body.String(), "business-id") {
		t.Fatal(history.Body.String())
	}
	rt := g.current.Load().Routes["test"]
	ref, _ := g.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, alice})
	if !strings.HasSuffix(ref.Upstream, "-compressed") {
		t.Fatal("compression lineage missing")
	}
	ep := endpoint(rt, ref.EndpointID)
	patch := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID+"/endpoints/"+ep.ID, `{"state":"draining"}`, tag(ep.Version), "")
	if patch.Code != 200 {
		t.Fatal(patch.Body.String())
	}
	if w := hermesCall(g, "POST", "/api/sessions/"+alice+"/fork", `{}`, "alice"); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(g, "POST", "/api/sessions/"+alice+"/chat", `{"input":"hi"}`, "alice"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(g, "GET", "/api/sessions?limit=1", "", "alice"); w.Code != 200 || strings.Contains(w.Body.String(), bob) {
		t.Fatal(w.Body.String())
	}
	if g.inflight.Load() != 0 {
		t.Fatal("lease leak")
	}
}
func TestHermesStreamsRunsReplayAndReconcile(t *testing.T) {
	f := newHermesFake()
	server := newTestServer(f)
	defer server.Close()
	g := setup(t)
	createHermesRoute(t, g, server.URL)
	sid := hermesCreated(t, g, "alice", "native")
	stream := hermesCall(g, "POST", "/api/sessions/"+sid+"/chat/stream", `{"input":"hi"}`, "alice")
	if stream.Code != 200 || strings.Contains(stream.Body.String(), "native-1") || !strings.Contains(stream.Body.String(), "id: 12") || !strings.Contains(stream.Body.String(), ": real heartbeat") || strings.Contains(stream.Body.String(), "UPSTREAM_STREAM_INTERRUPTED") {
		t.Fatal(stream.Body.String())
	}
	f.mu.Lock()
	f.loseRun = true
	f.mu.Unlock()
	body := `{"session_id":"` + sid + `","input":"work"}`
	if w := hermesCall(g, "POST", "/v1/runs", body, "alice", "Idempotency-Key", "retry"); w.Code != 502 {
		t.Fatal(w.Code)
	}
	replay := hermesCall(g, "POST", "/v1/runs", body, "alice", "Idempotency-Key", "retry")
	if replay.Code != 202 {
		t.Fatal(replay.Code, replay.Body.String())
	}
	value, _ := hermesJSON(replay.Body.Bytes())
	runID := stringField(value, "run_id")
	if !strings.HasPrefix(runID, "run_") {
		t.Fatal(value)
	}
	again := hermesCall(g, "POST", "/v1/runs", body, "alice", "Idempotency-Key", "retry")
	if again.Body.String() != replay.Body.String() {
		t.Fatal("retry changed run")
	}
	if w := hermesCall(g, "POST", "/v1/runs", strings.Replace(body, "work", "different", 1), "alice", "Idempotency-Key", "retry"); w.Code != 409 {
		t.Fatal(w.Code)
	}
	if w := hermesCall(g, "GET", "/v1/runs/"+runID, "", "bob"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	stop := hermesCall(g, "POST", "/v1/runs/"+runID+"/stop", `{}`, "alice")
	if stop.Code != 200 {
		t.Fatal(stop.Body.String())
	}
	rt := g.current.Load().Routes["test"]
	ref, _ := g.resources.lookup(resourceKey{"alice", rt.ID, hermesRun, runID})
	if ref.State == resourceTerminal {
		t.Fatal("stop treated as completion")
	}
	if w := hermesCall(g, "POST", "/v1/runs/"+runID+"/approval", `{"request_id":"approval-test","choice":"deny"}`, "bob"); w.Code != 404 {
		t.Fatal("foreign approval accepted", w.Code)
	}
	if w := hermesCall(g, "POST", "/v1/runs/"+runID+"/approval", `{"request_id":"approval-test","choice":"deny"}`, "alice"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	f.mu.Lock()
	f.runs[ref.Upstream]["status"] = "completed"
	f.mu.Unlock()
	g.reconcileResource(t.Context(), ref)
	ref, _ = g.resources.lookup(ref.key())
	if ref.State != resourceTerminal {
		t.Fatal("background completion not observed")
	}
	if g.inflight.Load() != 0 {
		t.Fatal("inflight leaked")
	}
}

func TestHermesResponsesCompletionAndListIsolation(t *testing.T) {
	f := newHermesFake()
	s := newTestServer(f)
	defer s.Close()
	g := setup(t)
	createHermesRoute(t, g, s.URL)
	first := hermesCall(g, "POST", "/v1/responses", `{"input":"one","conversation":"named"}`, "alice")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	value, _ := hermesJSON(first.Body.Bytes())
	previous := stringField(value, "id")
	sid := first.Header().Get("X-Hermes-Session-Id")
	if !strings.HasPrefix(previous, "response_") || value["conversation"] != "named" {
		t.Fatal(value)
	}
	next := hermesCall(g, "POST", "/v1/responses", `{"input":"two","conversation":"named","stream":true}`, "alice")
	if next.Code != 200 || next.Header().Get("X-Hermes-Session-Id") != sid || strings.Contains(next.Body.String(), "UPSTREAM_STREAM_INTERRUPTED") || strings.Contains(next.Body.String(), "resp-") {
		t.Fatal(next.Body.String())
	}
	next = hermesCall(g, "POST", "/v1/responses", `{"input":"two","previous_response_id":"`+previous+`"}`, "alice")
	if next.Code != 200 || !strings.Contains(next.Body.String(), previous) {
		t.Fatal(next.Code, next.Body.String())
	}
	if w := hermesCall(g, "POST", "/v1/responses", `{"input":"two","previous_response_id":"`+previous+`"}`, "bob"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	native := hermesCreated(t, g, "alice", "native")
	completion := hermesCall(g, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"stream":true}`, "alice", "X-Hermes-Session-Id", native)
	if completion.Code != 200 || !strings.Contains(completion.Body.String(), "[DONE]") || strings.Contains(completion.Body.String(), "chatcmpl-") {
		t.Fatal(completion.Body.String())
	}
	page := hermesCall(g, "GET", "/api/sessions?limit=1", "", "alice")
	v, _ := hermesJSON(page.Body.Bytes())
	token := stringField(v, "next_page_token")
	if page.Code != 200 || token == "" {
		t.Fatal(page.Body.String())
	}
	if w := hermesCall(g, "GET", "/api/sessions?limit=1&page_token="+token, "", "bob"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	last := hermesCall(g, "GET", "/api/sessions?limit=1&page_token="+token, "", "alice")
	if last.Code != 200 || last.Body.String() == page.Body.String() {
		t.Fatal(last.Body.String())
	}
	if w := hermesCall(g, "GET", "/v1/capabilities", "", "alice"); strings.Contains(w.Body.String(), "jobs_admin") || strings.Contains(w.Body.String(), `"durable":true`) {
		t.Fatal(w.Body.String())
	}
}
func TestHermesUnsentReservationsAndConcurrentCreation(t *testing.T) {
	f := newHermesFake()
	s := newTestServer(f)
	defer s.Close()
	g := setup(t)
	rt := createHermesRoute(t, g, s.URL)
	g.inflight.Store(int64(g.opts.MaxInflight))
	if w := hermesCall(g, "POST", "/api/sessions", `{"id":"retry"}`, "alice"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if n, _, _ := g.resources.sizes(); n != 0 {
		t.Fatal("unsent reservation leaked", n)
	}
	g.inflight.Store(0)
	var wg sync.WaitGroup
	results := make(chan int, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- hermesCall(g, "POST", "/api/sessions", `{"id":"retry"}`, "alice").Code
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for code := range results {
		if code == 201 {
			success++
		} else if code != 409 {
			t.Fatal(code)
		}
	}
	if success != 1 {
		t.Fatal("concurrent create", success)
	}
	refs := g.resources.list("alice", rt.ID, hermesSession, "")
	if len(refs) != 1 {
		t.Fatal(refs)
	}
	fork := hermesCall(g, "POST", "/api/sessions/"+refs[0].ID+"/fork", `{"id":"child"}`, "alice")
	if fork.Code != 201 || !strings.Contains(fork.Body.String(), refs[0].ID) {
		t.Fatal(fork.Code, fork.Body.String())
	}
	if g.inflight.Load() != 0 {
		t.Fatal("concurrency leak")
	}
}

func TestHermesSubscriptionGetsInitialTask(t *testing.T) {
	upstream := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "agent-card.json") {
			writeJSON(w, 200, map[string]any{"supportedInterfaces": []any{map[string]any{"protocolVersion": "1.0", "protocolBinding": "JSONRPC"}}, "capabilities": map[string]any{"streaming": true}})
			return
		}
		var q rpc
		json.NewDecoder(r.Body).Decode(&q)
		task := map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
		if q.Method == "SubscribeToTask" {
			w.Header().Set("Content-Type", "text/event-stream")
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": map[string]any{"statusUpdate": map[string]any{"taskId": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}}}})
			fmt.Fprintf(w, "data: %s\n\n", raw)
			return
		}
		writeTestTask(w, q, task)
	}))
	defer upstream.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]EndpointInterface{"proxy": {URL: upstream.URL}, "a2a": {URL: upstream.URL, ProtocolVersion: "1.0"}})
	if w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"agent_type":"hermes"}`, tag(rt.Version), ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	task := map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.mapObject(task, "task", "alice", rt.ID, rt.Endpoints[0].ID, "as_subscribe"); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"SubscribeToTask","params":{"id":%q}}`, task["id"])
	streamed := dataRequest(g, "POST", "/agents/test/a2a", q, "alice", "")
	if streamed.Code != 200 || strings.Contains(streamed.Body.String(), `"error"`) || strings.Contains(streamed.Body.String(), "up-task") || strings.Count(streamed.Body.String(), "data:") != 2 {
		t.Fatal(streamed.Body.String())
	}
}
func TestHermesInterruptedStreamRetainsRun(t *testing.T) {
	f := newHermesFake()
	upstream := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/chat/stream") {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "event: run.started\ndata: {\"run_id\":\"interrupted-upstream\"}\n\ndata: {\"run_id\":\"unfinished")
			return
		}
		f.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	g := setup(t)
	rt := createHermesRoute(t, g, upstream.URL)
	sid := hermesCreated(t, g, "alice", "interruption")
	result := hermesCall(g, "POST", "/api/sessions/"+sid+"/chat/stream", `{"input":"hi"}`, "alice")
	if !strings.Contains(result.Body.String(), "UPSTREAM_STREAM_INTERRUPTED") || strings.Contains(result.Body.String(), "unfinished") {
		t.Fatal(result.Body.String())
	}
	refs := g.resources.list("alice", rt.ID, hermesRun, "")
	if len(refs) != 1 || refs[0].State == resourceTerminal || refs[0].ActiveRefs != 0 {
		t.Fatal(refs)
	}
	if g.inflight.Load() != 0 {
		t.Fatal("leaked inflight")
	}
}

func TestHermesImmediateKeepsUpstreamAfterResponse(t *testing.T) {
	finish := make(chan struct{})
	upstream := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "agent-card.json") {
			writeJSON(w, 200, map[string]any{"supportedInterfaces": []any{map[string]any{"protocolVersion": "1.0", "protocolBinding": "JSONRPC"}}, "capabilities": map[string]any{"streaming": true}})
			return
		}
		var q rpc
		json.NewDecoder(r.Body).Decode(&q)
		if q.Method != "SendStreamingMessage" {
			t.Error("immediate send did not use Hermes stream")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": map[string]any{"task": map[string]any{"id": "async-up", "contextId": "async-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}}})
		fmt.Fprintf(w, "data: %s\n\n", raw)
		w.(http.Flusher).Flush()
		select {
		case <-finish:
		case <-r.Context().Done():
			return
		}
		raw, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": map[string]any{"statusUpdate": map[string]any{"taskId": "async-up", "contextId": "async-context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}}}})
		fmt.Fprintf(w, "data: %s\n\n", raw)
	}))
	defer upstream.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]EndpointInterface{"proxy": {URL: upstream.URL}, "a2a": {URL: upstream.URL, ProtocolVersion: "1.0"}})
	admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"agent_type":"hermes"}`, tag(rt.Version), "")
	request := strings.Replace(sendV1, `"message":`, `"configuration":{"returnImmediately":true},"message":`, 1)
	response := dataRequest(g, "POST", "/agents/test/a2a", request, "alice", "")
	value, _ := hermesJSON(response.Body.Bytes())
	if value["error"] != nil {
		close(finish)
		t.Fatal(response.Body.String())
	}
	task := value["result"].(map[string]any)["task"].(map[string]any)
	if task["status"].(map[string]any)["state"] != "TASK_STATE_WORKING" {
		close(finish)
		t.Fatal(task)
	}
	if g.inflight.Load() != 1 {
		close(finish)
		t.Fatal("background HTTP lease released early")
	}
	close(finish)
	for i := 0; i < 100 && g.inflight.Load() != 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	ref, _ := g.resources.lookup(resourceKey{"alice", rt.ID, a2aTask, stringField(task, "id")})
	if g.inflight.Load() != 0 || ref.State != resourceTerminal {
		t.Fatal("background stream did not complete/release", ref)
	}
}

func TestHermesObservationCapacityIsAtomic(t *testing.T) {
	f := newHermesFake()
	server := newTestServer(f)
	defer server.Close()
	g := setup(t)
	rt := createHermesRoute(t, g, server.URL)
	sid := hermesCreated(t, g, "alice", "atomic")
	session, _ := g.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, sid})
	current := g.current.Load().Routes["test"]
	reservation, err := g.reserveResource("alice", current, resourceCreation{Kind: hermesRun, ParentKind: hermesSession, ParentID: sid, Operation: "test", Fingerprint: "test"})
	if err != nil {
		t.Fatal(err)
	}
	d := g.resources
	d.mu.Lock()
	d.maxAliases = d.aliasCount
	d.mu.Unlock()
	if err = d.observeHermes(session, reservation.Resource, []string{"rotated-session"}, "rotated-session", "run-upstream", resourceLive); err != errResourceCapacity {
		t.Fatal(err)
	}
	unchanged, _ := d.lookup(session.key())
	execution, _ := d.lookup(reservation.Resource.key())
	if unchanged.Upstream != session.Upstream || execution.Upstream != "" || d.publicUpstream("alice", rt.ID, hermesSession, session.EndpointID, "rotated-session") != "" {
		t.Fatal("partial ID mapping after capacity rejection")
	}
	d.mu.Lock()
	d.maxAliases++
	d.mu.Unlock()
	if err = d.observeHermes(session, reservation.Resource, nil, "rotated-session", "run-upstream", resourceLive); err != nil {
		t.Fatal(err)
	}
}

func TestHermesIncompatibleEndpointIsNotScheduled(t *testing.T) {
	bad := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/capabilities" {
			t.Error("sent a user request to incompatible endpoint")
		}
		writeJSON(w, 200, map[string]any{"features": map[string]any{"session_resources": true}})
	}))
	defer bad.Close()
	good := newTestServer(newHermesFake())
	defer good.Close()
	g := setup(t)
	rt := createHermesRoute(t, g, bad.URL, good.URL)
	sid := hermesCreated(t, g, "alice", "capabilities")
	ref, _ := g.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, sid})
	if ref.EndpointID != rt.Endpoints[1].ID {
		t.Fatal("incompatible endpoint selected")
	}
	detail := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, "", "", "")
	if !strings.Contains(detail.Body.String(), `"ready":false`) {
		t.Fatal(detail.Body.String())
	}
}
