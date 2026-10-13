package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/protocol"
)

var hermesFeatures = []string{"session_resources", "session_model_lock", "session_chat", "session_chat_streaming", "session_fork", "chat_completions", "chat_completions_streaming", "responses_api", "responses_streaming", "run_submission", "run_status", "run_events_sse", "run_stop", "run_steer", "run_approval_response"}

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
		protocol.WriteJSON(w, 200, map[string]any{"features": features})
		return
	}
	var body map[string]any
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&body)
	}

	if path == "/v1/chat/completions" {
		sid := r.Header.Get("X-Hermes-Session-Id")
		if f.sessions[sid] == nil {
			protocol.WriteJSON(w, 404, map[string]any{})
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
		protocol.WriteJSON(w, 200, value)
		return
	}
	if path == "/v1/responses" {
		previous := protocol.StringField(body, "previous_response_id")
		conversation := protocol.StringField(body, "conversation")
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
		protocol.WriteJSON(w, 200, value)
		return
	}
	if path == "/api/sessions" && r.Method == "POST" || strings.HasSuffix(path, "/fork") {
		id := protocol.StringField(body, "id")
		if !strings.HasPrefix(id, "hgs_") {
			protocol.WriteJSON(w, 400, map[string]string{"error": "unisolated"})
			return
		}
		if f.sessions[id] != nil {
			protocol.WriteJSON(w, 409, map[string]string{"error": "exists"})
			return
		}
		s := map[string]any{"id": id, "source": "api_server", "title": body["title"], "metadata": map[string]any{"session_id": "business-id"}}
		if strings.HasSuffix(path, "/fork") {
			s["parent_session_id"] = strings.Split(path, "/")[3]
		}
		f.sessions[id] = s
		protocol.WriteJSON(w, 201, map[string]any{"session": s})
		return
	}
	if strings.HasPrefix(path, "/api/sessions/") {
		parts := strings.Split(path, "/")
		id := parts[3]
		s := f.sessions[id]
		if s == nil {
			protocol.WriteJSON(w, 404, map[string]any{})
			return
		}
		if len(parts) == 4 {
			if r.Method == "DELETE" {
				delete(f.sessions, id)
				protocol.WriteJSON(w, 200, map[string]any{"id": id, "deleted": true})
				return
			}
			if r.Method == "PATCH" {
				s["title"] = body["title"]
			}
			protocol.WriteJSON(w, 200, map[string]any{"session": s})
			return
		}
		if parts[4] == "messages" {
			child := id + "-compressed"
			f.sessions[child] = map[string]any{"id": child, "source": "api_server"}
			protocol.WriteJSON(w, 200, map[string]any{"session_id": child, "data": []any{map[string]any{"session_id": id, "role": "user", "content": "business-id"}}})
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
			protocol.WriteJSON(w, 200, map[string]any{"session_id": id, "message": map[string]any{"content": "business-id"}})
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
			protocol.WriteJSON(w, 502, map[string]any{"error": "lost"})
			return
		}
		protocol.WriteJSON(w, 202, map[string]any{"run_id": rid, "status": f.runs[rid]["status"]})
		return
	}
	if strings.HasPrefix(path, "/v1/runs/") {
		parts := strings.Split(path, "/")
		run := f.runs[parts[3]]
		if run == nil {
			protocol.WriteJSON(w, 404, map[string]any{})
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
		protocol.WriteJSON(w, 200, run)
		return
	}
	protocol.WriteJSON(w, 501, map[string]any{"error": "unsupported"})
}

func createHermesRoute(t *testing.T, g *gateway.Gateway, urls ...string) gateway.Route {
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
	var rt gateway.Route
	json.Unmarshal(w.Body.Bytes(), &rt)
	return rt
}

func hermesCall(g *gateway.Gateway, method, path, body, caller string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/agents/test"+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+caller+"-secret")
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, req)
	return w
}

func hermesCreated(t *testing.T, g *gateway.Gateway, caller, alias string) string {
	t.Helper()
	w := hermesCall(g, "POST", "/api/sessions", `{"id":"`+alias+`"}`, caller)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	v, _ := protocol.JSONObject(w.Body.Bytes())
	return protocol.StringField(v["session"].(map[string]any), "id")
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
	rt := readRoute(t, g, "test")
	ref := readResource(t, g, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesSession, ID: alice})
	if !strings.HasSuffix(ref.Upstream, "-compressed") {
		t.Fatal("compression lineage missing")
	}
	ep := rt.Endpoint(ref.EndpointID)
	patch := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID+"/endpoints/"+ep.ID, `{"state":"draining"}`, etag(ep.Version), "")
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
	if metric(t, g, "gateway_inflight") != 0 {
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
	value, _ := protocol.JSONObject(replay.Body.Bytes())
	runID := protocol.StringField(value, "run_id")
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
	rt := readRoute(t, g, "test")
	ref := readResource(t, g, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesRun, ID: runID})
	if ref.State == protocol.ResourceTerminal {
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
	waitFor(t, 15*time.Second, "background run reconciliation", func() bool {
		return readResource(t, g, ref.Key()).State == protocol.ResourceTerminal
	})
	ref = readResource(t, g, ref.Key())
	if ref.State != protocol.ResourceTerminal {
		t.Fatal("background completion not observed")
	}
	if metric(t, g, "gateway_inflight") != 0 {
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
	value, _ := protocol.JSONObject(first.Body.Bytes())
	previous := protocol.StringField(value, "id")
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
	v, _ := protocol.JSONObject(page.Body.Bytes())
	token := protocol.StringField(v, "next_page_token")
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
	started, finish := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finish) })
	s := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(started)
			select {
			case <-finish:
				fmt.Fprint(w, "released")
			case <-r.Context().Done():
			}
			return
		}
		f.ServeHTTP(w, r)
	}))
	defer s.Close()
	defer unblock()
	g := setup(t, func(opts *gateway.Options) { opts.MaxInflight = 1 })
	rt := createHermesRoute(t, g, s.URL)
	body := fmt.Sprintf(`{"key":"blocker","consumers":[{"id":"alice","token":"alice-secret"}],"expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":%q}}}]}`, s.URL)
	if w := admin(t, g, "POST", "/admin/v1/routes", body, "", ""); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- dataRequest(g, "GET", "/agents/blocker/hold", "", "alice", "held") }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking request did not reach upstream")
	}
	if w := hermesCall(g, "POST", "/api/sessions", `{"id":"retry"}`, "alice"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if n := metric(t, g, "gateway_resources"); n != 0 {
		t.Fatal("unsent reservation leaked", n)
	}
	unblock()
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatal("blocking request failed", w.Code, w.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking request did not release capacity")
	}
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
	refs := readResources(t, g, "alice", rt.ID, protocol.HermesSession, "")
	if len(refs) != 1 {
		t.Fatal(refs)
	}
	fork := hermesCall(g, "POST", "/api/sessions/"+refs[0].ID+"/fork", `{"id":"child"}`, "alice")
	if fork.Code != 201 || !strings.Contains(fork.Body.String(), refs[0].ID) {
		t.Fatal(fork.Code, fork.Body.String())
	}
	if metric(t, g, "gateway_inflight") != 0 {
		t.Fatal("concurrency leak")
	}
}

func TestHermesSubscriptionGetsInitialTask(t *testing.T) {
	upstream := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "agent-card.json") {
			protocol.WriteJSON(w, 200, map[string]any{"supportedInterfaces": []any{map[string]any{"protocolVersion": "1.0", "protocolBinding": "JSONRPC"}}, "capabilities": map[string]any{"streaming": true}})
			return
		}
		var q rpcMessage
		json.NewDecoder(r.Body).Decode(&q)
		task := map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
		if q.Method == "SendMessage" {
			task["status"] = map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}
		}
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
	rt := createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: upstream.URL}, "a2a": {URL: upstream.URL, ProtocolVersion: "1.0"}})
	if w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"agent_type":"hermes"}`, etag(rt.Version), ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	task := rpcTask(t, dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "as_subscribe"), true)
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
	refs := readResources(t, g, "alice", rt.ID, protocol.HermesRun, "")
	if len(refs) != 1 || refs[0].State == protocol.ResourceTerminal || refs[0].ActiveRefs != 0 {
		t.Fatal(refs)
	}
	if metric(t, g, "gateway_inflight") != 0 {
		t.Fatal("leaked inflight")
	}
}

func TestHermesImmediateKeepsUpstreamAfterResponse(t *testing.T) {
	finish := make(chan struct{})
	upstream := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "agent-card.json") {
			protocol.WriteJSON(w, 200, map[string]any{"supportedInterfaces": []any{map[string]any{"protocolVersion": "1.0", "protocolBinding": "JSONRPC"}}, "capabilities": map[string]any{"streaming": true}})
			return
		}
		var q rpcMessage
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
	rt := createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: upstream.URL}, "a2a": {URL: upstream.URL, ProtocolVersion: "1.0"}})
	admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"agent_type":"hermes"}`, etag(rt.Version), "")
	request := strings.Replace(sendV1, `"message":`, `"configuration":{"returnImmediately":true},"message":`, 1)
	response := dataRequest(g, "POST", "/agents/test/a2a", request, "alice", "")
	value, _ := protocol.JSONObject(response.Body.Bytes())
	if value["error"] != nil {
		close(finish)
		t.Fatal(response.Body.String())
	}
	task := value["result"].(map[string]any)["task"].(map[string]any)
	if task["status"].(map[string]any)["state"] != "TASK_STATE_WORKING" {
		close(finish)
		t.Fatal(task)
	}
	if metric(t, g, "gateway_inflight") != 1 {
		close(finish)
		t.Fatal("background HTTP lease released early")
	}
	close(finish)
	waitFor(t, 5*time.Second, "background stream release", func() bool {
		return metric(t, g, "gateway_inflight") == 0
	})
	ref := readResource(t, g, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.A2ATask, ID: protocol.StringField(task, "id")})
	if metric(t, g, "gateway_inflight") != 0 || ref.State != protocol.ResourceTerminal {
		t.Fatal("background stream did not complete/release", ref)
	}
}

func TestHermesCapabilityDiscoveryDoesNotBlockProxy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"partial_features", 200, `{"features":{"session_resources":true,"session_chat":true}}`},
		{"missing_endpoint", 404, `{}`},
		{"probe_failure", 503, `{}`},
		{"invalid_response", 200, `invalid json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newHermesFake()
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if r.URL.Path == "/v1/capabilities" {
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/model") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				fake.ServeHTTP(w, r)
			}))
			defer up.Close()
			g := setup(t, func(opts *gateway.Options) { opts.HealthInterval = 50 * time.Millisecond })
			rt := createHermesRoute(t, g, up.URL)
			sid := hermesCreated(t, g, "alice", "legacy")
			if w := hermesCall(g, "GET", "/api/sessions/"+sid, "", "alice"); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if w := hermesCall(g, "POST", "/api/sessions/"+sid+"/chat", `{"input":"hello"}`, "alice"); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if w := hermesCall(g, "POST", "/api/sessions/"+sid+"/model", `{}`, "alice"); w.Code != 404 || !strings.Contains(w.Body.String(), "HERMES_UPSTREAM_ERROR") {
				t.Fatal("unsupported operation did not reach upstream", w.Code, w.Body.String())
			}
			capabilities := hermesCall(g, "GET", "/v1/capabilities", "", "alice")
			if capabilities.Code != 200 || strings.Contains(capabilities.Body.String(), `"run_submission":true`) {
				t.Fatal(capabilities.Code, capabilities.Body.String())
			}
			detail := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, "", "", "")
			var value struct {
				Runtime struct {
					Interfaces map[string]struct {
						Healthy      bool                   `json:"healthy"`
						Ready        bool                   `json:"ready"`
						Capabilities *protocol.Capabilities `json:"capabilities"`
					} `json:"interfaces"`
				} `json:"runtime"`
			}
			if err := json.Unmarshal(detail.Body.Bytes(), &value); err != nil {
				t.Fatal(err)
			}
			iface := value.Runtime.Interfaces["proxy"]
			if !iface.Healthy || !iface.Ready || iface.Capabilities == nil || iface.Capabilities.Ready {
				t.Fatal("capability diagnostics changed interface readiness", detail.Body.String())
			}
			endpointPath := "/admin/v1/routes/" + rt.ID + "/endpoints/" + rt.Endpoints[0].ID
			patch := fmt.Sprintf(`{"interfaces":{"proxy":{"health_url":%q}}}`, up.URL+"/health")
			if w := admin(t, g, "PATCH", endpointPath, patch, etag(rt.Endpoints[0].Version), ""); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			waitFor(t, 5*time.Second, "failed endpoint health probe", func() bool {
				detail := admin(t, g, "GET", endpointPath, "", "", "")
				if err := json.Unmarshal(detail.Body.Bytes(), &value); err != nil || detail.Code != 200 {
					t.Fatal("endpoint diagnostics unavailable", detail.Code, err)
				}
				return !value.Runtime.Interfaces["proxy"].Healthy
			})
			if w := hermesCall(g, "POST", "/api/sessions", `{"id":"unhealthy"}`, "alice"); w.Code != 503 {
				t.Fatal("unhealthy endpoint accepted a new session", w.Code, w.Body.String())
			}
			if w := hermesCall(g, "GET", "/api/sessions/"+sid, "", "alice"); w.Code != 503 {
				t.Fatal("unhealthy endpoint accepted an existing session", w.Code, w.Body.String())
			}
		})
	}
}

func TestHermesCapabilityDiscoveryDoesNotBlockA2A(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"no_streaming", 200, `{"supportedInterfaces":[{"protocolVersion":"1.0","protocolBinding":"JSONRPC"}],"capabilities":{"streaming":false}}`},
		{"incomplete_card", 200, `{"capabilities":{}}`},
		{"missing_card", 404, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/.well-known/agent-card.json") {
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
					return
				}
				var q rpcMessage
				if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}})
			}))
			defer up.Close()
			g := setup(t)
			rt := create(t, g, "a2a", up.URL)
			if w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"agent_type":"hermes"}`, etag(rt.Version), ""); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			w := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "")
			var response struct {
				Result struct {
					Task struct {
						ID string `json:"id"`
					} `json:"task"`
				} `json:"result"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Result.Task.ID == "" {
				t.Fatal(w.Code, w.Body.String())
			}
			w = dataRequest(g, "POST", "/agents/test/a2a", rpcBody("GetTask", map[string]any{"id": response.Result.Task.ID}), "alice", "")
			if w.Code != 200 || strings.Contains(w.Body.String(), `"error"`) || !strings.Contains(w.Body.String(), response.Result.Task.ID) {
				t.Fatal("existing task blocked by capability discovery", w.Code, w.Body.String())
			}
		})
	}
}
