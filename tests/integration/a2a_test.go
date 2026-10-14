package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/protocol"
)

func TestA2AV1RejectsOldWireAndInvalidParams(t *testing.T) {
	var calls atomic.Int64
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	for _, version := range []string{"", "0.3", "0.3.0", "1.0.1", "2.0"} {
		req := httptest.NewRequest("POST", "/agents/test/a2a", strings.NewReader(sendV1))
		req.Header.Set("Authorization", "Bearer alice-secret")
		req.Header.Set("A2A-Version", version)
		w := httptest.NewRecorder()
		g.DataHandler().ServeHTTP(w, req)
		requireRPCCode(t, w, -32009)
	}
	cases := []struct {
		body string
		code int
	}{
		{`{`, -32700}, {`[]`, -32600},
		{strings.Replace(sendV1, `"id":1`, `"id":null`, 1), -32600},
		{strings.Replace(sendV1, "SendMessage", "message/send", 1), -32601},
		{strings.Replace(sendV1, "ROLE_USER", "user", 1), -32602},
		{strings.Replace(sendV1, `"messageId"`, `"kind":"message","messageId"`, 1), -32602},
		{strings.Replace(sendV1, `"text":"hello"`, `"text":"hello","data":{}`, 1), -32602},
		{strings.Replace(sendV1, `"params":{`, `"params":{"tenant":"attacker",`, 1), -32602},
		{strings.Replace(sendV1, `"params":{`, `"params":{"configuration":{"blocking":false},`, 1), -32602},
		{strings.Replace(sendV1, `"params":{`, `"params":{"configuration":{"taskPushNotificationConfig":{}},`, 1), -32003},
		{rpcBody("ListTasks", map[string]any{"pageSize": 101}), -32602},
		{rpcBody("ListTasks", map[string]any{"historyLength": -1}), -32602},
		{rpcBody("ListTasks", map[string]any{"status": "working"}), -32602},
		{rpcBody("ListTasks", map[string]any{"statusTimestampAfter": "yesterday"}), -32602},
		{rpcBody("GetExtendedAgentCard", map[string]any{}), -32007},
		{rpcBody("CreateTaskPushNotificationConfig", map[string]any{}), -32003},
	}
	for _, tc := range cases {
		requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", tc.body, "alice", ""), tc.code)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid requests reached upstream", calls.Load())
	}
}

func TestA2AV1PinnedFixturesAndTenant(t *testing.T) {
	input, err := os.ReadFile("testdata/a2a-v1.0.1/send-message.json")
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile("testdata/a2a-v1.0.1/task-response.json")
	if err != nil {
		t.Fatal(err)
	}
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("A2A-Version") != "1.0" {
			t.Error("upstream version header missing")
		}
		raw, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(raw), `"tenant":"deployment-tenant"`) || !strings.Contains(string(raw), `9007199254740993`) || !strings.Contains(string(raw), `"taskId":"business-task"`) {
			t.Error("request/tenant corrupted", string(raw))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(output)
	}))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL, ProtocolVersion: "1.0", Tenant: "deployment-tenant"}})
	w := dataRequest(g, "POST", "/agents/test/a2a", string(input), "alice", "")
	if strings.Contains(w.Body.String(), `"error"`) || !strings.Contains(w.Body.String(), `9007199254740993`) || !strings.Contains(w.Body.String(), `"taskId":"business-task"`) || strings.Contains(w.Body.String(), `"id":"up-task"`) {
		t.Fatal(w.Body.String())
	}
	card := dataRequest(g, "GET", "/agents/test/.well-known/agent-card.json", "", "alice", "")
	var c map[string]any
	_ = json.Unmarshal(card.Body.Bytes(), &c)
	if c["url"] != nil || c["protocolVersion"] != nil || c["security"] != nil || c["supportsAuthenticatedExtendedCard"] != nil || strings.Contains(card.Body.String(), "deployment-tenant") {
		t.Fatal(card.Body.String())
	}
	iface := c["supportedInterfaces"].([]any)[0].(map[string]any)
	if iface["protocolVersion"] != "1.0" || iface["protocolBinding"] != "JSONRPC" {
		t.Fatal(iface)
	}
	scheme := c["securitySchemes"].(map[string]any)["gatewayBearer"].(map[string]any)["httpAuthSecurityScheme"].(map[string]any)
	if scheme["scheme"] != "Bearer" || c["securityRequirements"] == nil {
		t.Fatal(card.Body.String())
	}
	path := "/admin/v1/routes/" + rt.ID + "/endpoints/" + rt.Endpoints[0].ID
	if w := admin(t, g, "PATCH", path, `{"interfaces":{"a2a":{"tenant":"other"}}}`, `"1"`, ""); w.Code != 409 {
		t.Fatal("changed bound tenant", w.Body.String())
	}
}

func TestA2AV1UpstreamValidationIsAtomic(t *testing.T) {
	for _, response := range []string{
		`{"jsonrpc":"2.0","id":99,"result":{"task":{"id":"up","status":{"state":"TASK_STATE_WORKING"}}}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"up","status":{"state":"TASK_STATE_WORKING"}},"message":{}}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"up","status":{"state":"working"}}}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"kind":"task","id":"up"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"err"},"result":{}}`,
	} {
		t.Run(response, func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) }))
			defer up.Close()
			g := setup(t)
			create(t, g, "a2a", up.URL)
			requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", ""), -32006)
			if len(readResources(t, g, "", "", "", "")) != 0 {
				t.Fatal("bad frame created resources")
			}
		})
	}
}

func TestA2AV1MessageAndErrors(t *testing.T) {
	var failure atomic.Bool
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failure.Load() {
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"task is already completed","data":{"state":"completed","token":"provider-secret"}}}`)
			return
		}
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"message":{"messageId":"reply","contextId":"up-context","role":"ROLE_AGENT","parts":[{"text":"hello"}]}}}`)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	w := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "")
	if !strings.Contains(w.Body.String(), `"contextId":"ctx_`) || strings.Contains(w.Body.String(), `"error"`) {
		t.Fatal(w.Body.String())
	}
	failure.Store(true)
	w = dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "")
	requireRPCCode(t, w, -32002)
	if !strings.Contains(w.Body.String(), "task is already completed") || !strings.Contains(w.Body.String(), `"state":"completed"`) || strings.Contains(w.Body.String(), "provider-secret") {
		t.Fatal("upstream details lost or credentials exposed", w.Body.String())
	}
}

const firstTask = `{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"up","contextId":"ctx","status":{"state":"TASK_STATE_WORKING"}}}}`

func statusEvent(task, state string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":%q,"contextId":"ctx","status":{"state":%q}}}}`, task, state)
}

func sse(events ...string) string { return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n" }

func TestA2AV1StreamFramingAndLifecycle(t *testing.T) {
	artifact := `{"jsonrpc":"2.0","id":1,"result":{"artifactUpdate":{"taskId":"up","contextId":"ctx","artifact":{"artifactId":"a","parts":[{"text":"result"}]},"lastChunk":true}}}`
	cases := []struct {
		name, wire string
		code       int
		state      protocol.ResourceState
		tasks      int
	}{
		{"complete", sse(firstTask, artifact, statusEvent("up", "TASK_STATE_COMPLETED")), 0, protocol.ResourceTerminal, 1},
		{"waiting", sse(firstTask, statusEvent("up", "TASK_STATE_INPUT_REQUIRED")), 0, protocol.ResourceWaiting, 1},
		{"early eof", sse(firstTask), -32006, protocol.ResourceLive, 1},
		{"unknown state", sse(firstTask, statusEvent("up", "TASK_STATE_FUTURE")), -32006, protocol.ResourceUnknown, 1},
		{"wrong first", sse(statusEvent("up", "TASK_STATE_COMPLETED")), -32006, "", 0},
		{"wrong identity", sse(firstTask, statusEvent("other", "TASK_STATE_COMPLETED")), -32006, protocol.ResourceLive, 1},
		{"partial event", sse(firstTask) + "data: " + statusEvent("up", "TASK_STATE_COMPLETED"), -32006, protocol.ResourceLive, 1},
		{"CR framing", strings.ReplaceAll(sse(firstTask, statusEvent("up", "TASK_STATE_COMPLETED")), "\n", "\r"), 0, protocol.ResourceTerminal, 1},
		{"multiline", ": hello\r\ndata: {\r\ndata: \"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"task\":{\"id\":\"up\",\"contextId\":\"ctx\",\"status\":{\"state\":\"TASK_STATE_COMPLETED\"}}}}\r\n\r\n", 0, protocol.ResourceTerminal, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, tc.wire)
			}))
			defer up.Close()
			g := setup(t)
			rt := create(t, g, "a2a", up.URL)
			w := dataRequest(g, "POST", "/agents/test/a2a", strings.Replace(sendV1, "SendMessage", "SendStreamingMessage", 1), "alice", "")
			if strings.Contains(w.Body.String(), `"error"`) != (tc.code != 0) {
				t.Fatal(w.Body.String())
			}
			tasks := readResources(t, g, "alice", rt.ID, protocol.A2ATask, "")
			if len(tasks) != tc.tasks || len(tasks) > 0 && tasks[0].State != tc.state {
				t.Fatal(tasks, w.Body.String())
			}
			if strings.Contains(w.Body.String(), `"taskId":"other"`) {
				t.Fatal("wrong stream identity leaked")
			}
		})
	}
}

func TestA2AV1StreamCancellation(t *testing.T) {
	canceled := make(chan struct{})
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sse(firstTask))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	server := newTestServer(g.DataHandler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/agents/test/a2a", strings.NewReader(strings.Replace(sendV1, "SendMessage", "SendStreamingMessage", 1)))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("A2A-Version", "1.0")
	res, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(res.Body).ReadString('\n')
	if err != nil || !strings.Contains(line, `"id":"task_`) {
		t.Fatal(line, err)
	}
	res.Body.Close()
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream was not canceled")
	}
}

func TestA2AV1BlockingSend(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		writeTestTask(w, q, map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", ""), -32006)
	async := strings.Replace(sendV1, `"params":{`, `"params":{"configuration":{"returnImmediately":true},`, 1)
	w := dataRequest(g, "POST", "/agents/test/a2a", async, "alice", "")
	if strings.Contains(w.Body.String(), `"error"`) {
		t.Fatal(w.Body.String())
	}
}

func TestA2AV1TaskHistoryMayContainInitialClientMessage(t *testing.T) {
	task := map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}, "history": []any{map[string]any{"messageId": "first", "role": "ROLE_USER", "parts": []any{map[string]any{"text": "start"}}}}}
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		writeTestTask(w, q, task)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	mapped := rpcTask(t, dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", ""), true)
	if len(mapped["history"].([]any)) != 1 {
		t.Fatal("initial client history was lost", mapped)
	}
}

func TestA2ATaskMappingIsolation(t *testing.T) {
	var queried string
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		json.NewDecoder(r.Body).Decode(&q)
		if q.Method == "GetTask" {
			queried, _ = q.Params["id"].(string)
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}, "artifacts": []any{map[string]any{"artifactId": "a", "parts": []any{map[string]any{"data": map[string]any{"taskId": "user-data"}}}}}})
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	v := rpcCall(t, g, "alice", `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`, "s")
	result, ok := v["result"].(map[string]any)
	if !ok {
		t.Fatal(v)
	}
	result = result["task"].(map[string]any)
	id := result["id"].(string)
	ctx := result["contextId"].(string)
	if id == "up-task" || ctx == "up-context" {
		t.Fatal("raw IDs exposed")
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, id)
	v = rpcCall(t, g, "alice", body, "")
	if v["error"] != nil || queried != "up-task" {
		t.Fatal(v, queried)
	}
	v = rpcCall(t, g, "bob", body, "")
	if v["error"] == nil {
		t.Fatal("cross caller task access")
	}
	raw, _ := json.Marshal(result)
	if !bytes.Contains(raw, []byte(`"taskId":"user-data"`)) {
		t.Fatal("arbitrary data rewritten")
	}
	v = rpcCall(t, g, "alice", body, "conflicting")
	if v["error"] == nil {
		t.Fatal("session conflict accepted")
	}
}

func TestA2AStream(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"task\":{\"id\":\"up\",\"contextId\":\"ctx\",\"status\":{\"state\":\"TASK_STATE_WORKING\"}}}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"statusUpdate\":{\"taskId\":\"up\",\"contextId\":\"ctx\",\"status\":{\"state\":\"TASK_STATE_COMPLETED\"}}}}\n\n")
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	r := httptest.NewRequest("POST", "/a2a/test", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`))
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Authorization", "Bearer alice-secret")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `"taskId":"task_`) || !strings.Contains(w.Body.String(), `"contextId":"ctx_`) {
		t.Fatal(w.Body.String())
	}
}

func TestA2AResourceFailureNeverLeaksPartialIDs(t *testing.T) {
	var invalid atomic.Bool
	invalid.Store(true)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}}
		if invalid.Load() {
			result["history"] = []any{map[string]any{"messageId": "m", "role": "ROLE_AGENT", "contextId": "ctx", "parts": []any{map[string]any{"text": "ok"}}, "referenceTaskIds": []any{"missing"}}}
		}
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		writeTestTask(w, q, result)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	first := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "s")
	if !strings.Contains(first.Body.String(), `"error"`) || strings.Contains(first.Body.String(), `"task_`) || len(readResources(t, g, "", "", "", "")) != 0 {
		t.Fatal("partial response/mapping escaped", first.Body.String())
	}
	invalid.Store(false)
	second := dataRequest(g, "POST", "/a2a/test", sendV1, "alice", "s")
	var result struct {
		Result struct {
			Task struct {
				ID string `json:"id"`
			} `json:"task"`
		} `json:"result"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &result); err != nil || result.Result.Task.ID == "" {
		t.Fatal(second.Body.String(), err)
	}
	query := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, result.Result.Task.ID)
	rt := readRoute(t, g, "test")
	view := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/tasks?owner_id=alice", "", "", "")
	if view.Code != 200 || !strings.Contains(view.Body.String(), `"gateway_task_id":"`+result.Result.Task.ID+`"`) || !strings.Contains(view.Body.String(), `"resource_kind":"a2a_task"`) {
		t.Fatal("legacy task view changed", view.Body.String())
	}
	denied := dataRequest(g, "POST", "/agents/test/a2a", query, "bob", "")
	if !strings.Contains(denied.Body.String(), `"code":-32001`) {
		t.Fatal("cross caller task access", denied.Body.String())
	}
}
