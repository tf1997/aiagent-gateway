package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
)

func TestA2AV1TaskOperationsStayOnOrigin(t *testing.T) {
	var calls atomic.Int64
	var wrongTask atomic.Bool
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		if q.Method == "SendMessage" {
			writeTestTask(w, q, map[string]any{"id": requestedTask(q), "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
			return
		}
		calls.Add(1)
		if q.Params["id"] != "original" {
			t.Error("task ID not translated", q.Params)
		}
		id := "original"
		if wrongTask.Load() {
			id = "other"
		}
		if q.Method == "SubscribeToTask" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, sse(strings.ReplaceAll(firstTask, `"up"`, `"original"`), statusEvent("original", "TASK_STATE_AUTH_REQUIRED")))
			return
		}
		state := "TASK_STATE_WORKING"
		if q.Method == "CancelTask" {
			state = "TASK_STATE_CANCELED"
		}
		writeTestTask(w, q, map[string]any{"id": id, "contextId": "ctx", "status": map[string]any{"state": state}})
	}))
	defer up.Close()
	g := setup(t)
	rt := create(t, g, "a2a", up.URL)
	task := sendA2ATask(t, g, "alice", "original")
	addEndpoint(t, g, "http://localhost:1")
	id := protocol.StringField(task, "id")
	path := "/admin/v1/routes/" + rt.ID + "/endpoints/" + rt.Endpoints[0].ID
	if w := admin(t, g, "PATCH", path, `{"state":"draining"}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for _, method := range []string{"GetTask", "SubscribeToTask", "CancelTask"} {
		w := dataRequest(g, "POST", "/agents/test/a2a", rpcBody(method, map[string]any{"id": id}), "alice", "")
		if strings.Contains(w.Body.String(), `"error"`) || !strings.Contains(w.Body.String(), id) {
			t.Fatal(method, w.Body.String())
		}
	}
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("SubscribeToTask", map[string]any{"id": id}), "alice", ""), -32004)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("CancelTask", map[string]any{"id": id}), "alice", ""), -32002)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("GetTask", map[string]any{"id": id}), "bob", ""), -32001)
	wrongTask.Store(true)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("GetTask", map[string]any{"id": id}), "alice", ""), -32006)
	if calls.Load() != 4 {
		t.Fatal("unexpected upstream requests", calls.Load())
	}
	if len(readResources(t, g, "alice", rt.ID, protocol.A2ATask, "")) != 1 {
		t.Fatal("changed identity registered")
	}
	if metric(t, g, "gateway_inflight") != 0 {
		t.Fatal("lease leaked")
	}
}

func TestA2AV1ListTasksIsolationPaginationAndFilters(t *testing.T) {
	var calls atomic.Int64
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		if q.Method == "SendMessage" {
			writeTestTask(w, q, map[string]any{"id": requestedTask(q), "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
			return
		}
		if q.Method != "GetTask" {
			t.Error("global upstream task list requested", q.Method)
		}
		id := protocol.StringField(q.Params, "id")
		if id == "bob-task" {
			t.Error("queried another caller task")
		}
		if id == "gone" {
			writeRPCError(w, q.ID, -32001, "Task not found")
			return
		}
		stamp := "2026-09-30T01:00:00Z"
		if id == "two" {
			stamp = "2026-09-30T02:00:00Z"
		}
		task := map[string]any{"id": id, "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING", "timestamp": stamp}, "history": []any{map[string]any{"messageId": "m", "role": "ROLE_USER", "contextId": "ctx", "parts": []any{map[string]any{"text": "hi"}}}}, "artifacts": []any{map[string]any{"artifactId": "a", "parts": []any{map[string]any{"text": "file"}}}}}
		writeTestTask(w, q, task)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	one := sendA2ATask(t, g, "alice", "one")
	two := sendA2ATask(t, g, "alice", "two")
	sendA2ATask(t, g, "alice", "gone")
	sendA2ATask(t, g, "bob", "bob-task")
	fetch := func(params map[string]any) map[string]any {
		t.Helper()
		w := dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", params), "alice", "")
		var env map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &env)
		result, ok := env["result"].(map[string]any)
		if !ok {
			t.Fatal(w.Body.String())
		}
		return result
	}
	page := fetch(map[string]any{"pageSize": 1})
	tasks := page["tasks"].([]any)
	first := tasks[0].(map[string]any)
	if len(tasks) != 1 || first["id"] != two["id"] || first["artifacts"] != nil || len(first["history"].([]any)) != 0 || page["totalSize"] != float64(2) {
		t.Fatal(page)
	}
	token := page["nextPageToken"].(string)
	if token == "" {
		t.Fatal(page)
	}
	count := calls.Load()
	for i := 0; i < 2; i++ {
		last := fetch(map[string]any{"pageSize": 1, "pageToken": token})
		if last["nextPageToken"] != "" || last["tasks"].([]any)[0].(map[string]any)["id"] != one["id"] {
			t.Fatal(last)
		}
	}
	if calls.Load() != count {
		t.Fatal("snapshot page repeated upstream calls")
	}
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 1, "pageToken": token}), "bob", ""), -32602)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 2, "pageToken": token}), "alice", ""), -32602)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 1, "pageToken": token, "includeArtifacts": true}), "alice", ""), -32602)
	filtered := fetch(map[string]any{"contextId": one["contextId"], "status": "TASK_STATE_WORKING", "statusTimestampAfter": "2026-09-30T02:00:00Z", "includeArtifacts": true, "historyLength": 1})
	entries := filtered["tasks"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["id"] != two["id"] || entries[0].(map[string]any)["artifacts"] == nil || len(entries[0].(map[string]any)["history"].([]any)) != 1 {
		t.Fatal(filtered)
	}
	if empty := fetch(map[string]any{"contextId": "not-owned"}); len(empty["tasks"].([]any)) != 0 || empty["nextPageToken"] != "" {
		t.Fatal(empty)
	}
	if metric(t, g, "gateway_inflight") != 0 {
		t.Fatal("list query lease leaked")
	}
}

func TestA2AV1ListDoesNotFallbackAfterUpstreamFailure(t *testing.T) {
	var other atomic.Int64
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		if q.Method == "SendMessage" {
			writeTestTask(w, q, map[string]any{"id": requestedTask(q), "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer up.Close()
	fallback := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { other.Add(1) }))
	defer fallback.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	sendA2ATask(t, g, "alice", "one")
	addEndpoint(t, g, fallback.URL)
	requireRPCCode(t, dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{}), "alice", ""), -32603)
	if other.Load() != 0 || metric(t, g, "gateway_inflight") != 0 {
		t.Fatal("task query moved or leaked lease")
	}
}
