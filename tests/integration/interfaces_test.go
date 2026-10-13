package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"aiagent-gateway/internal/gateway"
)

func TestUnifiedEntriesAndAliases(t *testing.T) {
	var proxyCalls, rpcCalls atomic.Int64
	proxy := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer proxy-key" || r.Header.Get("Cookie") != "" {
			t.Error("wrong proxy credential")
		}
		if r.URL.EscapedPath() != "/base/api/a%2Fb" || r.URL.RawQuery != "fixed=1&q=2" {
			t.Error("path/query changed", r.URL.String())
		}
		fmt.Fprint(w, "chat")
	}))
	defer proxy.Close()
	rpcUp := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalls.Add(1)
		if r.URL.String() != "/rpc?fixed=1" || r.Header.Get("Authorization") != "Bearer a2a-key" || r.Header.Get("Cookie") != "" {
			t.Error("wrong RPC interface", r.URL.String())
		}
		var q rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		if q.Method == "GetTask" && q.Params["id"] != "up-task" {
			t.Error("task not translated", q.Params)
		}
		if q.Method == "SendStreamingMessage" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"task\":{\"id\":\"up-task\",\"contextId\":\"up-context\",\"status\":{\"state\":\"TASK_STATE_WORKING\"}}}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"statusUpdate\":{\"taskId\":\"up-task\",\"contextId\":\"up-context\",\"status\":{\"state\":\"TASK_STATE_INPUT_REQUIRED\"}}}}\n\n")
			return
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}})
	}))
	defer rpcUp.Close()
	g := setup(t)
	createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: proxy.URL + "/base?fixed=1", Token: "proxy-key"}, "a2a": {URL: rpcUp.URL + "/rpc?fixed=1", Token: "a2a-key", ProtocolVersion: "1.0"}})
	for _, prefix := range []string{"/agents/test", "/proxy/test", "/agents/te%73t"} {
		w := dataRequest(g, "GET", prefix+"/api/a%2Fb?q=2", "", "alice", "session")
		if w.Code != 200 || w.Body.String() != "chat" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/agents/test/a2a", "/a2a/test"} {
		w := dataRequest(g, "POST", path, sendV1, "alice", "session")
		var env map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil || env["result"] == nil {
			t.Fatal(w.Body.String(), err)
		}
		task := env["result"].(map[string]any)["task"].(map[string]any)["id"].(string)
		w = dataRequest(g, "POST", path, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, task), "alice", "")
		if strings.Contains(w.Body.String(), `"error"`) || !strings.Contains(w.Body.String(), task) {
			t.Fatal(w.Body.String())
		}
		w = dataRequest(g, "POST", path, strings.Replace(sendV1, "SendMessage", "SendStreamingMessage", 1), "alice", "session")
		if w.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(w.Body.String(), `"taskId":"task_`) {
			t.Fatal(w.Body.String())
		}
	}
	card := dataRequest(g, "GET", "/agents/test/.well-known/agent-card.json", "", "alice", "")
	if card.Code != 200 || !strings.Contains(card.Body.String(), `"url":"http://localhost:8080/agents/test/a2a"`) || strings.Contains(card.Body.String(), "internal") || !strings.Contains(card.Body.String(), "supportedInterfaces") {
		t.Fatal(card.Body.String())
	}
	if proxyCalls.Load() != 3 || rpcCalls.Load() != 6 {
		t.Fatal("requests misrouted", proxyCalls.Load(), rpcCalls.Load())
	}
}

func TestReservedPathsAndDisabledEntries(t *testing.T) {
	var calls atomic.Int64
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL, ProtocolVersion: "1.0"}})
	for _, tc := range []struct {
		method, path, caller string
		status               int
	}{
		{"GET", "/agents/test/a2a", "alice", 405},
		{"POST", "/agents/test/.well-known/agent-card.json", "alice", 405},
		{"POST", "/agents/test/a2a/unknown", "alice", 404},
		{"POST", "/proxy/test/a2a/unknown", "alice", 404},
		{"POST", "/agents/test/.well-known/other", "alice", 404},
		{"POST", "/a2a/test/unknown", "alice", 404},
		{"POST", "/agents/test/a2a", "", 401},
		{"POST", "/a2a/test", "", 401},
	} {
		w := dataRequest(g, tc.method, tc.path, sendV1, tc.caller, "s")
		if w.Code != tc.status {
			t.Fatalf("%s: want %d got %d %s", tc.path, tc.status, w.Code, w.Body.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatal("reserved path reached upstream")
	}
	path := "/admin/v1/routes/" + rt.ID
	w := admin(t, g, "PATCH", path, `{"expose":["proxy"]}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for _, url := range []string{"/agents/test/a2a", "/a2a/test", "/proxy/test/a2a", "/agents/test/.well-known/agent-card.json"} {
		if w := dataRequest(g, "POST", url, sendV1, "alice", "s"); w.Code != 404 {
			t.Fatal(url, w.Code)
		}
	}
	w = admin(t, g, "PATCH", path, `{"expose":["a2a"]}`, `"2"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for _, url := range []string{"/agents/test/api/sessions", "/proxy/test/api/sessions"} {
		if w := dataRequest(g, "GET", url, "", "alice", "s"); w.Code != 404 {
			t.Fatal(url, w.Code)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("disabled entry reached upstream")
	}
}

func TestUnsupportedAdaptersAreNotProxied(t *testing.T) {
	g := setup(t)
	rt := createDual(t, g, localInterfaces())
	routePath := "/admin/v1/routes/" + rt.ID
	if w := admin(t, g, "PATCH", routePath, `{"agent_type":"hermes"}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := dataRequest(g, "POST", "/agents/test/v1/browser-control/register", `{}`, "alice", "s"); w.Code != 501 {
		t.Fatal(w.Code, w.Body.String())
	}
	card := dataRequest(g, "GET", "/agents/test/.well-known/agent-card.json", "", "alice", "")
	if card.Code != 200 || !strings.Contains(card.Body.String(), `"protocolVersion":"1.0"`) {
		t.Fatal(card.Body.String())
	}
}
