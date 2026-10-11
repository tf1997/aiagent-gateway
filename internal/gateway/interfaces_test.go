package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const sendV1 = `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`

func createDual(t *testing.T, g *Gateway, eps ...map[string]EndpointInterface) Route {
	t.Helper()
	endpoints := []map[string]any{}
	for _, ep := range eps {
		endpoints = append(endpoints, map[string]any{"interfaces": ep})
	}
	raw, _ := json.Marshal(map[string]any{"consumers": testConsumers(), "key": "test", "expose": []string{"proxy", "a2a"}, "endpoints": endpoints, "agent_card": map[string]any{"name": "test", "description": "test", "version": "1", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}, "capabilities": map[string]any{"streaming": true}, "supportedInterfaces": []any{map[string]any{"url": "http://internal:9900"}}}})
	w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", "")
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var rt Route
	if err := json.Unmarshal(w.Body.Bytes(), &rt); err != nil {
		t.Fatal(err)
	}
	return rt
}
func dataRequest(g *Gateway, method, path, body, caller, sid string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if caller != "" {
		r.Header.Set("Authorization", "Bearer "+caller+"-secret")
	}
	if sid != "" {
		r.Header.Set("X-Session-ID", sid)
	}
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Cookie", "untrusted=1")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}
func localInterfaces() map[string]EndpointInterface {
	return map[string]EndpointInterface{"proxy": {URL: "http://localhost:1"}, "a2a": {URL: "http://localhost:2/rpc", ProtocolVersion: "1.0"}}
}
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
		var q rpc
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
	createDual(t, g, map[string]EndpointInterface{"proxy": {URL: proxy.URL + "/base?fixed=1", CredentialRef: "TEST_PROXY_KEY"}, "a2a": {URL: rpcUp.URL + "/rpc?fixed=1", CredentialRef: "TEST_A2A_KEY", ProtocolVersion: "1.0"}})
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
	rt := createDual(t, g, map[string]EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL, ProtocolVersion: "1.0"}})
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

func TestInterfaceHealthAndSharedCapacity(t *testing.T) {
	var failProxy atomic.Bool
	failProxy.Store(true)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer health-key" {
			t.Error("health request missing credential")
		}
		if r.URL.Path == "/proxy-health" && failProxy.Load() {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/ok")
			w.WriteHeader(302)
			return
		}
		w.WriteHeader(200)
	}))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]EndpointInterface{"proxy": {URL: up.URL, HealthURL: up.URL + "/proxy-health", CredentialRef: "TEST_HEALTH_KEY"}, "a2a": {URL: up.URL + "/rpc", HealthURL: up.URL + "/a2a-health", CredentialRef: "TEST_HEALTH_KEY", ProtocolVersion: "1.0"}})
	g.checkHealth(context.Background())
	if l, err := g.acquire("alice", "test", "new", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("unhealthy proxy selected")
	}
	a, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		t.Fatal("proxy failure disabled A2A", err)
	}
	failProxy.Store(false)
	g.checkHealth(context.Background())
	g.opts.MaxPerEndpoint = 1
	if l, err := g.acquire("alice", "test", "same", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("endpoint concurrency not shared")
	}
	a.release()
	g.opts.MaxPerEndpoint = 2
	p, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err = g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		p.release()
		t.Fatal(err)
	}
	detail := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, "", "", "")
	var result struct {
		Runtime struct {
			Inflight   int `json:"http_inflight"`
			Interfaces map[string]struct {
				Inflight int `json:"http_inflight"`
			} `json:"interfaces"`
		} `json:"runtime"`
	}
	if err := json.Unmarshal(detail.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Runtime.Inflight != 2 || result.Runtime.Interfaces["proxy"].Inflight != 1 || result.Runtime.Interfaces["a2a"].Inflight != 1 {
		t.Fatal(detail.Body.String())
	}
	p.release()
	a.release()
	if g.inflight.Load() != 0 || g.state(rt.ID, rt.Endpoints[0].ID).active.Load() != 0 {
		t.Fatal("counter leak")
	}
	epPath := "/admin/v1/routes/" + rt.ID + "/endpoints/" + rt.Endpoints[0].ID
	w := admin(t, g, "PATCH", epPath, fmt.Sprintf(`{"interfaces":{"proxy":{"health_url":%q}}}`, up.URL+"/redirect"), `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	g.checkHealth(context.Background())
	if g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Load() {
		t.Fatal("redirect reported healthy")
	}
	w = admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"health_url":null}}}`, `"2"`, "")
	if w.Code != 200 || !g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Load() {
		t.Fatal("removed health URL left stale failure", w.Body.String())
	}
}

func TestInterfaceBindingsReferencesAndPatches(t *testing.T) {
	g := setup(t)
	rt := createDual(t, g, localInterfaces(), localInterfaces())
	g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Store(false)
	p, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	p.release()
	a, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		t.Fatal(err)
	}
	a.release()
	if p.ep.ID == a.ep.ID {
		t.Fatal("proxy and A2A shared a session namespace")
	}
	epPath := "/admin/v1/routes/" + rt.ID + "/endpoints/" + a.ep.ID
	routePath := "/admin/v1/routes/" + rt.ID
	for _, body := range []string{`{"interfaces":{"a2a":{"url":"http://localhost:99"}}}`, `{"interfaces":{"a2a":{"tenant":"changed"}}}`, `{"interfaces":{"a2a":null}}`} {
		if w := admin(t, g, "PATCH", epPath, body, `"1"`, ""); w.Code != 409 {
			t.Fatal("changed bound endpoint", w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{"expose":["proxy"]}`, `{"agent_type":"hermes"}`, `{"key":"other"}`} {
		if w := admin(t, g, "PATCH", routePath, body, `"1"`, ""); w.Code != 409 {
			t.Fatal("changed bound route", w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "PATCH", epPath, `{"url":"http://localhost:99"}`, `"1"`, ""); w.Code != 409 {
		t.Fatal("ambiguous legacy endpoint patch accepted")
	}
	if w := admin(t, g, "PATCH", routePath, `{"protocol":"proxy"}`, `"1"`, ""); w.Code != 409 {
		t.Fatal("ambiguous legacy route patch accepted")
	}
	if w := admin(t, g, "PATCH", epPath, `{"state":"draining","weight":50}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	bound, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil || bound.ep.ID != a.ep.ID {
		t.Fatal("drain broke binding", err)
	}
	bound.release()
	fresh, err := g.acquire("alice", "test", "fresh", "", "a2a", nil)
	if err != nil || fresh.ep.ID == a.ep.ID {
		t.Fatal("draining accepted new session", err)
	}
	fresh.release()
	old := g.current.Load().Routes["test"]
	w := admin(t, g, "PATCH", routePath, `{"allowed_methods":["GET"]}`, tag(old.Version), "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if l, err := g.acquire("alice", "test", "stale", "", "a2a", old); err == nil {
		l.release()
		t.Fatal("stale route accepted")
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

func writeTestTask(w http.ResponseWriter, q rpc, task map[string]any) {
	var result any = task
	if q.Method == "SendMessage" {
		result = map[string]any{"task": task}
	}
	writeJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": result})
}
