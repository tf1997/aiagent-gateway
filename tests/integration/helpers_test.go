package integration_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/protocol"
)

// These fixtures exercise the HTTP contract without calling adapter internals.
type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  map[string]any  `json:"params"`
}

func newTestServer(h http.Handler) *httptest.Server { return httptest.NewServer(h) }

func testClient() *http.Client { return &http.Client{Timeout: 3 * time.Second} }

func etag(version uint64) string { return fmt.Sprintf(`"%d"`, version) }

func consumerTokenHash(token string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(token))) }

func readRoute(t *testing.T, g *gateway.Gateway, key string) *gateway.Route {
	t.Helper()
	w := admin(t, g, "GET", "/admin/v1/routes/"+url.PathEscape(key), "", "", "")
	var route gateway.Route
	if err := json.Unmarshal(w.Body.Bytes(), &route); err != nil || w.Code != http.StatusOK {
		t.Fatalf("read route %s: %d %s (%v)", key, w.Code, w.Body.String(), err)
	}
	return &route
}

func adminItems[T any](t *testing.T, g *gateway.Gateway, path string, query url.Values) []T {
	t.Helper()
	if query == nil {
		query = url.Values{}
	}
	var items []T
	for {
		query.Set("offset", strconv.Itoa(len(items)))
		query.Set("limit", "100")
		w := admin(t, g, "GET", path+"?"+query.Encode(), "", "", "")
		var page struct {
			Items []T `json:"items"`
			Total int `json:"total"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != http.StatusOK {
			t.Fatalf("read %s: %d %s (%v)", path, w.Code, w.Body.String(), err)
		}
		items = append(items, page.Items...)
		if len(items) == page.Total {
			return items
		}
		if len(page.Items) == 0 || len(items) > page.Total {
			t.Fatalf("invalid admin pagination: %s", w.Body.String())
		}
	}
}

func readRoutes(t *testing.T, g *gateway.Gateway) []gateway.Route {
	t.Helper()
	return adminItems[gateway.Route](t, g, "/admin/v1/routes", nil)
}

func readResources(t *testing.T, g *gateway.Gateway, owner, route string, kind protocol.ResourceKind, endpoint string) []protocol.Resource {
	t.Helper()
	if route == "" {
		var resources []protocol.Resource
		for _, rt := range readRoutes(t, g) {
			resources = append(resources, readResources(t, g, owner, rt.ID, kind, endpoint)...)
		}
		return resources
	}
	query := url.Values{"owner_id": {owner}, "resource_kind": {string(kind)}, "endpoint_id": {endpoint}}
	return adminItems[protocol.Resource](t, g, "/admin/v1/routes/"+url.PathEscape(route)+"/resources", query)
}

func readResource(t *testing.T, g *gateway.Gateway, key protocol.ResourceKey) protocol.Resource {
	t.Helper()
	for _, resource := range readResources(t, g, key.Owner, key.Route, key.Kind, "") {
		if resource.ID == key.ID {
			return resource
		}
	}
	t.Fatalf("resource %s/%s not found", key.Kind, key.ID)
	return protocol.Resource{}
}

func metric(t *testing.T, g *gateway.Gateway, name string) int64 {
	t.Helper()
	w := admin(t, g, "GET", "/metrics", "", "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("read metrics: %d %s", w.Code, w.Body.String())
	}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func persistenceHealthy(t *testing.T, g *gateway.Gateway) bool {
	t.Helper()
	w := admin(t, g, "GET", "/admin/v1/config/status", "", "", "")
	var status struct {
		Healthy bool `json:"persistence_healthy"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil || w.Code != http.StatusOK {
		t.Fatalf("read config status: %d %s (%v)", w.Code, w.Body.String(), err)
	}
	return status.Healthy
}

func waitFor(t *testing.T, timeout time.Duration, description string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-tick.C:
		}
	}
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	protocol.WriteJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}

// The fake upstream takes its task ID from the message text, while the gateway
// creates and returns the public task/context IDs through SendMessage.
func sendA2ATask(t *testing.T, g *gateway.Gateway, owner, name string) map[string]any {
	t.Helper()
	body := rpcBody("SendMessage", map[string]any{
		"configuration": map[string]any{"returnImmediately": true},
		"message":       map[string]any{"messageId": "message-" + name, "role": "ROLE_USER", "parts": []any{map[string]any{"text": name}}},
	})
	return rpcTask(t, dataRequest(g, "POST", "/agents/test/a2a", body, owner, "task-fixture"), true)
}

func rpcTask(t *testing.T, w *httptest.ResponseRecorder, wrapped bool) map[string]any {
	t.Helper()
	var response struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != http.StatusOK || response.Result == nil {
		t.Fatalf("RPC task: %d %s (%v)", w.Code, w.Body.String(), err)
	}
	task := response.Result
	if wrapped {
		task, _ = task["task"].(map[string]any)
	}
	if id, ok := task["id"].(string); !ok || !strings.HasPrefix(id, "task_") {
		t.Fatalf("missing public task ID: %s", w.Body.String())
	}
	return task
}

func requestedTask(q rpcMessage) string {
	message := q.Params["message"].(map[string]any)
	return message["parts"].([]any)[0].(map[string]any)["text"].(string)
}

func addEndpoint(t *testing.T, g *gateway.Gateway, upstream string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"url": upstream})
	w := admin(t, g, "POST", "/admin/v1/routes/test/endpoints", string(body), "", "")
	if w.Code != http.StatusCreated {
		t.Fatalf("add endpoint: %d %s", w.Code, w.Body.String())
	}
}

func rpcBody(method string, params map[string]any) string {
	b, _ := json.Marshal(rpcMessage{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: method, Params: params})
	return string(b)
}

func requireRPCCode(t *testing.T, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	var env struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(w.Body.Bytes(), &env) != nil || env.Error.Code != code {
		t.Fatalf("want RPC %d: %s", code, w.Body.String())
	}
}

func consumerCall(g *gateway.Gateway, path, token, session string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Session-ID", session)
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}

func testOptions(dir string) gateway.Options {
	return gateway.Options{DataDir: dir, AdminToken: "admin-secret", PublicURL: "http://localhost:8080"}
}

func testConsumers() []map[string]string {
	return []map[string]string{{"id": "alice", "token": "alice-secret"}, {"id": "bob", "token": "bob-secret"}}
}

func setup(t *testing.T, configure ...func(*gateway.Options)) *gateway.Gateway {
	t.Helper()
	opts := testOptions(t.TempDir())
	for _, change := range configure {
		change(&opts)
	}
	g, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func admin(t *testing.T, g *gateway.Gateway, method, path, body, etag, idem string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer admin-secret")
	r.Header.Set("If-Match", etag)
	r.Header.Set("Idempotency-Key", idem)
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, r)
	return w
}

func create(t *testing.T, g *gateway.Gateway, entry string, urls ...string) gateway.Route {
	t.Helper()
	eps := []map[string]any{}
	for _, u := range urls {
		eps = append(eps, map[string]any{"url": u, "weight": 100})
	}
	v := map[string]any{"consumers": testConsumers(), "key": "test", "protocol": entry, "endpoints": eps}
	if entry == "a2a" {
		v["agent_card"] = map[string]any{"name": "test", "description": "test", "capabilities": map[string]any{"streaming": true}, "version": "1", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}}
	}
	raw, _ := json.Marshal(v)
	w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", "")
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	var rt gateway.Route
	json.Unmarshal(w.Body.Bytes(), &rt)
	return rt
}

func rpcCall(t *testing.T, g *gateway.Gateway, caller, body, sid string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("POST", "/a2a/test", strings.NewReader(body))
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Authorization", "Bearer "+caller+"-secret")
	if sid != "" {
		r.Header.Set("X-Session-ID", sid)
	}
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	var v map[string]any
	if json.Unmarshal(w.Body.Bytes(), &v) != nil {
		t.Fatalf("bad rpc %s", w.Body.String())
	}
	return v
}

const sendV1 = `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`

func createDual(t *testing.T, g *gateway.Gateway, eps ...map[string]gateway.EndpointInterface) gateway.Route {
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
	var rt gateway.Route
	if err := json.Unmarshal(w.Body.Bytes(), &rt); err != nil {
		t.Fatal(err)
	}
	return rt
}

func dataRequest(g *gateway.Gateway, method, path, body, caller, sid string) *httptest.ResponseRecorder {
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

func localInterfaces() map[string]gateway.EndpointInterface {
	return map[string]gateway.EndpointInterface{"proxy": {URL: "http://localhost:1"}, "a2a": {URL: "http://localhost:2/rpc", ProtocolVersion: "1.0"}}
}

func writeTestTask(w http.ResponseWriter, q rpcMessage, task map[string]any) {
	var result any = task
	if q.Method == "SendMessage" {
		result = map[string]any{"task": task}
	}
	protocol.WriteJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": result})
}
