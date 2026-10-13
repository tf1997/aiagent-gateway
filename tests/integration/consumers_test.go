package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/releaseprotocol"
)

func TestDynamicConsumersRotationRevocationAndRecovery(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, r.Header.Get("X-Gateway-Caller-ID"))
	}))
	defer up.Close()
	o := testOptions(t.TempDir())
	g, err := gateway.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	body := fmt.Sprintf(`{"key":"dynamic","expose":["proxy"],"consumers":[{"id":"app","token":"first-consumer-secret"}],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":%q}}}]}`, up.URL)
	created := admin(t, g, "POST", "/admin/v1/routes", body, "", "create")
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	before := readRoute(t, g, "dynamic")
	if w := consumerCall(g, "/agents/dynamic/chat", "first-consumer-secret", "same"); w.Code != 200 || w.Body.String() != "app" {
		t.Fatal(w.Code, w.Body.String())
	}
	rotation := `{"key":"dynamic","consumers":[{"id":"app","token":"second-consumer-secret"}]}`
	updated := admin(t, g, "POST", "/admin/v1/routes", rotation, "", "rotate")
	if updated.Code != 200 {
		t.Fatal(updated.Code, updated.Body.String())
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "first-consumer-secret", "same"); w.Code != 401 {
		t.Fatal("old token accepted", w.Code)
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "second-consumer-secret", "same"); w.Code != 200 {
		t.Fatal("new token rejected", w.Code)
	}
	if rt := readRoute(t, g, "dynamic"); rt.ID != before.ID || rt.Endpoints[0].ID != before.Endpoints[0].ID || metric(t, g, "gateway_sessions") != 1 {
		t.Fatal("rotation lost identity or binding")
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"dynamic","consumers":[{"id":"app"},{"id":"other","token":"other-secret"}]}`, "", ""); w.Code != 200 {
		t.Fatal("preserving existing token failed", w.Code)
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "second-consumer-secret", "same"); w.Code != 200 {
		t.Fatal("omitted token not retained")
	}
	for _, response := range []string{created.Body.String(), updated.Body.String(), admin(t, g, "GET", "/admin/v1/routes/dynamic", "", "", "").Body.String(), admin(t, g, "GET", "/admin/v1/routes", "", "", "").Body.String(), admin(t, g, "GET", "/admin/v1/audit-logs", "", "", "").Body.String()} {
		if strings.Contains(response, "consumer-secret") || strings.Contains(response, "token_hash") || strings.Contains(response, consumerTokenHash("first-consumer-secret")) {
			t.Fatal("management response leaked credentials")
		}
	}
	raw, err := os.ReadFile(filepath.Join(o.DataDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "consumer-secret") || strings.Contains(string(raw), "other-secret") || !strings.Contains(string(raw), consumerTokenHash("second-consumer-secret")) {
		t.Fatal("tokens were not persisted as hashes")
	}
	g.Close()
	g, err = gateway.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "second-consumer-secret", "same"); w.Code != 200 {
		t.Fatal("consumer not recovered", w.Code)
	}
	replay := admin(t, g, "POST", "/admin/v1/routes", body, "", "create")
	if replay.Code != 201 || replay.Body.String() != created.Body.String() {
		t.Fatal("receipt not retained")
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "first-consumer-secret", "same"); w.Code != 401 {
		t.Fatal("receipt restored old credential")
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"dynamic","consumers":[]}`, "", ""); w.Code != 200 {
		t.Fatal("consumer revoke failed", w.Code)
	}
	if w := consumerCall(g, "/agents/dynamic/chat", "second-consumer-secret", "same"); w.Code != 401 {
		t.Fatal("revoked token accepted")
	}
}

func TestConsumersSharedWorkerAndA2AAuthentication(t *testing.T) {
	f := sharedSetup(t)
	rt := createDual(t, f.core, localInterfaces())
	worker := f.worker(t, "consumer-worker")
	identity := releaseprotocol.Identity{Instance: "a", Release: "test", Incarnation: "consumer-worker"}
	request := func(token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/agents/test/.well-known/agent-card.json", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set(releaseprotocol.AuthHeader, "admin-secret")
		w := httptest.NewRecorder()
		worker.WorkerHandler(identity).ServeHTTP(w, r)
		return w
	}
	if w := request("alice-secret"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	q := releaseprotocol.Request{Path: "/agents/test/a2a", Method: "POST", Header: http.Header{"Authorization": []string{"Bearer alice-secret"}}}
	if r, err := gateway.ResolveShared(f.socket, "admin-secret", q); err != nil || r.Status != 0 {
		t.Fatal(r, err)
	}
	w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test","consumers":[{"id":"alice","token":"new-secret"}]}`, "", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if r, err := gateway.ResolveShared(f.socket, "admin-secret", q); err != nil || r.Status != 401 {
		t.Fatal("entry retained revoked token", r, err)
	}
	if w := request("alice-secret"); w.Code != 401 {
		t.Fatal("worker retained revoked token", w.Code)
	}
	if w := request("new-secret"); w.Code != 200 {
		t.Fatal("worker did not refresh credentials", w.Code, w.Body.String())
	}
	if readRoute(t, worker, "test").ID != rt.ID {
		t.Fatal("shared update changed route")
	}
}

func TestDynamicConsumerPatchAndInputLimit(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, r.Header.Get("X-Gateway-Caller-ID"))
	}))
	defer up.Close()
	g := setup(t)
	rt := create(t, g, "proxy", up.URL)
	body := `{"consumers":[{"id":"alice","token":"patch-token"}]}`
	w := admin(t, g, "PATCH", "/admin/v1/routes/test", body, etag(rt.Version), "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "patch-token") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := consumerCall(g, "/agents/test/chat", "patch-token", "same"); w.Code != 200 || w.Body.String() != "alice" {
		t.Fatal("patched consumer was not authenticated", w.Code, w.Body.String())
	}
	if w := consumerCall(g, "/agents/test/chat", "alice-secret", "same"); w.Code != 401 {
		t.Fatal("old consumer token still accepted", w.Code)
	}
	// The API accepts at most 1,000 consumers on a route.
	inputs := make([]map[string]string, 1000+1)
	for i := range inputs {
		inputs[i] = map[string]string{"id": fmt.Sprintf("app-%d", i), "token": fmt.Sprintf("token-%d", i)}
	}
	raw, _ := json.Marshal(map[string]any{"key": "test", "consumers": inputs})
	if w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", ""); w.Code != 400 {
		t.Fatal("consumer limit not enforced", w.Code)
	}
}
