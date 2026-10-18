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

func TestRouteUpstreamTokenPersistenceAndRedaction(t *testing.T) {
	t.Setenv("UPSTREAM_TOKEN", "environment-secret")
	up := credentialUpstream()
	defer up.Close()
	dir := t.TempDir()
	opts := testOptions(dir)
	g, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	body := fmt.Sprintf(`{"key":"test","consumers":[{"id":"alice","token":"alice-secret"}],"expose":["proxy","a2a"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":%q,"token":"route-secret"},"a2a":{"url":%q,"protocol_version":"1.0","token":"route-secret"}}}]}`, up.URL, up.URL+"/rpc")
	w := admin(t, g, "POST", "/admin/v1/routes", body, "", "create-route")
	if w.Code != 201 || strings.Contains(w.Body.String(), "route-secret") {
		t.Fatal("route response exposed upstream token", w.Code, w.Body.String())
	}
	rt := readRoute(t, g, "test")
	path := "/admin/v1/routes/test"
	epPath := path + "/endpoints/" + rt.Endpoints[0].ID
	for _, target := range []string{path, "/admin/v1/routes", path + "/endpoints", epPath} {
		w = admin(t, g, "GET", target, "", "", "")
		if w.Code != 200 || strings.Contains(w.Body.String(), "route-secret") {
			t.Fatal("management read exposed upstream token", target, w.Body.String())
		}
	}
	w = admin(t, g, "POST", "/admin/v1/routes", body, "", "create-route")
	if w.Code != 201 || strings.Contains(w.Body.String(), "route-secret") {
		t.Fatal("idempotent replay exposed upstream token", w.Body.String())
	}
	assertUpstreamToken(t, g, "route-secret")
	stored, err := os.ReadFile(filepath.Join(dir, "routes.json"))
	if err != nil || !strings.Contains(string(stored), `"token": "route-secret"`) {
		t.Fatal("route token was not persisted", err)
	}
	for _, name := range []string{"config.json", "route-logs.json"} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || strings.Contains(string(raw), "route-secret") || strings.Contains(string(raw), "alice-secret") {
			t.Fatal("non-route file contains credentials", name, err)
		}
	}
	update := fmt.Sprintf(`{"key":"test","endpoints":[{"name":"one","interfaces":{"proxy":{"url":%q},"a2a":{"url":%q,"protocol_version":"1.0"}}}]}`, up.URL, up.URL+"/rpc")
	w = admin(t, g, "POST", "/admin/v1/routes", update, "", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "route-secret") {
		t.Fatal("upsert failed or exposed token", w.Body.String())
	}
	assertUpstreamToken(t, g, "route-secret")
	w = admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"token":"rotated-secret"},"a2a":{"token":"rotated-secret"}}}`, `"1"`, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "rotated-secret") {
		t.Fatal("token rotation failed or exposed token", w.Body.String())
	}
	assertUpstreamToken(t, g, "rotated-secret")
	g.Close()
	g, err = gateway.Open(opts)
	if err != nil {
		t.Fatal("restart lost route token", err)
	}
	defer g.Close()
	assertUpstreamToken(t, g, "rotated-secret")
	w = admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"token":null},"a2a":{"token":null}}}`, `"2"`, "")
	if w.Code != 200 {
		t.Fatal("token removal failed", w.Body.String())
	}
	assertUpstreamToken(t, g, "")
}

func assertUpstreamToken(t *testing.T, g *gateway.Gateway, token string) {
	t.Helper()
	want := ""
	if token != "" {
		want = "Bearer " + token
	}
	proxy := dataRequest(g, "GET", "/agents/test/chat", "", "alice", "credential-check")
	if proxy.Code != 200 || proxy.Body.String() != want {
		t.Fatal("proxy used an unexpected upstream token", proxy.Code, proxy.Body.String())
	}
	rpc := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "credential-check")
	var result struct {
		Result struct {
			Message struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"message"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rpc.Body.Bytes(), &result); err != nil || rpc.Code != 200 || len(result.Result.Message.Parts) != 1 || result.Result.Message.Parts[0].Text != "upstream authorization: "+want {
		t.Fatal("A2A used an unexpected upstream token", rpc.Code, rpc.Body.String(), err)
	}
}

func credentialUpstream() *httptest.Server {
	return newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Get("Authorization")
		if r.URL.Path != "/rpc" {
			fmt.Fprint(w, authorization)
			return
		}
		var q rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": map[string]any{"message": map[string]any{"messageId": "reply", "contextId": "credential-context", "role": "ROLE_AGENT", "parts": []any{map[string]any{"text": "upstream authorization: " + authorization}}}}})
	}))
}

func TestLegacyCredentialReferenceRejected(t *testing.T) {
	g := setup(t)
	w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","protocol":"proxy","endpoints":[{"url":"http://localhost:1","credential_ref":"UPSTREAM_TOKEN"}]}`, "", "")
	if w.Code != 400 {
		t.Fatal("accepted deprecated credential reference", w.Body.String())
	}
	w = admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","token":"bad\nvalue"}}}]}`, "", "")
	if w.Code != 400 || strings.Contains(w.Body.String(), "bad") {
		t.Fatal("accepted or echoed invalid token", w.Body.String())
	}
	w = admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","token":123456789}}}]}`, "", "")
	if w.Code != 400 || strings.Contains(w.Body.String(), "123456789") {
		t.Fatal("accepted or echoed numeric token", w.Body.String())
	}
}

func TestSharedWorkerUsesUpdatedUpstreamToken(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.Header.Get("Authorization")))
	}))
	defer up.Close()
	f := sharedSetup(t)
	rt := create(t, f.core, "proxy", up.URL)
	worker := f.worker(t, "token-worker")
	handler := worker.WorkerHandler(releaseprotocol.Identity{Instance: "a", Release: "test", Incarnation: "token-worker"})
	for i, token := range []string{"first-token", "rotated-token", ""} {
		body, _ := json.Marshal(map[string]any{"interfaces": map[string]any{"proxy": map[string]string{"token": token}}})
		w := admin(t, f.core, "PATCH", "/admin/v1/routes/test/endpoints/"+rt.Endpoints[0].ID, string(body), etag(uint64(i+1)), "")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		req := httptest.NewRequest("GET", "/agents/test/chat", nil)
		req.Header.Set("Authorization", "Bearer alice-secret")
		req.Header.Set("X-Session-ID", "same-session")
		req.Header.Set(releaseprotocol.AuthHeader, "admin-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		want := ""
		if token != "" {
			want = "Bearer " + token
		}
		if response.Code != 200 || response.Body.String() != want {
			t.Fatal("worker did not apply route token", response.Code, response.Body.String())
		}
	}
}

func TestTokenSurvivesUnnamedEndpointUpsert(t *testing.T) {
	up := credentialUpstream()
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]gateway.EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL + "/rpc", ProtocolVersion: "1.0"}})
	epPath := "/admin/v1/routes/test/endpoints/" + rt.Endpoints[0].ID
	w := admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"token":"route-secret"},"a2a":{"token":"route-secret"}}}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	update := fmt.Sprintf(`{"key":"test","endpoints":[{"interfaces":{"proxy":{"url":%q},"a2a":{"url":%q,"protocol_version":"1.0"}}}]}`, up.URL, up.URL+"/rpc")
	w = admin(t, g, "POST", "/admin/v1/routes", update, "", "")
	if w.Code != 200 || readRoute(t, g, "test").Endpoints[0].ID != rt.Endpoints[0].ID {
		t.Fatal("unnamed endpoint identity changed", w.Body.String())
	}
	assertUpstreamToken(t, g, "route-secret")
}
