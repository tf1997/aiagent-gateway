package gateway

import (
	"aiagent-gateway/internal/releaseprotocol"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRouteUpstreamTokenPersistenceAndRedaction(t *testing.T) {
	t.Setenv("UPSTREAM_TOKEN", "environment-secret")
	dir := t.TempDir()
	opts := testOptions(dir)
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	body := `{"key":"test","expose":["proxy"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":"http://localhost:1","token":"route-secret"}}}]}`
	w := admin(t, g, "POST", "/admin/v1/routes", body, "", "create-route")
	if w.Code != 201 || strings.Contains(w.Body.String(), "route-secret") {
		t.Fatal("route response exposed upstream token", w.Code, w.Body.String())
	}
	rt := g.current.Load().Routes["test"]
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
	stored, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil || !strings.Contains(string(stored), `"token": "route-secret"`) {
		t.Fatal("route token was not persisted", err)
	}
	w = admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","expose":["proxy"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, "", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "route-secret") {
		t.Fatal("upsert failed or exposed token", w.Body.String())
	}
	assertUpstreamToken(t, g, "route-secret")
	w = admin(t, g, "PATCH", epPath, `{"token":"rotated-secret"}`, `"1"`, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "rotated-secret") {
		t.Fatal("token rotation failed or exposed token", w.Body.String())
	}
	assertUpstreamToken(t, g, "rotated-secret")
	g.Close()
	opts.Initialize = false
	g, err = Open(opts)
	if err != nil {
		t.Fatal("restart lost route token", err)
	}
	defer g.Close()
	assertUpstreamToken(t, g, "rotated-secret")
	w = admin(t, g, "PATCH", epPath, `{"token":null}`, `"2"`, "")
	if w.Code != 200 || g.current.Load().Routes["test"].Endpoints[0].Interfaces["proxy"].Token != "" {
		t.Fatal("token removal failed", w.Body.String())
	}
}

func assertUpstreamToken(t *testing.T, g *Gateway, want string) {
	t.Helper()
	iface := g.current.Load().Routes["test"].Endpoints[0].Interfaces["proxy"]
	h := http.Header{"Authorization": []string{"Bearer caller-secret"}}
	g.upstreamHeaders(h, Caller{ID: "alice"}, iface)
	if h.Get("Authorization") != "Bearer "+want {
		t.Fatal("proxy used an unexpected upstream token")
	}
	q := rpc{Method: "GetTask", Params: map[string]any{"id": "task"}}
	iface.URL = "http://localhost:1"
	req, err := g.a2aUpstreamRequest(context.Background(), q, Caller{ID: "alice"}, iface)
	if err != nil || req.Header.Get("Authorization") != "Bearer "+want {
		t.Fatal("A2A used an unexpected upstream token", err)
	}
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
		w := admin(t, f.core, "PATCH", "/admin/v1/routes/test/endpoints/"+rt.Endpoints[0].ID, string(body), tag(uint64(i+1)), "")
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
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	epPath := "/admin/v1/routes/test/endpoints/" + rt.Endpoints[0].ID
	w := admin(t, g, "PATCH", epPath, `{"token":"route-secret"}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, "", "")
	if w.Code != 200 || g.current.Load().Routes["test"].Endpoints[0].ID != rt.Endpoints[0].ID {
		t.Fatal("unnamed endpoint identity changed", w.Body.String())
	}
	assertUpstreamToken(t, g, "route-secret")
}
