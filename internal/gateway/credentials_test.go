package gateway

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestCredentialReferencesNeverUseEnvironment(t *testing.T) {
	t.Setenv("UPSTREAM_TOKEN", "environment-secret")
	opts := testOptions(t.TempDir())
	opts.Credentials = map[string]string{"UPSTREAM_TOKEN": "yaml-secret"}
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	opts.Credentials["UPSTREAM_TOKEN"] = "changed-after-open"
	h := http.Header{"Authorization": []string{"Bearer caller-secret"}}
	g.upstreamHeaders(h, Caller{ID: "alice"}, EndpointInterface{CredentialRef: "UPSTREAM_TOKEN"})
	if h.Get("Authorization") != "Bearer yaml-secret" {
		t.Fatal("credential snapshot changed or environment won")
	}
	q := rpc{Method: "GetTask", Params: map[string]any{"id": "task"}}
	req, err := g.a2aUpstreamRequest(context.Background(), q, Caller{ID: "alice"}, EndpointInterface{URL: "http://localhost:1", CredentialRef: "UPSTREAM_TOKEN"})
	if err != nil || req.Header.Get("Authorization") != "Bearer yaml-secret" {
		t.Fatal("A2A did not use YAML credentials", err)
	}
	body := `{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","credential_ref":"UPSTREAM_TOKEN"}}}]}`
	w := admin(t, g, "POST", "/admin/v1/routes", body, "", "")
	if w.Code != 201 || strings.Contains(w.Body.String(), "yaml-secret") {
		t.Fatal("credential creation or redaction failed", w.Code)
	}
	g.Close()
	opts.Credentials = nil
	opts.Initialize = false
	if reopened, err := Open(opts); err == nil {
		reopened.Close()
		t.Fatal("startup accepted credential available only in environment")
	}
	fresh := setup(t)
	w = admin(t, fresh, "POST", "/admin/v1/routes", body, "", "")
	if w.Code != 400 {
		t.Fatal("management write accepted environment credential", w.Code)
	}
}
