package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicHandlerAuthenticationAndPrivatePaths(t *testing.T) {
	g, err := Open(Options{DataDir: t.TempDir(), AdminToken: "admin-secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	server := httptest.NewServer(PublicHandler(g.DataHandler(), g.AdminHandler()))
	defer server.Close()
	client := server.Client()
	for _, check := range []struct {
		path, token string
		status      int
	}{
		{"/healthz", "", 200}, {"/readyz", "", 200},
		{"/admin/v1/routes", "", 401}, {"/admin/v1/routes", "caller-token", 401},
		{"/admin/v1/routes", "admin-secret", 200},
		{"/metrics", "", 401}, {"/metrics", "caller-token", 401}, {"/metrics", "admin-secret", 200},
		{"/admin/unknown", "", 401}, {"/admin/unknown", "admin-secret", 404},
		{"/control/v1/rollout", "admin-secret", 404}, {"/_process/metrics", "admin-secret", 404},
	} {
		req, _ := http.NewRequest("GET", server.URL+check.path, nil)
		req.Header.Set("Authorization", "Bearer "+check.token)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != check.status {
			t.Fatalf("%s: status=%d want=%d body=%s err=%v", check.path, res.StatusCode, check.status, body, err)
		}
		if check.path == "/metrics" && check.status == 200 && !strings.Contains(string(body), "gateway_requests_total") {
			t.Fatal("missing metrics on shared HTTP listener")
		}
	}
}
