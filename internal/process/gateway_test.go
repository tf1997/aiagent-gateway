package process

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway/runtime/shared"
	p "aiagent-gateway/internal/releaseprotocol"
)

func completeGateway(t *testing.T, dir, incarnation string) (*Gateway, appconfig.Config, p.Target, p.Identity) {
	t.Helper()
	cfg := appconfig.Defaults()
	cfg.DataDir, cfg.AdminToken = dir, "admin-secret"
	id := p.Identity{Release: strings.Repeat("a", 64), Incarnation: incarnation, Instance: "a"}
	target := p.Target{Release: id.Release, Incarnation: id.Incarnation, Instance: id.Instance, URL: "http://gateway", Socket: filepath.Join(dir, "absent.sock")}
	g, err := OpenGateway(cfg, target, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g, cfg, target, id
}

func configureGateway(t *testing.T, g *Gateway, upstream string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"key": "test", "protocol": "proxy", "consumers": []map[string]string{{"id": "alice", "token": "alice-secret"}}, "endpoints": []map[string]any{{"url": upstream}}})
	r := httptest.NewRequest("POST", "/admin/v1/routes", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatalf("configure: %d %s", w.Code, w.Body.String())
	}
}

func processRequest(g *Gateway, path, session string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(`{"input":"hello"}`))
	r.Header.Set("Authorization", "Bearer alice-secret")
	r.Header.Set("X-Session-ID", session)
	r.Header.Set(p.AuthHeader, "forged")
	r.Header.Set(p.OutcomeTrailer, "success")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}

func TestCompleteGatewayLocalExecutionVerdictsAndRecovery(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		for key := range r.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-gateway-rollout-") {
				t.Error("internal header reached upstream", key)
			}
		}
		switch r.URL.Path {
		case "/stream", "/broken":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"text\":\"hello\"}\n\n")
			w.(http.Flusher).Flush()
			if r.URL.Path == "/stream" {
				fmt.Fprint(w, "data: [DONE]\n\n")
			}
		case "/fail":
			http.Error(w, "upstream failed", 500)
		default:
			fmt.Fprint(w, `{"ok":true}`)
		}
	}))
	defer up.Close()
	g, cfg, target, identity := completeGateway(t, t.TempDir(), "first")
	configureGateway(t, g, up.URL)
	traffic := g.entry.Traffic()
	traffic.Revision++
	traffic.Rollout = "evidence"
	if err := g.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path   string
		status int
	}{{"/ok", 200}, {"/stream", 200}, {"/broken", 200}, {"/fail", 500}} {
		w := processRequest(g, "/agents/test"+tc.path, "session")
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
		res := w.Result()
		for key := range res.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-gateway-rollout-") {
				t.Fatal("internal response header leaked", key)
			}
		}
		if res.Header.Get("Trailer") != "" {
			t.Fatal("local executor emitted internal trailers", res.Header)
		}
	}
	stats := g.entry.Stats(time.Time{}, identity.Incarnation)
	if stats.Counts["http"].Success != 1 || stats.Counts["sse"].Success != 1 || stats.Failures != 2 || stats.Active != 0 || calls.Load() != 4 {
		t.Fatalf("verdicts or duplicate execution: %+v calls=%d", stats, calls.Load())
	}
	if g.shared.Count("") != 0 {
		t.Fatal("local execution leaked leases")
	}
	if other, err := OpenGateway(cfg, target, identity); err == nil {
		other.Close()
		t.Fatal("second writer opened")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	identity.Incarnation, target.Incarnation = "second", "second"
	reopened, err := OpenGateway(cfg, target, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if w := processRequest(reopened, "/agents/test/ok", "session"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestIngressDrainKeepsStateAvailable(t *testing.T) {
	g, _, _, _ := completeGateway(t, t.TempDir(), "draining")
	g.StopIngress()
	if g.Ready() {
		t.Fatal("draining process is ready for new ingress")
	}
	if w := processRequest(g, "/agents/test/ok", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
	client := shared.NewLocalClient(g.shared, "candidate")
	defer client.Close()
	var out shared.Response
	if err := client.Call("config", shared.Request{}, &out); err != nil || out.Config == nil {
		t.Fatal("state stopped with ingress", err)
	}
	g.ResumeIngress()
	if !g.Ready() {
		t.Fatal("ingress could not resume")
	}
	g.core.BeginShutdown()
	if g.Ready() {
		t.Fatal("unavailable state owner still advertised readiness")
	}
}
