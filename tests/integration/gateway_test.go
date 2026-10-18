package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway"
)

func TestMissingStoreInitializesAutomatically(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	g, err := gateway.Open(gateway.Options{DataDir: dir, AdminToken: "admin-secret", DeferBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	raw, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c gateway.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if c.Schema != 2 || c.Revision != 1 || len(c.Routes) != 0 || !persistenceHealthy(t, g) {
		t.Fatal("new store not initialized", c)
	}
	for _, name := range []string{"routes.json", "route-logs.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal("missing store part", name, err)
		}
	}
}

func TestDanglingStoreSymlinkIsNotInitialized(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	target := filepath.Join(dir, "missing.json")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if g, err := gateway.Open(testOptions(dir)); err == nil {
		g.Close()
		t.Fatal("unreadable existing store was initialized")
	}
	if got, err := os.Readlink(path); err != nil || got != target {
		t.Fatal("existing store symlink was replaced", got, err)
	}
}

func TestUnavailableSharedStateDoesNotInitializeLocalStore(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	opts := gateway.Options{DataDir: dir, AdminToken: "admin-secret", StateSocket: filepath.Join(root, "missing.sock"), WorkerID: "test-worker"}
	if g, err := gateway.Open(opts); err == nil {
		g.Close()
		t.Fatal("worker started without shared state")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("worker initialized a local store", err)
	}
}

func TestStoreCRUDRecovery(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(dir)
	g, err := gateway.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := gateway.Open(o); err == nil {
		other.Close()
		t.Fatal("second writer allowed")
	}
	body := `{"key":"test","protocol":"proxy","endpoints":[{"url":"http://localhost:1234"}]}`
	first := admin(t, g, "POST", "/admin/v1/routes", body, "", "key")
	if first.Code != 201 {
		t.Fatal(first.Body.String())
	}
	again := admin(t, g, "POST", "/admin/v1/routes", body, "", "key")
	if first.Body.String() != again.Body.String() {
		t.Fatal("idempotency changed response")
	}
	if admin(t, g, "POST", "/admin/v1/routes", body+" ", "", "key").Code != 409 {
		t.Fatal("idempotency collision")
	}
	var rt gateway.Route
	json.Unmarshal(first.Body.Bytes(), &rt)
	path := "/admin/v1/routes/" + rt.ID
	if admin(t, g, "PATCH", path, `{"enabled":false}`, `"99"`, "").Code != 412 {
		t.Fatal("ETag not enforced")
	}
	if w := admin(t, g, "PATCH", path, `{"enabled":false}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if readRoute(t, g, "test").Enabled {
		t.Fatal("not disabled")
	}
	audit := admin(t, g, "GET", "/admin/v1/audit-logs", "", "", "")
	if audit.Code != 200 || !strings.Contains(audit.Body.String(), "PATCH") {
		t.Fatal("missing audit", audit.Body.String())
	}
	g.Close()
	g, err = gateway.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if readRoute(t, g, "test").Enabled {
		t.Fatal("recovery lost config")
	}
	if metric(t, g, "gateway_sessions") != 0 {
		t.Fatal("bindings persisted")
	}
	if admin(t, g, "POST", "/admin/v1/routes", body, "", "key").Body.String() != first.Body.String() {
		t.Fatal("receipt not recovered")
	}
	if got := admin(t, g, "GET", "/admin/v1/audit-logs", "", "", ""); got.Code != 200 || got.Body.String() != audit.Body.String() {
		t.Fatal("audit not recovered", got.Body.String())
	}
}

func TestCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if g, err := gateway.Open(testOptions(dir)); err == nil {
		g.Close()
		t.Fatal("corrupt file accepted")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "broken" {
		t.Fatal("corrupt file was overwritten", err)
	}
}

func TestProxyStreamingAndCancellation(t *testing.T) {
	cancelled := make(chan struct{})
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-secret" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Gateway-Caller-ID") != "alice" {
			t.Error("credential isolation failed")
		}
		if r.URL.Path != "/base/stream" || r.URL.Query().Get("q") != "hello" {
			t.Error("URL rewrite", r.URL)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	}))
	defer up.Close()
	g := setup(t)
	rt := create(t, g, "proxy", up.URL+"/base")
	w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, `{"token":"upstream-secret"}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	srv := newTestServer(g.DataHandler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/proxy/test/stream?q=hello", strings.NewReader("hello"))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("Cookie", "secret=client")
	req.Header.Set("X-Session-ID", "s")
	req.Header.Set("X-Gateway-Caller-ID", "forged")
	res, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(res.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatal("buffered stream", line, err)
	}
	res.Body.Close()
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("cancel not propagated")
	}
}

func TestProxyStatusAndPath(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(418)
		fmt.Fprint(w, r.URL.EscapedPath()+"?"+r.URL.RawQuery)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "proxy", up.URL+"/base?fixed=1")
	r := httptest.NewRequest("GET", "/proxy/test/a%2Fb?q=x", nil)
	r.Header.Set("Authorization", "Bearer alice-secret")
	r.Header.Set("X-Session-ID", "s")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	if w.Code != 418 || w.Body.String() != "/base/a%2Fb?fixed=1&q=x" {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestHTTPBodyTimeout(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	g := setup(t)
	rt := create(t, g, "proxy", up.URL)
	admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"timeout":{"stream_idle_ms":50,"max_duration_ms":300}}`, `"1"`, "")
	s := newTestServer(g.DataHandler())
	defer s.Close()
	req, _ := http.NewRequest("GET", s.URL+"/proxy/test/x", nil)
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "a")
	client := testClient()
	res, err := client.Do(req)
	if err == nil {
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
}

func TestRestartDropsTasks(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
		_ = json.NewDecoder(r.Body).Decode(&q)
		writeTestTask(w, q, map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}})
	}))
	defer up.Close()
	dir := t.TempDir()
	g, err := gateway.Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	create(t, g, "a2a", up.URL)
	task := rpcTask(t, dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "s"), true)
	id := task["id"].(string)
	g.Close()
	g, err = gateway.Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	v := rpcCall(t, g, "alice", fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":%q}}`, id), "")
	if v["error"] == nil {
		t.Fatal("task survived restart")
	}
}
