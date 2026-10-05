package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testOptions(dir string) Options {
	return Options{DataDir: dir, AdminToken: "admin-secret", Callers: []Caller{{ID: "alice", Token: "alice-secret", Routes: []string{"*"}}, {ID: "bob", Token: "bob-secret", Routes: []string{"*"}}}, Initialize: true, dialContext: testDial, PublicURL: "http://localhost:8080"}
}
func setup(t *testing.T) *Gateway {
	t.Helper()
	g, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}
func admin(t *testing.T, g *Gateway, method, path, body, etag, idem string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer admin-secret")
	r.Header.Set("If-Match", etag)
	r.Header.Set("Idempotency-Key", idem)
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, r)
	return w
}
func create(t *testing.T, g *Gateway, protocol string, urls ...string) Route {
	t.Helper()
	eps := []map[string]any{}
	for _, u := range urls {
		eps = append(eps, map[string]any{"url": u, "weight": 100})
	}
	v := map[string]any{"key": "test", "protocol": protocol, "endpoints": eps}
	if protocol == "a2a" {
		v["agent_card"] = map[string]any{"name": "test", "description": "test", "capabilities": map[string]any{"streaming": true}, "version": "1", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}}
	}
	raw, _ := json.Marshal(v)
	w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", "")
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	var rt Route
	json.Unmarshal(w.Body.Bytes(), &rt)
	return rt
}
func TestStoreCRUDRecovery(t *testing.T) {
	dir := t.TempDir()
	o := testOptions(dir)
	g, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(o); err == nil {
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
	var rt Route
	json.Unmarshal(first.Body.Bytes(), &rt)
	path := "/admin/v1/routes/" + rt.ID
	if admin(t, g, "PATCH", path, `{"enabled":false}`, `"99"`, "").Code != 412 {
		t.Fatal("ETag not enforced")
	}
	if w := admin(t, g, "PATCH", path, `{"enabled":false}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if g.current.Load().Routes["test"].Enabled {
		t.Fatal("not disabled")
	}
	g.Close()
	g, err = Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.current.Load().Routes["test"].Enabled {
		t.Fatal("recovery lost config")
	}
	if g.bindings.Load() != 0 {
		t.Fatal("bindings persisted")
	}
	if admin(t, g, "POST", "/admin/v1/routes", body, "", "key").Body.String() != first.Body.String() {
		t.Fatal("receipt not recovered")
	}
}
func TestPersistenceFailure(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	g.persist = func(Config) (bool, error) { return false, fmt.Errorf("disk full") }
	w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"enabled":false}`, `"1"`, "")
	if w.Code != 503 || g.current.Load() != before || !g.healthy.Load() {
		t.Fatal("pre-rename failure changed state")
	}
	g.persist = func(Config) (bool, error) { return true, fmt.Errorf("directory fsync") }
	w = admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"enabled":false}`, `"1"`, "")
	if w.Code != 503 || g.healthy.Load() {
		t.Fatal("uncertain commit remained ready")
	}
}
func TestCorruptFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("broken"), 0600)
	if g, err := Open(testOptions(dir)); err == nil {
		g.Close()
		t.Fatal("corrupt file accepted")
	}
}
func TestStickyConcurrentDrain(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1", "http://localhost:2")
	var wg sync.WaitGroup
	ids := make(chan string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := g.acquire("alice", "test", "same", "", "proxy", nil)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- l.ep.ID
			l.release()
		}()
	}
	wg.Wait()
	close(ids)
	chosen := ""
	for id := range ids {
		if chosen != "" && chosen != id {
			t.Fatal("split binding")
		}
		chosen = id
	}
	path := "/admin/v1/routes/" + rt.ID + "/endpoints/" + chosen
	w := admin(t, g, "PATCH", path, `{"state":"draining"}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	l, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil || l.ep.ID != chosen {
		t.Fatal("drain broke affinity", err)
	}
	l.release()
	other, err := g.acquire("alice", "test", "new", "", "proxy", nil)
	if err != nil || other.ep.ID == chosen {
		t.Fatal("new session sent to draining")
	}
	other.release()
	if admin(t, g, "DELETE", path, "", `"2"`, "").Code != 409 {
		t.Fatal("deleted bound endpoint")
	}
	g.state(rt.ID, chosen).proxy.healthy.Store(false)
	if l, err := g.acquire("alice", "test", "same", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("failed endpoint silently reselected")
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
	t.Setenv("UPSTREAM_TEST_TOKEN", "upstream-secret")
	w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, `{"credential_ref":"UPSTREAM_TEST_TOKEN"}`, `"1"`, "")
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
func rpcCall(t *testing.T, g *Gateway, caller, body, sid string) map[string]any {
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
func TestA2ATaskMappingIsolation(t *testing.T) {
	var queried string
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpc
		json.NewDecoder(r.Body).Decode(&q)
		if q.Method == "GetTask" {
			queried, _ = q.Params["id"].(string)
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}, "artifacts": []any{map[string]any{"artifactId": "a", "parts": []any{map[string]any{"data": map[string]any{"taskId": "user-data"}}}}}})
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	v := rpcCall(t, g, "alice", `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`, "s")
	result, ok := v["result"].(map[string]any)
	if !ok {
		t.Fatal(v)
	}
	result = result["task"].(map[string]any)
	id := result["id"].(string)
	ctx := result["contextId"].(string)
	if id == "up-task" || ctx == "up-context" {
		t.Fatal("raw IDs exposed")
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, id)
	v = rpcCall(t, g, "alice", body, "")
	if v["error"] != nil || queried != "up-task" {
		t.Fatal(v, queried)
	}
	v = rpcCall(t, g, "bob", body, "")
	if v["error"] == nil {
		t.Fatal("cross caller task access")
	}
	raw, _ := json.Marshal(result)
	if !bytes.Contains(raw, []byte(`"taskId":"user-data"`)) {
		t.Fatal("arbitrary data rewritten")
	}
	v = rpcCall(t, g, "alice", body, "conflicting")
	if v["error"] == nil {
		t.Fatal("session conflict accepted")
	}
}
func TestA2AStream(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"task\":{\"id\":\"up\",\"contextId\":\"ctx\",\"status\":{\"state\":\"TASK_STATE_WORKING\"}}}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"statusUpdate\":{\"taskId\":\"up\",\"contextId\":\"ctx\",\"status\":{\"state\":\"TASK_STATE_COMPLETED\"}}}}\n\n")
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	r := httptest.NewRequest("POST", "/a2a/test", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`))
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Authorization", "Bearer alice-secret")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `"taskId":"task_`) || !strings.Contains(w.Body.String(), `"contextId":"ctx_`) {
		t.Fatal(w.Body.String())
	}
}
func TestSessionCapacityAndTTL(t *testing.T) {
	g := setup(t)
	g.opts.MaxSessions = 1
	create(t, g, "proxy", "http://localhost:1")
	l, err := g.acquire("alice", "test", "a", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
	if l, err := g.acquire("alice", "test", "b", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("capacity ignored")
	}
	key := sessionKey{"alice", g.current.Load().Routes["test"].ID, "a", "proxy"}
	sh := g.shard(key)
	sh.mu.Lock()
	sh.items[key].Seen = time.Now().Add(-48 * time.Hour)
	sh.mu.Unlock()
	l, err = g.acquire("alice", "test", "a", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
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
func BenchmarkBoundSession(b *testing.B) {
	g, err := Open(testOptions(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	r := Route{ID: "rt", Key: "test", AgentType: "generic", Expose: []string{"proxy"}, Enabled: true, Endpoints: []Endpoint{{ID: "ep", Interfaces: map[string]EndpointInterface{"proxy": {URL: "http://localhost:1"}}, State: "active", Weight: 100}}}
	defaults(&r)
	g.current.Store(build(Config{Schema: 2, Routes: []Route{r}}))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := g.acquire("alice", "test", "session", "", "proxy", nil)
			if err != nil {
				b.Error(err)
				return
			}
			l.release()
		}
	})
}

func TestCallerIsolationAndLimits(t *testing.T) {
	g := setup(t)
	g.opts.MaxPerCaller = 1
	create(t, g, "proxy", "http://localhost:1", "http://localhost:2")
	a, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := g.acquire("alice", "test", "other", "", "proxy", nil); err == nil {
		extra.release()
		t.Fatal("caller concurrency limit ignored")
	}
	b, err := g.acquire("bob", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.ep.ID == b.ep.ID {
		t.Fatal("owners shared session binding")
	}
	a.release()
	b.release()
	if g.inflight.Load() != 0 || g.callerCounts["alice"].Load() != 0 {
		t.Fatal("concurrency slot leaked")
	}
}
func TestMergePatchClearsOptionalField(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	path := "/admin/v1/routes/" + rt.ID
	w := admin(t, g, "PATCH", path, `{"allowed_methods":["POST"]}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = admin(t, g, "PATCH", path, `{"allowed_methods":null}`, `"2"`, "")
	if w.Code != 200 || len(g.current.Load().Routes["test"].AllowedMethods) != 0 {
		t.Fatal("merge patch null did not remove field")
	}
}
func TestRestartDropsTasks(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	create(t, g, "a2a", "http://localhost:1")
	g.mapResult(map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}}, "alice", g.current.Load().Routes["test"].ID, "ep", "s")
	var id string
	for _, res := range g.resources.list("alice", g.current.Load().Routes["test"].ID, a2aTask, "") {
		id = res.ID
	}
	g.Close()
	g, err = Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	v := rpcCall(t, g, "alice", fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":%q}}`, id), "")
	if v["error"] == nil {
		t.Fatal("task survived restart")
	}
}
