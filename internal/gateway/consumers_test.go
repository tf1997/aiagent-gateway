package gateway

import (
	"aiagent-gateway/internal/releaseprotocol"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func consumerCall(g *Gateway, path, token, session string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Session-ID", session)
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}

func TestDynamicConsumersRotationRevocationAndRecovery(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, r.Header.Get("X-Gateway-Caller-ID"))
	}))
	defer up.Close()
	o := testOptions(t.TempDir())
	g, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	body := fmt.Sprintf(`{"key":"dynamic","expose":["proxy"],"consumers":[{"id":"app","token":"first-consumer-secret"}],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":%q}}}]}`, up.URL)
	created := admin(t, g, "POST", "/admin/v1/routes", body, "", "create")
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	before := g.current.Load().Routes["dynamic"]
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
	if rt := g.current.Load().Routes["dynamic"]; rt.ID != before.ID || rt.Endpoints[0].ID != before.Endpoints[0].ID || g.bindings.Load() != 1 {
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
	g, err = Open(o)
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

func TestConsumersValidationAtomicityAndRouteScope(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	for _, input := range []string{
		`null`, `{}`, `[{}]`, `[{"id":"new"}]`, `[{"id":"new","token":""}]`,
		`[{"id":"alice","token":null}]`,
		`[{"id":"new","token":"admin-secret"}]`, `[{"id":"invalid/id","token":"secret"}]`,
		`[{"id":"new","token_hash":"sensitive-value"}]`, `[{"id":"new","token":{}}]`,
		`[{"id":"new","token":"has\nnewline"}]`,
		`[{"id":"same","token":"one"},{"id":"same","token":"two"}]`,
		`[{"id":"one","token":"same"},{"id":"two","token":"same"}]`,
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":`+input+`}`, "", "")
		if w.Code != 400 || g.current.Load() != before || strings.Contains(w.Body.String(), "sensitive-value") {
			t.Fatal(input, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"other","expose":["proxy"]}`, "", ""); w.Code != 201 {
		t.Fatal(w.Code)
	}
	if w := consumerCall(g, "/agents/other/chat", "alice-secret", "session"); w.Code != 401 {
		t.Fatal("token granted access to another route", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"other","consumers":[{"id":"different-owner","token":"alice-secret"}]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if caller := authenticateConsumer(g.current.Load().Routes["other"], "Bearer alice-secret"); caller == nil || caller.ID != "different-owner" {
		t.Fatal("consumer identity not route scoped")
	}
	before = g.current.Load()
	g.persist = func(Config) (bool, error) { return false, fmt.Errorf("disk full") }
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[]}`, "", "failed-revoke"); w.Code != 503 || g.current.Load() != before {
		t.Fatal("failed persistence changed access")
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
	if r := f.core.resolveRollout(q); r.Status != 0 {
		t.Fatal(r)
	}
	w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test","consumers":[{"id":"alice","token":"new-secret"}]}`, "", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if r := f.core.resolveRollout(q); r.Status != 401 {
		t.Fatal("entry retained revoked token", r)
	}
	if w := request("alice-secret"); w.Code != 401 {
		t.Fatal("worker retained revoked token", w.Code)
	}
	if w := request("new-secret"); w.Code != 200 {
		t.Fatal("worker did not refresh credentials", w.Code, w.Body.String())
	}
	if worker.current.Load().Routes["test"].ID != rt.ID {
		t.Fatal("shared update changed route")
	}
}

func TestReconcileContinuesAfterConsumerRevocation(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpc
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}})
	}))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL, ProtocolVersion: "1.0"}})
	value := map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.mapResult(value, "alice", rt.ID, rt.Endpoints[0].ID, ""); err != nil {
		t.Fatal(err)
	}
	id := value["id"].(string)
	ref, err := g.resources.lookup(resourceKey{"alice", rt.ID, a2aTask, id})
	if err != nil {
		t.Fatal(err)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	g.reconcileResource(context.Background(), ref)
	got, err := g.resources.lookup(ref.key())
	if err != nil || got.State != resourceTerminal {
		t.Fatal("revocation stranded background execution", got.State, err)
	}
	if _, _, err := g.acquireResource("alice", g.current.Load().Routes["test"], a2aTask, id); err == nil {
		t.Fatal("revoked caller accessed resource")
	}
}

func TestDynamicConsumerCountersConcurrentRotation(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				l, err := g.acquire("alice", "test", "", "", "proxy", nil)
				if err == nil {
					l.release()
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		w := admin(t, g, "POST", "/admin/v1/routes", fmt.Sprintf(`{"key":"test","consumers":[{"id":"alice","token":"token-%d"}]}`, i), "", "")
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	wg.Wait()
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	if len(g.callerCounts) != 0 || g.inflight.Load() != 0 {
		t.Fatal("consumer counters leaked")
	}
}

func TestConsumerRotationDoesNotResetInflightLimit(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	g.opts.MaxPerCaller = 1
	l, err := g.acquire("alice", "test", "", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.release()
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[{"id":"alice","token":"rotated"}]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if next, err := g.acquire("alice", "test", "", "", "proxy", nil); err == nil {
		next.release()
		t.Fatal("rotation reset inflight capacity")
	}
	l.release()
	next, err := g.acquire("alice", "test", "", "", "proxy", nil)
	if err != nil {
		t.Fatal("capacity not released", err)
	}
	next.release()
}

func TestDynamicConsumerPatchAndInputLimit(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	body := `{"consumers":[{"id":"alice","token":"patch-token"}]}`
	w := admin(t, g, "PATCH", "/admin/v1/routes/test", body, tag(rt.Version), "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "patch-token") || authenticateConsumer(g.current.Load().Routes["test"], "Bearer patch-token") == nil {
		t.Fatal(w.Code, w.Body.String())
	}
	inputs := make([]map[string]string, maxRouteConsumers+1)
	for i := range inputs {
		inputs[i] = map[string]string{"id": fmt.Sprintf("app-%d", i), "token": fmt.Sprintf("token-%d", i)}
	}
	raw, _ := json.Marshal(map[string]any{"key": "test", "consumers": inputs})
	if w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", ""); w.Code != 400 {
		t.Fatal("consumer limit not enforced", w.Code)
	}
}
