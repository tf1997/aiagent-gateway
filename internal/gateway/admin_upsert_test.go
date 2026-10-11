package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const namedRouteInput = `{"consumers":[{"id":"alice","token":"alice-secret"},{"id":"bob","token":"bob-secret"}],"key":"test","agent_type":"generic","expose":["proxy"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":"http://localhost:8001"}}},{"name":"two","interfaces":{"proxy":{"url":"http://localhost:8002"}}}]}`

func TestRouteUpsertByKeyAndReceiptRecovery(t *testing.T) {
	opts := testOptions(t.TempDir())
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	created := admin(t, g, "POST", "/admin/v1/routes", namedRouteInput, "", "create")
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	var original Route
	if err := json.Unmarshal(created.Body.Bytes(), &original); err != nil {
		t.Fatal(err)
	}
	for _, selector := range []string{"test", original.ID} {
		w := admin(t, g, "GET", "/admin/v1/routes/"+selector, "", "", "")
		if w.Code != 200 || w.Body.String() != created.Body.String() {
			t.Fatal(selector, w.Code, w.Body.String())
		}
	}
	body := `{"key":"test","enabled":false,"timeout":{"response_header_ms":180000}}`
	updated := admin(t, g, "POST", "/admin/v1/routes", body, "", "update")
	if updated.Code != 200 || updated.Header().Get("ETag") != `"2"` {
		t.Fatal(updated.Code, updated.Body.String())
	}
	rt := g.current.Load().Routes["test"]
	if rt.ID != original.ID || rt.Enabled || rt.Timeout.ResponseHeaderMS != 180000 || rt.Timeout.StreamIdleMS != original.Timeout.StreamIdleMS || !reflect.DeepEqual(rt.Endpoints, original.Endpoints) {
		t.Fatal("partial update lost configuration or identity", rt)
	}
	if len(g.current.Load().Config.Routes) != 1 {
		t.Fatal("upsert duplicated route")
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", body, `"1"`, ""); w.Code != 412 {
		t.Fatal("stale ETag accepted", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", body+" ", "", "update"); w.Code != 409 {
		t.Fatal("idempotency conflict accepted", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","enabled":true}`, `"2"`, ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	g.Close()
	g, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body, key string
		code      int
		response  string
	}{
		{namedRouteInput, "create", 201, created.Body.String()},
		{body, "update", 200, updated.Body.String()},
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", tc.body, "", tc.key)
		if w.Code != tc.code || w.Body.String() != tc.response {
			t.Fatal("replay changed status or body", w.Code, w.Body.String())
		}
	}
	if !g.current.Load().Routes["test"].Enabled {
		t.Fatal("replay applied an old update")
	}
	if w := admin(t, g, "DELETE", "/admin/v1/routes/test", "", `"1"`, ""); w.Code != 412 {
		t.Fatal("delete ignored ETag", w.Code)
	}
	if w := admin(t, g, "DELETE", "/admin/v1/routes/test", "", "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := admin(t, g, "GET", "/admin/v1/routes/test", "", "", ""); w.Code != 404 {
		t.Fatal("deleted key still resolves")
	}
}

func TestRouteUpsertReconcilesEndpointsAndProtectsBindings(t *testing.T) {
	g := setup(t)
	if w := admin(t, g, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	original := g.current.Load().Routes["test"]
	l, err := g.acquire("alice", "test", "sticky", original.Endpoints[0].ID, "proxy", original)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
	input := `{"key":"test","endpoints":[{"name":"two","interfaces":{"proxy":{"url":"http://localhost:8002"}}},{"name":"one","weight":200,"state":"draining","interfaces":{"proxy":{"url":"http://localhost:8001"}}},{"name":"three","interfaces":{"proxy":{"url":"http://localhost:8003"}}}]}`
	w := admin(t, g, "POST", "/admin/v1/routes", input, "", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	rt := g.current.Load().Routes["test"]
	if rt.Endpoints[0].ID != original.Endpoints[1].ID || rt.Endpoints[0].Version != 1 || rt.Endpoints[1].ID != original.Endpoints[0].ID || rt.Endpoints[1].Version != 2 || rt.Endpoints[2].ID == "" {
		t.Fatal("endpoint identity/version changed", rt.Endpoints)
	}
	if original.Endpoints[0].Weight != 100 || original.Endpoints[0].State != "active" {
		t.Fatal("old snapshot mutated")
	}
	l, err = g.acquire("alice", "test", "sticky", "", "proxy", rt)
	if err != nil {
		t.Fatal(err)
	}
	if l.ep.ID != original.Endpoints[0].ID {
		t.Fatal("binding moved")
	}
	l.release()
	before := g.current.Load()
	for _, body := range []string{
		strings.Replace(input, "localhost:8001", "localhost:9001", 1),
		`{"key":"test","endpoints":[]}`,
		`{"key":"test","agent_type":"hermes"}`,
		`{"key":"test","expose":["a2a"]}`,
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", body, "", "")
		if w.Code != 409 || g.current.Load() != before {
			t.Fatal("referenced update was not atomic", body, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "DELETE", "/admin/v1/routes/test", "", "", ""); w.Code != 409 {
		t.Fatal("deleted referenced route", w.Code)
	}
	w = admin(t, g, "GET", "/admin/v1/routes/test/endpoints/"+rt.Endpoints[1].ID, "", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "resource_references") {
		t.Fatal("key endpoint lookup failed", w.Code)
	}
}

func TestRouteUpsertValidationAndAtomicPersistence(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"enabled":false}`, 400},
		{`{"key":"test","id":""}`, 400},
		{`{"key":"test","version":0}`, 400},
		{`{"key":"test","unknown":true}`, 400},
		{`{"key":"test","endpoints":null}`, 400},
		{`{"key":"test","endpoints":[{"id":"invented","interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 400},
		{`{"key":"test","endpoints":[{"name":"same","interfaces":{"proxy":{"url":"http://localhost:1"}}},{"name":"same","interfaces":{"proxy":{"url":"http://localhost:2"}}}]}`, 409},
		{`{"key":"test","protocol":"proxy","expose":["proxy"]}`, 409},
		{`{"key":"test","timeout":{"response_header_ms":-1}}`, 400},
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", tc.body, "", "")
		if w.Code != tc.code || g.current.Load() != before {
			t.Fatal(tc.body, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"missing","expose":["proxy"]}`, `"1"`, ""); w.Code != 412 {
		t.Fatal("created with stale precondition", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","protocol":"proxy","endpoints":[{"url":"http://localhost:1"}]}`, "", ""); w.Code != 200 {
		t.Fatal("legacy input failed", w.Code, w.Body.String())
	}
	if g.current.Load().Routes["test"].Endpoints[0].ID != before.Routes["test"].Endpoints[0].ID {
		t.Fatal("legacy endpoint identity lost")
	}
	before = g.current.Load()
	g.configFrozen.Store(true)
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","enabled":false}`, "", ""); w.Code != 409 {
		t.Fatal("upsert bypassed rollout freeze")
	}
	g.configFrozen.Store(false)
	g.persist = func(Config) (bool, error) { return false, fmt.Errorf("disk full") }
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","enabled":false}`, "", "update"); w.Code != 503 || g.current.Load() != before {
		t.Fatal("failed persistence published update", w.Code)
	}
}

func TestRouteKeyPrecedesInternalID(t *testing.T) {
	g := setup(t)
	first := create(t, g, "proxy", "http://localhost:1")
	body := fmt.Sprintf(`{"key":%q,"expose":["proxy"]}`, first.ID)
	if w := admin(t, g, "POST", "/admin/v1/routes", body, "", ""); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := admin(t, g, "GET", "/admin/v1/routes/"+first.ID, "", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"key":"`+first.ID+`"`) {
		t.Fatal("internal ID shadowed key", w.Code, w.Body.String())
	}
	if w := admin(t, g, "DELETE", "/admin/v1/routes/"+first.ID, "", "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if g.current.Load().Routes["test"] == nil {
		t.Fatal("deleted ID alias target instead of key")
	}
}

func TestRouteUpsertAddsA2AWithoutMovingChat(t *testing.T) {
	proxy := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "chat response")
	}))
	defer proxy.Close()
	rpcUp := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpc
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}})
	}))
	defer rpcUp.Close()
	g := setup(t)
	initial := fmt.Sprintf(`{"consumers":[{"id":"alice","token":"alice-secret"}],"key":"test","expose":["proxy"],"endpoints":[{"name":"agent","interfaces":{"proxy":{"url":%q}}}]}`, proxy.URL)
	if w := admin(t, g, "POST", "/admin/v1/routes", initial, "", ""); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := dataRequest(g, "POST", "/agents/test/chat", `{}`, "alice", "conversation"); w.Code != 200 || w.Body.String() != "chat response" {
		t.Fatal(w.Code, w.Body.String())
	}
	original := g.current.Load().Routes["test"]
	updated := fmt.Sprintf(`{"key":"test","expose":["proxy","a2a"],"endpoints":[{"name":"agent","interfaces":{"proxy":{"url":%q},"a2a":{"url":%q,"protocol_version":"1.0"}}}]}`, proxy.URL, rpcUp.URL)
	if w := admin(t, g, "POST", "/admin/v1/routes", updated, "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if g.current.Load().Routes["test"].Endpoints[0].ID != original.Endpoints[0].ID {
		t.Fatal("adding an interface changed endpoint identity")
	}
	if w := dataRequest(g, "POST", "/agents/test/chat", `{}`, "alice", "conversation"); w.Code != 200 || w.Body.String() != "chat response" {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"id":"task_`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", initial, "", ""); w.Code != 409 {
		t.Fatal("removed interface with live task", w.Code, w.Body.String())
	}
}

func TestConcurrentRouteUpsertsCreateOnce(t *testing.T) {
	g := setup(t)
	var wg sync.WaitGroup
	codes := make(chan int, 16)
	for i := 0; i < cap(codes); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- admin(t, g, "POST", "/admin/v1/routes", namedRouteInput, "", "").Code
		}()
	}
	wg.Wait()
	close(codes)
	created := 0
	for code := range codes {
		if code == http.StatusCreated {
			created++
		} else if code != http.StatusOK {
			t.Fatal(code)
		}
	}
	if created != 1 || len(g.current.Load().Config.Routes) != 1 || g.current.Load().Routes["test"].Version != 16 {
		t.Fatal("concurrent upserts duplicated route or lost updates")
	}
}

func TestRouteUpsertProtectsSharedResourceOwnership(t *testing.T) {
	f := sharedSetup(t)
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal(w.Code)
	}
	worker := f.worker(t, "upsert-worker")
	rt := worker.current.Load().Routes["test"]
	reserved, err := worker.reserveResource("alice", rt, resourceCreation{Kind: hermesSession, Alias: "conversation", Operation: "create", Fingerprint: "body"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.resources.update(reserved.Resource, resourceUnknown); err != nil {
		t.Fatal(err)
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test","endpoints":[]}`, "", ""); w.Code != 409 {
		t.Fatal("removed endpoint with shared resource", w.Code, w.Body.String())
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	got, err := worker.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, reserved.Resource.ID})
	if err != nil || got.EndpointID != reserved.Resource.EndpointID {
		t.Fatal("resource ownership changed", got, err)
	}
}
