package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"aiagent-gateway/internal/gateway"
)

const namedRouteInput = `{"consumers":[{"id":"alice","token":"alice-secret"},{"id":"bob","token":"bob-secret"}],"key":"test","agent_type":"generic","expose":["proxy"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":"http://localhost:8001"}}},{"name":"two","interfaces":{"proxy":{"url":"http://localhost:8002"}}}]}`

func TestRouteUpsertByKeyAndReceiptRecovery(t *testing.T) {
	opts := testOptions(t.TempDir())
	g, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	created := admin(t, g, "POST", "/admin/v1/routes", namedRouteInput, "", "create")
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	var original gateway.Route
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
	rt := readRoute(t, g, "test")
	if rt.ID != original.ID || rt.Enabled || rt.Timeout.ResponseHeaderMS != 180000 || rt.Timeout.StreamIdleMS != original.Timeout.StreamIdleMS || !reflect.DeepEqual(rt.Endpoints, original.Endpoints) {
		t.Fatal("partial update lost configuration or identity", rt)
	}
	if len(readRoutes(t, g)) != 1 {
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
	g, err = gateway.Open(opts)
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
	if !readRoute(t, g, "test").Enabled {
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
	if readRoute(t, g, "test") == nil {
		t.Fatal("deleted ID alias target instead of key")
	}
}

func TestRouteUpsertAddsA2AWithoutMovingChat(t *testing.T) {
	proxy := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "chat response")
	}))
	defer proxy.Close()
	rpcUp := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q rpcMessage
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
	original := readRoute(t, g, "test")
	updated := fmt.Sprintf(`{"key":"test","expose":["proxy","a2a"],"endpoints":[{"name":"agent","interfaces":{"proxy":{"url":%q},"a2a":{"url":%q,"protocol_version":"1.0"}}}]}`, proxy.URL, rpcUp.URL)
	if w := admin(t, g, "POST", "/admin/v1/routes", updated, "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if readRoute(t, g, "test").Endpoints[0].ID != original.Endpoints[0].ID {
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
	if created != 1 || len(readRoutes(t, g)) != 1 || readRoute(t, g, "test").Version != 16 {
		t.Fatal("concurrent upserts duplicated route or lost updates")
	}
}

func TestNewModelValidation(t *testing.T) {
	cases := []struct {
		body   string
		status int
	}{
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 201},
		{`{"key":"test","agent_type":"hermes","expose":["proxy","a2a"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"},"a2a":{"url":"http://localhost:2/rpc","protocol_version":"1.0"}}}]}`, 201},
		{`{"key":"test","expose":[]}`, 400},
		{`{"key":"test","agent_type":"other","expose":["proxy"]}`, 400},
		{`{"key":"test","expose":["proxy","proxy"]}`, 400},
		{`{"key":"test","expose":["proxy","a2a"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 400},
		{`{"key":"test","protocol":"proxy","expose":["proxy"]}`, 409},
		{`{"key":"test","protocol":"proxy","endpoints":[{"interfaces":{}}]}`, 409},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"url":"http://localhost:1"}]}`, 409},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","protocol_version":"1.0"}}}]}`, 400},
		{`{"key":"test","expose":["a2a"],"endpoints":[{"interfaces":{"a2a":{"url":"http://localhost:1","protocol_version":"1.0.1"}}}]}`, 400},
		{`{"key":"test","expose":["a2a"],"endpoints":[{"interfaces":{"a2a":{"url":"http://localhost:1","protocol_version":"1.0"}}},{"interfaces":{"a2a":{"url":"http://localhost:2","protocol_version":"0.3.0"}}}]}`, 400},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","extra":true}}}]}`, 400},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			g := setup(t)
			w := admin(t, g, "POST", "/admin/v1/routes", tc.body, "", "")
			if w.Code != tc.status {
				t.Fatalf("want %d got %d: %s", tc.status, w.Code, w.Body.String())
			}
			if tc.status == 201 && (bytes.Contains(w.Body.Bytes(), []byte(`"protocol":`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"agent_type":`))) {
				t.Fatal("legacy response shape", w.Body.String())
			}
		})
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
	if w.Code != 200 || len(readRoute(t, g, "test").AllowedMethods) != 0 {
		t.Fatal("merge patch null did not remove field")
	}
}
