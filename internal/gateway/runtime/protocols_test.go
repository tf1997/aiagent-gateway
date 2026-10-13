package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

func TestA2AV1ListRejectsExpiredCursor(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q a2a.RPC
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		writeTestTask(w, q, map[string]any{"id": q.Params["id"], "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
	}))
	defer up.Close()
	g := setup(t)
	var offset atomic.Int64
	g.taskLists = shared.NewListCache(func() time.Time { return time.Now().Add(time.Duration(offset.Load())) })
	rt := create(t, g, "a2a", up.URL)
	seedA2ATask(t, g, rt, "alice", rt.Endpoints[0].ID, "one", "ctx")
	seedA2ATask(t, g, rt, "alice", rt.Endpoints[0].ID, "two", "ctx")
	w := dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 1}), "alice", "")
	var page struct {
		Result struct {
			Next string `json:"nextPageToken"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || page.Result.Next == "" {
		t.Fatal("initial page missing cursor", w.Body.String(), err)
	}
	offset.Store(int64(24 * time.Hour))
	w = dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 1, "pageToken": page.Result.Next}), "alice", "")
	requireRPCCode(t, w, -32602)
	if g.inflight.Load() != 0 {
		t.Fatal("expired cursor leaked a lease")
	}
}

func seedA2ATask(t *testing.T, g *Gateway, rt protocol.Route, owner, ep, up, ctx string) map[string]any {
	t.Helper()
	task := map[string]any{"id": up, "contextId": ctx, "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.a2aHandler.MapResult(task, owner, rt.ID, ep, "s"); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestA2AV1ListCapacityIsBounded(t *testing.T) {
	// Exceed the documented limit before any upstream query is attempted.
	const ownedTaskLimit = 1000
	g := setup(t)
	rt := create(t, g, "a2a", "http://localhost:1")
	for i := 0; i <= ownedTaskLimit; i++ {
		seedA2ATask(t, g, rt, "alice", rt.Endpoints[0].ID, fmt.Sprintf("up-%d", i), "ctx")
	}
	w := dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{}), "alice", "")
	requireRPCCode(t, w, -32603)
	if !strings.Contains(w.Body.String(), "Too many owned tasks") {
		t.Fatal("limit did not fire before upstream I/O", w.Body.String())
	}
	var releases []func()
	for i := 0; i < 4; i++ {
		release, err := g.acquireList("tasks")
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	w = dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{}), "alice", "")
	requireRPCCode(t, w, -32603)
	if release, err := g.acquireList("tasks"); err == nil {
		release()
		t.Fatal("rejected query released another request's slot")
	}
	for _, release := range releases {
		release()
	}
	for i := 0; i < 4; i++ {
		release, err := g.acquireList("tasks")
		if err != nil {
			t.Fatal("active count leaked", err)
		}
		defer release()
	}
}
