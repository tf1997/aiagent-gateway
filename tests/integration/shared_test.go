package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/protocol"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

type sharedFixture struct {
	core   *gateway.Gateway
	server *gateway.SharedServer
	socket string
	j      *state.Journal
}

func sharedSetup(t *testing.T) *sharedFixture {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gateway-state-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	opts := testOptions(filepath.Join(dir, "data"))
	opts.DeferBackground = true
	g, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	j, err := state.Open(filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	shared, err := gateway.NewSharedServer(g, j)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "state.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: shared.Handler()}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close(); g.Close(); j.Close() })
	return &sharedFixture{g, shared, socket, j}
}

func (f *sharedFixture) worker(t *testing.T, id string) *gateway.Gateway {
	t.Helper()
	opts := testOptions("")
	opts.StateSocket = f.socket
	opts.WorkerID = id
	g, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestWorkerEntryProxyVerdictsAndCutover(t *testing.T) {
	f := sharedSetup(t)
	calls := 0
	var mu sync.Mutex
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		switch r.URL.Path {
		case "/error":
			w.WriteHeader(502)
		case "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"text\":\"hello\"}\n\ndata: [DONE]\n\n")
		case "/broken":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"text\":\"hello\"}\n\n")
		case "/rpc-error":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"failed"}}`)
		default:
			io.WriteString(w, "ok")
		}
	}))
	defer up.Close()
	create(t, f.core, "proxy", up.URL)
	a, b := f.worker(t, "a"), f.worker(t, "b")
	sa := httptest.NewServer(a.WorkerHandler(p.Identity{Incarnation: "a", Release: "old"}))
	defer sa.Close()
	sb := httptest.NewServer(b.WorkerHandler(p.Identity{Incarnation: "b", Release: "new"}))
	defer sb.Close()
	j, _ := state.Open(t.TempDir())
	defer j.Close()
	front, err := entry.Open(j, "admin-secret", func(ctx context.Context, q p.Request) (p.Resolution, error) {
		return gateway.ResolveShared(f.socket, "admin-secret", q)
	})
	if err != nil {
		t.Fatal(err)
	}
	old := p.Target{Instance: "a", Release: "old", Incarnation: "a", URL: sa.URL}
	candidate := p.Target{Instance: "b", Release: "new", Incarnation: "b", URL: sb.URL}
	if err = front.Apply(0, p.Traffic{Revision: 1, Rollout: "test", Stable: old, Candidate: &candidate, Percent: 100}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(front)
	defer server.Close()
	started := time.Now().Add(-time.Second)
	for _, path := range []string{"ok", "sse", "broken", "rpc-error", "error"} {
		req, _ := http.NewRequest("POST", server.URL+"/agents/test/"+path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer alice-secret")
		req.Header.Set("X-Session-ID", "one")
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	stats := front.Stats(started, "b")
	if stats.Counts["http"].Success != 1 || stats.Counts["sse"].Success != 1 || stats.Failures != 3 {
		raw, _ := json.Marshal(stats)
		t.Fatalf("verdicts: %s", raw)
	}
	mu.Lock()
	if calls != 5 {
		t.Fatal("request replayed", calls)
	}
	mu.Unlock()
	if err = front.Apply(1, p.Traffic{Revision: 2, Rollout: "test", Stable: old, Candidate: &candidate, Percent: 0}); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", server.URL+"/agents/test/ok", nil)
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "one")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(raw) != "ok" {
		t.Fatal("rollback request", res.StatusCode, string(raw))
	}
	if front.Stats(started, "a").Counts["http"].Success != 1 {
		t.Fatal("rollback did not reach old worker")
	}
}

func TestWorkerRejectsUntrustedEntry(t *testing.T) {
	f := sharedSetup(t)
	g := f.worker(t, "a")
	w := httptest.NewRecorder()
	g.WorkerHandler(p.Identity{}).ServeHTTP(w, httptest.NewRequest("GET", "/agents/test/x", nil))
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}

func TestSharedHermesHTTPAcrossWorkersWithoutCapabilities(t *testing.T) {
	f := sharedSetup(t)
	fake := newHermesFake()
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/capabilities" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer up.Close()
	rt := createHermesRoute(t, f.core, up.URL)
	a, b := f.worker(t, "a"), f.worker(t, "b")
	id := hermesCreated(t, a, "alice", "same")
	ref := readResource(t, f.core, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesSession, ID: id})
	if w := hermesCall(b, "GET", "/api/sessions/"+id, "", "alice"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(b, "POST", "/api/sessions/"+id+"/chat/stream", `{"input":"hello"}`, "alice"); w.Code != 200 || !strings.Contains(w.Body.String(), "done") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(b, "GET", "/api/sessions/"+id, "", "bob"); w.Code != 404 {
		t.Fatal("cross-caller leak")
	}
	next := readResource(t, f.core, ref.Key())
	if ref.EndpointID != next.EndpointID || ref.Upstream != next.Upstream {
		t.Fatal("upstream ownership changed")
	}
}

func TestSharedA2AAndCursorAcrossWorkers(t *testing.T) {
	var calls atomic.Int64
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var q rpcMessage
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		id, _ := q.Params["id"].(string)
		if q.Method == "SendMessage" {
			id = requestedTask(q)
		}
		writeTestTask(w, q, map[string]any{"id": id, "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}})
	}))
	defer up.Close()
	f := sharedSetup(t)
	rt := create(t, f.core, "a2a", up.URL)
	a, b := f.worker(t, "a"), f.worker(t, "b")
	one := sendA2ATask(t, a, "alice", "one")
	sendA2ATask(t, a, "alice", "two")
	got := rpcTask(t, dataRequest(b, "POST", "/agents/test/a2a", rpcBody("GetTask", map[string]any{"id": one["id"]}), "alice", ""), false)
	if got["id"] != one["id"] || got["contextId"] != one["contextId"] {
		t.Fatal("task identity changed across workers", got)
	}
	ref := readResource(t, f.core, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.A2ATask, ID: one["id"].(string)})
	if ref.Upstream != "one" || ref.EndpointID != rt.Endpoints[0].ID || ref.ParentID != one["contextId"] {
		t.Fatal("task ownership or lineage changed", ref)
	}
	first := dataRequest(a, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{"pageSize": 1}), "alice", "")
	var page struct {
		Result struct {
			Tasks []map[string]any `json:"tasks"`
			Next  string           `json:"nextPageToken"`
			Total int              `json:"totalSize"`
		} `json:"result"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || first.Code != 200 || len(page.Result.Tasks) != 1 || page.Result.Next == "" || page.Result.Total != 2 {
		t.Fatal("missing first page", first.Body.String(), err)
	}
	firstID, token, count := page.Result.Tasks[0]["id"], page.Result.Next, calls.Load()
	query := rpcBody("ListTasks", map[string]any{"pageSize": 1, "pageToken": token})
	for i := 0; i < 2; i++ {
		last := dataRequest(b, "POST", "/agents/test/a2a", query, "alice", "")
		if err := json.Unmarshal(last.Body.Bytes(), &page); err != nil || last.Code != 200 || len(page.Result.Tasks) != 1 || page.Result.Next != "" || page.Result.Tasks[0]["id"] == firstID || page.Result.Total != 2 {
			t.Fatal("cursor lost during switch", last.Body.String(), err)
		}
	}
	if calls.Load() != count {
		t.Fatal("second worker repeated snapshot queries")
	}
	requireRPCCode(t, dataRequest(b, "POST", "/agents/test/a2a", query, "bob", ""), -32602)
}

func TestEntryKeepsInflightStreamDuringSwitch(t *testing.T) {
	f := sharedSetup(t)
	finish := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			io.WriteString(w, "ok")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"start\":true}\n\n")
		w.(http.Flusher).Flush()
		<-finish
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer up.Close()
	create(t, f.core, "proxy", up.URL)
	a, b := f.worker(t, "a"), f.worker(t, "b")
	sa := httptest.NewServer(a.WorkerHandler(p.Identity{Incarnation: "a"}))
	defer sa.Close()
	sb := httptest.NewServer(b.WorkerHandler(p.Identity{Incarnation: "b"}))
	defer sb.Close()
	j, _ := state.Open(t.TempDir())
	defer j.Close()
	front, _ := entry.Open(j, "admin-secret", func(ctx context.Context, q p.Request) (p.Resolution, error) {
		return gateway.ResolveShared(f.socket, "admin-secret", q)
	})
	old := p.Target{Instance: "a", Incarnation: "a", Release: "old", URL: sa.URL}
	next := p.Target{Instance: "b", Incarnation: "b", Release: "new", URL: sb.URL}
	if err := front.Apply(0, p.Traffic{Revision: 1, Rollout: "stream", Stable: old}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(front)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/agents/test/stream", nil)
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "stream")
	res, err := srv.Client().Do(req)
	if err != nil {
		close(finish)
		t.Fatal(err)
	}
	if err = front.Apply(1, p.Traffic{Revision: 2, Rollout: "stream", Stable: old, Candidate: &next, Percent: 100}); err != nil {
		close(finish)
		t.Fatal(err)
	}
	if front.Stats(time.Time{}, "a").Active != 1 {
		close(finish)
		t.Fatal("old stream not tracked")
	}
	close(finish)
	raw, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || !strings.Contains(string(raw), "[DONE]") {
		t.Fatal("stream interrupted", err, string(raw))
	}
	for k := range res.Trailer {
		if strings.HasPrefix(strings.ToLower(k), "x-gateway-rollout-") {
			t.Fatal("internal result trailer leaked", k)
		}
	}
}
