package gateway

import (
	"aiagent-gateway/internal/entry"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
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
	"testing"
	"time"
)

type sharedFixture struct {
	core   *Gateway
	server *SharedServer
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
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	j, err := state.Open(filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	shared, err := NewSharedServer(g, j)
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
func (f *sharedFixture) worker(t *testing.T, id string) *Gateway {
	t.Helper()
	opts := testOptions("")
	opts.StateSocket = f.socket
	opts.WorkerID = id
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}
func TestSharedBindingsAndGlobalCapacity(t *testing.T) {
	f := sharedSetup(t)
	create(t, f.core, "proxy", "http://127.0.0.1:8001", "http://127.0.0.1:8002")
	a, b := f.worker(t, "a"), f.worker(t, "b")
	f.core.opts.MaxInflight = 1
	l, err := a.acquire("alice", "test", "session", "", "proxy", a.current.Load().Routes["test"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.acquire("alice", "test", "other", "", "proxy", b.current.Load().Routes["test"]); err == nil {
		t.Fatal("global capacity doubled")
	}
	ep := l.ep.ID
	l.release()
	next, err := b.acquire("alice", "test", "session", "", "proxy", b.current.Load().Routes["test"])
	if err != nil {
		t.Fatal(err)
	}
	defer next.release()
	if next.ep.ID != ep {
		t.Fatal("session moved upstream")
	}
	if f.core.bindings.Load() != 1 {
		t.Fatal("duplicate bindings")
	}
}
func TestSharedResourceReservationRecovery(t *testing.T) {
	f := sharedSetup(t)
	rt := create(t, f.core, "proxy", "http://127.0.0.1:8001")
	a, b := f.worker(t, "a"), f.worker(t, "b")
	spec := resourceCreation{Kind: hermesSession, Alias: "client-session", Operation: "create", Fingerprint: "fingerprint", IdempotencyKey: "once"}
	first, err := a.reserveResource("alice", a.current.Load().Routes[rt.Key], spec)
	if err != nil {
		t.Fatal(err)
	}
	again, err := b.reserveResource("alice", b.current.Load().Routes[rt.Key], spec)
	if err != nil || again.First || again.Resource.ID != first.Resource.ID {
		t.Fatalf("idempotency diverged: %+v %v", again, err)
	}
	if err = b.resources.update(first.Resource, resourceUnknown); err != nil {
		t.Fatal(err)
	}
	if err = a.resources.abandon(first.Resource); err != errResourceInUse {
		t.Fatal("unknown operation abandoned", err)
	}
	if _, err = b.resources.lookup(resourceKey{"bob", rt.ID, hermesSession, first.Resource.ID}); err != errResourceNotFound {
		t.Fatal("caller isolation", err)
	}
	// Restore metadata into a fresh gateway without replaying any external operation.
	restoredOpts := testOptions(t.TempDir())
	restoredOpts.DeferBackground = true
	g, err := Open(restoredOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.current.Store(build(clone(f.core.current.Load().Config)))
	if err = g.EnableDurability(f.j); err != nil {
		t.Fatal(err)
	}
	got, err := g.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, first.Resource.ID})
	if err != nil || got.State != resourceUnknown || got.EndpointID != first.Resource.EndpointID {
		t.Fatalf("restore: %+v %v", got, err)
	}
	replay, err := g.reserveResource("alice", g.current.Load().Routes[rt.Key], spec)
	if err != nil || replay.First {
		t.Fatal("recovery recreated operation", err)
	}
}
func TestSharedConcurrentFirstReservation(t *testing.T) {
	f := sharedSetup(t)
	create(t, f.core, "proxy", "http://127.0.0.1:8001")
	workers := []*Gateway{f.worker(t, "a"), f.worker(t, "b")}
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g := workers[i%2]
			r, err := g.reserveResource("alice", g.current.Load().Routes["test"], resourceCreation{Kind: hermesSession, Alias: "shared", Operation: "create", Fingerprint: "f"})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- r.Resource.ID
		}(i)
	}
	wg.Wait()
	close(ids)
	want := ""
	for id := range ids {
		if want != "" && want != id {
			t.Fatal("concurrent reservation split")
		}
		want = id
	}
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
		return ResolveShared(f.socket, "admin-secret", q)
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

func TestSharedHermesHTTPAcrossWorkers(t *testing.T) {
	f := sharedSetup(t)
	fake := newHermesFake()
	up := newTestServer(fake)
	defer up.Close()
	rt := createHermesRoute(t, f.core, up.URL)
	a, b := f.worker(t, "a"), f.worker(t, "b")
	id := hermesCreated(t, a, "alice", "same")
	if w := hermesCall(b, "GET", "/api/sessions/"+id, "", "alice"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(b, "POST", "/api/sessions/"+id+"/chat/stream", `{"input":"hello"}`, "alice"); w.Code != 200 || !strings.Contains(w.Body.String(), "done") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := hermesCall(b, "GET", "/api/sessions/"+id, "", "bob"); w.Code != 404 {
		t.Fatal("cross-caller leak")
	}
	ref, err := a.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, id})
	if err != nil {
		t.Fatal(err)
	}
	next, err := b.resources.lookup(ref.key())
	if err != nil || ref.EndpointID != next.EndpointID {
		t.Fatal("upstream ownership changed")
	}
}
func TestSharedA2AAndCursorAcrossWorkers(t *testing.T) {
	f := sharedSetup(t)
	rt := create(t, f.core, "a2a", "http://127.0.0.1:8001")
	a, b := f.worker(t, "a"), f.worker(t, "b")
	mapped, err := a.resources.observeBatch("alice", rt.ID, rt.Endpoints[0].ID, []resourceObservation{{Kind: a2aContext, Upstream: "ctx", State: resourceLive}, {Kind: a2aTask, Upstream: "task", ParentUpstream: "ctx", State: resourceTerminal}})
	if err != nil {
		t.Fatal(err)
	}
	task := mapped[resourceReverseKey{"alice", rt.ID, a2aTask, rt.Endpoints[0].ID, "task"}]
	got, err := b.resources.lookup(task.key())
	if err != nil || got.Upstream != "task" || got.ParentID == "" {
		t.Fatal(got, err)
	}
	key, err := a.cachePut("tasks", cacheRecord{Owner: "alice", Route: rt.ID, Revision: 1, Expires: time.Now().Add(time.Minute), Tasks: []json.RawMessage{json.RawMessage(`{"id":"task"}`)}, Size: 1, Bytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, found := b.cacheGet("tasks", key)
	if !found || snapshot.Owner != "alice" || len(snapshot.Tasks) != 1 {
		t.Fatal("cursor lost during switch")
	}
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
		return ResolveShared(f.socket, "admin-secret", q)
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
