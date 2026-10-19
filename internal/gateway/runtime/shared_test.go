package runtime

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

func TestRouteUpsertProtectsSharedResourceOwnership(t *testing.T) {
	f := sharedSetup(t)
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal(w.Code)
	}
	worker := f.worker(t, "upsert-worker")
	rt := worker.current.Load().Routes["test"]
	reserved, err := worker.reserveResource("alice", rt, protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "conversation", Operation: "create", Fingerprint: "body"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.resources.Update(reserved.Resource, protocol.ResourceUnknown); err != nil {
		t.Fatal(err)
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test","endpoints":[]}`, "", ""); w.Code != 409 {
		t.Fatal("removed endpoint with shared resource", w.Code, w.Body.String())
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	got, err := worker.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesSession, ID: reserved.Resource.ID})
	if err != nil || got.EndpointID != reserved.Resource.EndpointID {
		t.Fatal("resource ownership changed", got, err)
	}
}

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

func (f *sharedFixture) localWorker(t *testing.T, id string) *Gateway {
	t.Helper()
	opts := testOptions("")
	opts.StateClient = shared.NewLocalClient(f.server, id)
	opts.WorkerID = id
	g, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestLocalAndRemoteWorkersShareCapacityAffinityAndIdempotency(t *testing.T) {
	f := sharedSetup(t)
	rt := create(t, f.core, "proxy", "http://127.0.0.1:8001", "http://127.0.0.1:8002")
	a, b := f.localWorker(t, "local"), f.worker(t, "remote")
	f.core.opts.MaxInflight = 1
	l, err := a.acquire("alice", "test", "session", "", "proxy", a.current.Load().Routes["test"])
	if err != nil {
		t.Fatal(err)
	}
	if f.server.Count("local") != 1 {
		t.Fatal("local execution bypassed shared lease accounting")
	}
	if _, err = b.acquire("alice", "test", "other", "", "proxy", b.current.Load().Routes["test"]); err == nil {
		t.Fatal("remote execution bypassed global capacity")
	}
	l.release()
	l.release()
	if f.server.Count("local") != 0 || f.core.inflight.Load() != 0 {
		t.Fatal("local lease not released exactly once")
	}
	next, err := b.acquire("alice", "test", "session", "", "proxy", b.current.Load().Routes["test"])
	if err != nil {
		t.Fatal(err)
	}
	if next.ep.ID != l.ep.ID || f.core.bindings.Load() != 1 {
		t.Fatal("local/remote session affinity split")
	}
	next.release()
	spec := protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "client-session", Operation: "create", Fingerprint: "body", IdempotencyKey: "once"}
	first, err := a.reserveResource("alice", a.current.Load().Routes[rt.Key], spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.reserveResource("alice", b.current.Load().Routes[rt.Key], spec)
	if err != nil || second.First || second.Resource.ID != first.Resource.ID {
		t.Fatalf("idempotency split: %+v %v", second, err)
	}
	if _, err = b.resources.Lookup(protocol.ResourceKey{Owner: "bob", Route: rt.ID, Kind: protocol.HermesSession, ID: first.Resource.ID}); err != protocol.ErrResourceNotFound {
		t.Fatal("caller isolation changed", err)
	}
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
	spec := protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "client-session", Operation: "create", Fingerprint: "fingerprint", IdempotencyKey: "once"}
	first, err := a.reserveResource("alice", a.current.Load().Routes[rt.Key], spec)
	if err != nil {
		t.Fatal(err)
	}
	again, err := b.reserveResource("alice", b.current.Load().Routes[rt.Key], spec)
	if err != nil || again.First || again.Resource.ID != first.Resource.ID {
		t.Fatalf("idempotency diverged: %+v %v", again, err)
	}
	if err = b.resources.Update(first.Resource, protocol.ResourceUnknown); err != nil {
		t.Fatal(err)
	}
	if err = a.resources.Abandon(first.Resource); err != protocol.ErrResourceInUse {
		t.Fatal("unknown operation abandoned", err)
	}
	if _, err = b.resources.Lookup(protocol.ResourceKey{Owner: "bob", Route: rt.ID, Kind: protocol.HermesSession, ID: first.Resource.ID}); err != protocol.ErrResourceNotFound {
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
	g.current.Store(configuration.Build(configuration.Clone(f.core.current.Load().Config)))
	if err = g.EnableDurability(f.j); err != nil {
		t.Fatal(err)
	}
	got, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesSession, ID: first.Resource.ID})
	if err != nil || got.State != protocol.ResourceUnknown || got.EndpointID != first.Resource.EndpointID {
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
			r, err := g.reserveResource("alice", g.current.Load().Routes["test"], protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "shared", Operation: "create", Fingerprint: "f"})
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
