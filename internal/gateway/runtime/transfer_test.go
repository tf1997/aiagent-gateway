package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

func TestSharedTransferRebuildsExecutionAndResourceLeases(t *testing.T) {
	f := sharedSetup(t)
	rt := create(t, f.core, "proxy", "http://127.0.0.1:8001")
	f.core.opts.MaxInflight = 1
	c := shared.NewLocalClient(f.server, "candidate")
	defer c.Close()
	q := shared.Request{ID: "acquire-once", Owner: "alice", Route: rt.Key, Revision: rt.Version, Entry: "proxy", Session: "existing"}
	var active shared.Response
	if err := c.Call("acquire", q, &active); err != nil {
		t.Fatal(err)
	}
	releaseExecution := c.Release(active.Lease)
	var resource shared.Response
	if err := c.Call("reserve", shared.Request{Owner: "alice", Route: rt.Key, Revision: rt.Version,
		Creation: protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "existing-resource", Operation: "create", Fingerprint: "body", IdempotencyKey: "once"}}, &resource); err != nil {
		t.Fatal(err)
	}
	var pin shared.Response
	if err := c.Call("pin", shared.Request{Key: resource.Reservation.Resource.Key()}, &pin); err != nil {
		t.Fatal(err)
	}
	releasePin := c.Release(pin.Lease)
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	image, err := f.server.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(image.Leases) != 2 {
		t.Fatal("missing lease descriptors", image.Leases)
	}
	if err = f.core.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.j.Close(); err != nil {
		t.Fatal(err)
	}
	core, err := Open(f.core.opts)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	j, err := state.Open(filepath.Join(filepath.Dir(f.socket), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	server, err := NewSharedServer(core, j)
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = server.Import(image, "new-owner"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(context.Background(), image.Epoch, "new-owner", server); err != nil {
		t.Fatal(err)
	}
	server.Resume()
	var replay shared.Response
	if err = c.Call("acquire", q, &replay); err != nil || replay.Lease != active.Lease {
		t.Fatal("lost acquire receipt", err)
	}
	if core.inflight.Load() != 1 || server.Count("candidate") != 2 {
		t.Fatal("lost active accounting")
	}
	if err = c.Call("acquire", shared.Request{Owner: "alice", Route: rt.Key, Revision: rt.Version, Entry: "proxy"}, nil); err == nil {
		t.Fatal("lost global capacity")
	}
	if err = core.resources.Forget(resource.Reservation.Resource); err != protocol.ErrResourceInUse {
		t.Fatal("lost resource pin", err)
	}
	releasePin()
	releaseExecution()
	releaseExecution()
	if core.inflight.Load() != 0 || c.Active() != 0 || server.Count("") != 0 {
		t.Fatal("transferred leases did not release exactly once")
	}
	if err = c.Call("acquire", shared.Request{Owner: "alice", Route: rt.Key, Revision: rt.Version, Entry: "proxy", Session: "existing"}, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.Endpoint.ID != active.Endpoint.ID {
		t.Fatal("session changed Agent during transfer")
	}
	c.Release(replay.Lease)()
}
