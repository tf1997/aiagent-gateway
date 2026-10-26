package runtime

import (
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestStateRecordsMatchLiveImage(t *testing.T) {
	f, raw, digest := stateImageFixture(t)
	records, err := StateRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := state.Cursor{Epoch: "", Handoff: "records", Digest: digest}
	transfer, err := f.server.Export()
	if err != nil {
		t.Fatal(err)
	}
	at.Epoch, at.Revision = transfer.Epoch, transfer.Revision
	check := func() {
		t.Helper()
		live, _, err := f.core.ExportState(4 << 20)
		if err != nil {
			t.Fatal(err)
		}
		records, at, err := f.core.Recorder().Records()
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := RenderStateRecords(records, at.Epoch, at.Revision, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(live, got) {
			var a, b map[string]json.RawMessage
			json.Unmarshal(live, &a)
			json.Unmarshal(got, &b)
			for k, v := range a {
				if !bytes.Equal(v, b[k]) {
					t.Errorf("field %s: live %s, records %s", k, v, b[k])
				}
			}
			t.FailNow()
		}
	}
	f.server.StartReceiptOrder()
	if err = f.core.Recorder().Start(at, records, 4<<20); err != nil {
		t.Fatal(err)
	}
	check()
	client := shared.NewLocalClient(f.server, "records-client")
	defer client.Close()
	run := func(action string, q shared.Request) shared.Response {
		t.Helper()
		if err := f.server.Resume(); err != nil {
			t.Fatal(err)
		}
		var out shared.Response
		if err := client.Call(action, q, &out); err != nil {
			t.Fatal(action, err)
		}
		if err := f.server.Seal(context.Background()); err != nil {
			t.Fatal(err)
		}
		check()
		return out
	}
	route := f.core.current.Load().Routes["test"]
	if route == nil {
		t.Fatal("missing route")
	}
	f.core.setHealth(route.ID, route.Endpoints[0].ID, "proxy", true)
	lease := run("acquire", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Entry: "proxy", Session: "new-session"}).Lease
	run("release", shared.Request{Lease: lease})
	run("request_result", shared.Request{Flag: true})
	for _, kind := range []string{"tasks", "sessions"} {
		lease := run("cache_acquire", shared.Request{Entry: kind}).Lease
		raw := json.RawMessage(`{ "id": "exact" }`)
		id := run("cache_put", shared.Request{Entry: kind, Cache: protocol.ListSnapshot{Owner: "alice", Route: route.ID, Revision: route.Version, Expires: time.Now().Add(-time.Second), Size: 1, Bytes: len(raw), Tasks: []json.RawMessage{raw}}}).ID
		run("cache_get", shared.Request{Entry: kind, Session: id})
		run("release", shared.Request{Lease: lease})
	}
	resource := run("reserve", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Creation: protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "new-alias", Operation: "create", IdempotencyKey: "new-idem", Fingerprint: "new-body"}}).Reservation.Resource
	run("lookup", shared.Request{Key: resource.Key()})
	pin := run("pin", shared.Request{Key: resource.Key()}).Lease
	run("release", shared.Request{Lease: pin})
	run("alias", shared.Request{Resource: resource, Endpoint: "new-upstream", Flag: true})
	run("abandon", shared.Request{Resource: resource})
	run("retire", shared.Request{})
	if err := f.server.Resume(); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Retire("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Freeze(false); err != nil {
		t.Fatal(err)
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"new-route","protocol":"proxy","endpoints":[{"url":"http://127.0.0.1:8001"}]}`, "", "new-route-once"); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	f.core.retireCaches()
	shardIndex := 0
	f.core.maintainState(&shardIndex)
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestStateRecordsPreserveReceiptRetirement(t *testing.T) {
	f, raw, digest := stateImageFixture(t)
	records, err := StateRecords(raw)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := f.server.Export()
	if err != nil {
		t.Fatal(err)
	}
	base := state.Cursor{Epoch: transfer.Epoch, Handoff: "retirement", Revision: transfer.Revision, Digest: digest}
	f.server.StartReceiptOrder()
	if err := f.core.Recorder().Start(base, records, 32<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Resume(); err != nil {
		t.Fatal(err)
	}
	client := shared.NewLocalClient(f.server, "retirement-client")
	defer client.Close()
	for i := 0; i < 8193; i++ {
		if err := client.Call("request_result", shared.Request{}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch, at, err := f.core.Recorder().Batch(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyOperations(records, base, batch, 32<<20, 32<<20); err != nil {
		t.Fatal(err)
	}
	got, _, err := RenderStateRecords(records, at.Epoch, at.Revision, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	live, _, err := f.core.ExportState(32 << 20)
	if err != nil || !bytes.Equal(live, got) {
		t.Fatal("retiring receipts changed the reconstructed state", err)
	}
	transfer, err = f.server.Export()
	if err != nil || len(transfer.Receipts) != 8192 || len(transfer.Retired) == 0 {
		t.Fatal("test did not exercise receipt eviction", err)
	}
}
