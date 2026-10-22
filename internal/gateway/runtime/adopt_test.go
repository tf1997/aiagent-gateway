package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

func TestWritableImagePreservesExactStateAndReleasesOnce(t *testing.T) {
	f, raw, digest := stateImageFixture(t)
	view, err := OpenStateView(raw, digest, len(raw), f.core.opts)
	if err != nil {
		t.Fatal(err)
	}
	viewBefore, err := json.Marshal(view.image)
	if err != nil {
		t.Fatal(err)
	}
	opts := f.core.opts
	opts.DataDir = t.TempDir()
	j, err := state.Open(filepath.Join(opts.DataDir, "shared"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	before := j.Snapshot()
	g, s, err := OpenStateOwner(opts, view, j, "next-epoch")
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if !reflect.DeepEqual(before, j.Snapshot()) {
		t.Fatal("import wrote before ownership commit")
	}
	if g.backgroundStarted {
		t.Fatal("import started background writers")
	}
	encoded, _, err := g.ExportState(len(raw) * 2)
	if err != nil {
		t.Fatal(err)
	}
	var original, restored stateImage
	if err = json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	// Epoch is the only intentional change during a planned adoption.
	restored.Transfer.Epoch = original.Transfer.Epoch
	for i := range restored.Transfer.Receipts {
		restored.Transfer.Receipts[i].Response.Epoch = original.Transfer.Epoch
	}
	if !reflect.DeepEqual(original, restored) {
		t.Fatal("writable import changed state, counters or exact TTLs")
	}
	if err = s.Resume(); err != nil {
		t.Fatal(err)
	}
	c := shared.NewLocalClient(s, "candidate")
	defer c.Close()
	for _, lease := range original.Transfer.Leases {
		q := shared.Request{Lease: lease.ID}
		if err = c.Call("release", q, nil); err != nil {
			t.Fatal(err)
		}
		if err = c.Call("release", q, nil); err != nil {
			t.Fatal(err)
		}
	}
	if s.Count("") != 0 || g.inflight.Load() != 0 {
		t.Fatal("imported accounting leaked or underflowed")
	}
	if err = s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, afterDigest, err := g.ExportState(len(raw) * 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenStateView(after, afterDigest, len(after), opts); err != nil {
		t.Fatal("released references do not reconcile", err)
	}
	viewAfter, err := json.Marshal(view.image)
	if err != nil {
		t.Fatal(err)
	}
	if string(viewBefore) != string(viewAfter) {
		t.Fatal("writable owner mutated read-only source")
	}
}

func TestLogicalRevisionIncludesAdminAndMaintenance(t *testing.T) {
	f := sharedSetup(t)
	before := f.server.BarrierStatus().Revision
	createDual(t, f.core, localInterfaces())
	afterAdmin := f.server.BarrierStatus().Revision
	if afterAdmin <= before {
		t.Fatal("admin mutation did not advance logical revision")
	}
	index := 0
	f.core.maintainState(&index)
	if f.server.BarrierStatus().Revision <= afterAdmin {
		t.Fatal("maintenance did not advance logical revision")
	}
}
