package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

func TestEntryRecordsTrackAllocationCompletionAndTrafficReset(t *testing.T) {
	for _, mode := range []string{"legacy", "compact"} {
		t.Run(mode, func(t *testing.T) { testEntryRecords(t, mode == "compact") })
	}
}

func testEntryRecords(t *testing.T, compact bool) {
	j, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	e, err := Open(j, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.UseCompactResults(compact); err != nil {
		t.Fatal(err)
	}
	old := p.Target{Release: "old", Incarnation: "old-boot", Instance: "a", URL: "http://127.0.0.1:18080"}
	next := p.Target{Release: "new", Incarnation: "new-boot", Instance: "b", URL: "http://127.0.0.1:18081"}
	if err := e.SetLocal(old, func(http.ResponseWriter, *http.Request, func(p.Result)) {}); err != nil {
		t.Fatal(err)
	}
	traffic := p.Traffic{Revision: 1, Stable: old, Candidate: &next, Percent: 50, Rollout: "test"}
	if err := e.Apply(0, traffic); err != nil {
		t.Fatal(err)
	}
	capture := func() Transfer {
		t.Helper()
		if err := e.Seal(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer e.Resume()
		v, err := e.Export()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	var recorder state.Recorder
	base := state.Cursor{Epoch: "owner", Handoff: "entry", Digest: strings.Repeat("0", 64)}
	mirror := StateRecords(capture())
	if err := recorder.Start(base, mirror, 1<<20); err != nil {
		t.Fatal(err)
	}
	e.SetRecorder(&recorder)
	check := func() {
		t.Helper()
		live := capture()
		raw, at, err := recorder.Batch(base)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.ApplyOperations(mirror, base, raw, 1<<20, 1<<20); err != nil {
			t.Fatal(err)
		}
		got, err := RenderStateRecords(mirror)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(live)
		b, _ := json.Marshal(got)
		if !bytes.Equal(a, b) {
			t.Fatalf("entry mirror diverged: live %s, mirror %s", a, b)
		}
		if err := recorder.Acknowledge(at); err != nil {
			t.Fatal(err)
		}
		base = at
	}
	for _, id := range []string{"first", "second", "third"} {
		if _, _, err := e.startIngress(id, "", next.Incarnation, id); err != nil {
			t.Fatal(err)
		}
		check()
	}
	if err := e.AbandonAllocation(context.Background(), next.Incarnation, "first"); err != nil {
		t.Fatal(err)
	}
	check()
	if err := e.RetireIngress(next.Incarnation); err != nil {
		t.Fatal(err)
	}
	check()
	traffic.Revision, traffic.Percent = 2, 100
	if err := e.Apply(1, traffic); err != nil {
		t.Fatal(err)
	}
	check()
	traffic.Revision, traffic.Candidate, traffic.Percent, traffic.Rollout = 3, nil, 0, ""
	if err := e.Apply(2, traffic); err != nil {
		t.Fatal(err)
	}
	check()
}
