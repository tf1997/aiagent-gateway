package shared

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/state"
)

func TestAbortRecordingPreservesInProgressRevision(t *testing.T) {
	b := &Barrier{}
	var recorder state.Recorder
	b.SetRecorder(&recorder)
	base := state.Cursor{Epoch: "owner", Handoff: "first", Digest: strings.Repeat("0", 64)}
	if err := recorder.Start(base, map[string]json.RawMessage{}, 8192); err != nil {
		t.Fatal(err)
	}
	leave, err := b.TryEnter()
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	for i := 0; i < 5; i++ {
		recorder.Put("in-progress", i)
	}
	published := recorder.Cursor().Revision
	if b.Status().Revision != published || b.Revision() != published {
		t.Fatal("status hid published operations")
	}
	b.StopRecording()
	if recorder.Active() || b.Revision() != published {
		t.Fatal("aborting preparation regressed the sequence", b.Status())
	}
	leave()
	if b.Revision() <= published {
		t.Fatal("operation completion reused an old revision")
	}
	base.Handoff, base.Revision = "retry", b.Revision()
	if err := recorder.Start(base, map[string]json.RawMessage{}, 8192); err != nil {
		t.Fatal(err)
	}
	recorder.Put("next", true)
	b.StopRecording()
	if b.Revision() <= base.Revision {
		t.Fatal("new preparation reused an old revision")
	}
	base.Handoff, base.Revision = "overflow", b.Revision()
	if err := recorder.Start(base, map[string]json.RawMessage{}, 1); err != nil {
		t.Fatal(err)
	}
	recorder.Put("too-large", true)
	leave, err = b.TryEnter()
	if err != nil {
		t.Fatal(err)
	}
	leave()
	if recorder.Err() == nil || b.Revision() <= base.Revision {
		t.Fatal("overflow stopped the owner's logical sequence")
	}
	b.StopRecording()
}

func TestBarrierPauseMeasurements(t *testing.T) {
	b := &Barrier{}
	if err := b.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := b.Status(); !status.Sealed || status.PauseCount != 0 {
		t.Fatal("unfinished pause counted as complete", status)
	}
	if err := b.Resume(); err != nil {
		t.Fatal(err)
	}
	first := b.Status()
	if first.Sealed || first.CurrentPauseMicros != 0 || first.PauseCount != 1 || first.TotalPauseMicros != first.LastPauseMicros || first.MaxPauseMicros != first.LastPauseMicros {
		t.Fatal("incorrect completed pause", first)
	}
	if err := b.Resume(); err != nil || b.Status() != first {
		t.Fatal("duplicate resume counted another pause", err)
	}
	leave, err := b.TryEnter()
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Seal(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	last := b.Status()
	if last.Sealed || last.Sealing || last.CurrentPauseMicros != 0 || last.PauseCount != 2 || last.LastPauseMicros < 15000 || last.MaxPauseMicros < last.LastPauseMicros || last.TotalPauseMicros < first.TotalPauseMicros+last.LastPauseMicros {
		t.Fatal("cancelled seal lost its admission pause", last)
	}
}

func TestDirectControlsRespectSealedLeaseImage(t *testing.T) {
	var count, executions atomic.Int64
	var frozen bool
	backend := transferBackend(&count, &executions)
	execute := backend.Execute
	backend.Execute = func(q Request, leases *Leases) Response {
		if q.Action == "retire" {
			leases.Retire(q.Worker)
			return Response{}
		}
		return execute(q, leases)
	}
	backend.Freeze = func(v bool) { frozen = v }
	s := NewServer("token", backend)
	c := NewLocalClient(s, "candidate")
	defer c.Close()
	if err := c.Call("acquire", Request{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Freeze(true); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := s.Export()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retire("candidate"); !errors.Is(err, ErrSealed) {
		t.Fatal("retire did not report closed admission", err)
	}
	if err := s.Freeze(false); !errors.Is(err, ErrSealed) {
		t.Fatal("freeze did not report closed admission", err)
	}
	after, err := s.Export()
	if err != nil || !reflect.DeepEqual(before, after) || !frozen || count.Load() != 1 {
		t.Fatal("direct controls mutated sealed state", err)
	}
	if err := s.Resume(); err != nil {
		t.Fatal(err)
	}
	if err := s.Retire("candidate"); err != nil || count.Load() != 0 {
		t.Fatal("retire did not resume", err)
	}
	if err := s.Freeze(false); err != nil || frozen {
		t.Fatal("configuration remained frozen after abort", err)
	}
}

func TestDirectRetirePropagatesBackendFailure(t *testing.T) {
	s := NewServer("token", Backend{Execute: func(Request, *Leases) Response {
		return Response{Error: "STATE_PERSISTENCE_FAILED"}
	}})
	if err := s.Retire("dead"); err == nil || err.Error() != "STATE_PERSISTENCE_FAILED" {
		t.Fatal("retire silently discarded state failure", err)
	}
}
