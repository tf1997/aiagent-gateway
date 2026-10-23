package shared

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

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
