package shared

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestReceiptEvictionDoesNotRepeatOperationAcrossHandoff(t *testing.T) {
	var count, restored, executions atomic.Int64
	s := NewServer("token", transferBackend(&count, &executions))
	c := NewLocalClient(s, "candidate")
	defer c.Close()
	old := Request{ID: c.operationID()}
	var out Response
	if err := c.Call("acquire", old, &out); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8192; i++ {
		if err := c.Call("release", Request{Lease: "missing"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Call("acquire", old, nil); err == nil || err.Error() != "OPERATION_RESULT_UNKNOWN" {
		t.Fatal("evicted operation executed", err)
	}
	if executions.Load() != 1 {
		t.Fatal("duplicate acquisition")
	}
	if err := s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export()
	if err != nil {
		t.Fatal(err)
	}
	if v.Retired["candidate/"+c.stream] != 1 {
		t.Fatal("missing retirement boundary", v.Retired)
	}
	next := NewServer("token", transferBackend(&restored, &executions))
	if err = next.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = next.Import(v, "next"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(context.Background(), v.Epoch, "next", next); err != nil {
		t.Fatal(err)
	}
	if err = next.Resume(); err != nil {
		t.Fatal(err)
	}
	if err = c.Call("acquire", old, nil); err == nil || err.Error() != "OPERATION_RESULT_UNKNOWN" {
		t.Fatal("retirement boundary lost across epoch", err)
	}
	if err = c.Call("release", Request{Lease: out.Lease}, nil); err != nil {
		t.Fatal(err)
	}
	if restored.Load() != 0 || executions.Load() != 1 {
		t.Fatal("lease accounting changed")
	}
}

func TestNewClientForSameHolderHasIndependentReceiptSequence(t *testing.T) {
	var count, executions atomic.Int64
	s := NewServer("token", transferBackend(&count, &executions))
	c := NewLocalClient(s, "candidate")
	defer c.Close()
	old := Request{ID: c.operationID()}
	if err := c.Call("acquire", old, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8192; i++ {
		if err := c.Call("release", Request{Lease: "missing"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	next := NewLocalClient(s, "candidate")
	defer next.Close()
	if err := next.Call("acquire", Request{}, nil); err != nil {
		t.Fatal("new client inherited a different client's retirement floor", err)
	}
	if err := next.Call("acquire", old, nil); err == nil || err.Error() != "OPERATION_RESULT_UNKNOWN" {
		t.Fatal("new connection replayed an evicted operation", err)
	}
	if executions.Load() != 2 {
		t.Fatal("unexpected acquisitions", executions.Load())
	}
}
