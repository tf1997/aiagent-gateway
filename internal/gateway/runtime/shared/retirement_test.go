package shared

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestReceiptEvictionPreservesAdmittedDiscoveryAndDeduplicatesRetries(t *testing.T) {
	var count, executions atomic.Int64
	backend := transferBackend(&count, &executions)
	discovering, discovered := make(chan struct{}), make(chan struct{})
	backend.Discover = func(context.Context, Request) { close(discovering); <-discovered }
	s := NewServer("token", backend)
	c := NewLocalClient(s, "candidate")
	defer c.Close()
	unblock := sync.OnceFunc(func() { close(discovered) })
	defer unblock()
	q := Request{ID: c.operationID()}
	completed := make(chan Response, 1)
	go func() {
		var out Response
		if err := c.Call("acquire", q, &out); err != nil {
			out.Error = err.Error()
		}
		completed <- out
	}()
	<-discovering
	for i := 0; i < 8193; i++ {
		if err := c.Call("release", Request{Lease: "missing"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A duplicate waits on the original admitted operation, even after its
	// sequence falls below the retirement floor. Cancelling that waiter must
	// neither cancel the original nor execute its mutation a second time.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := c.CallContext(ctx, "acquire", q, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("in-flight retry did not retain cancellable identity", err)
	}
	conflict := q
	conflict.Owner = "changed"
	if err := c.Call("acquire", conflict, nil); err == nil || err.Error() != "OPERATION_CONFLICT" {
		t.Fatal("in-flight fingerprint conflict accepted", err)
	}
	unblock()
	out := <-completed
	if out.Error != "" || out.Lease == "" || executions.Load() != 1 {
		t.Fatal("admitted operation was rejected or repeated", out, executions.Load())
	}
	var replay Response
	if err := c.Call("acquire", q, &replay); err != nil || replay.Lease != out.Lease || executions.Load() != 1 {
		t.Fatal("completed retry did not use its receipt", err, replay)
	}
	for i := 0; i < 8192; i++ {
		if err := c.Call("release", Request{Lease: "missing"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Call("acquire", q, nil); err == nil || err.Error() != "OPERATION_RESULT_UNKNOWN" || executions.Load() != 1 {
		t.Fatal("retired operation reused in-flight eligibility", err, executions.Load())
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
