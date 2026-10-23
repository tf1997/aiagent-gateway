package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type transferTransport func(*http.Request) (*http.Response, error)

func (t transferTransport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

func transferBackend(count *atomic.Int64, executions *atomic.Int64) Backend {
	return Backend{
		Execute: func(q Request, leases *Leases) Response {
			switch q.Action {
			case "acquire":
				executions.Add(1)
				count.Add(1)
				return Response{Lease: leases.Add(LeaseRecord{Kind: "acquire", Worker: q.Worker}, func() { count.Add(-1) })}
			case "release":
				_, err := leases.Release(q.Worker, q.Lease)
				if err != nil {
					return Response{Error: err.Error()}
				}
			}
			return Response{}
		},
		RestoreLease: func(l LeaseRecord) (func(), error) {
			count.Add(1)
			return func() { count.Add(-1) }, nil
		},
	}
}

func TestTransferPreservesLeaseAndReceiptAcrossExplicitAdoption(t *testing.T) {
	var oldCount, newCount, executions atomic.Int64
	a := NewServer("token", transferBackend(&oldCount, &executions))
	c := NewLocalClient(a, "candidate")
	q := Request{ID: "original-acquire"}
	var out Response
	if err := c.Call("acquire", q, &out); err != nil {
		t.Fatal(err)
	}
	release := c.Release(out.Lease)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	image, err := a.Export()
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := EncodeTransfer(image, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var copy Transfer
	if err = json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	b := NewServer("token", transferBackend(&newCount, &executions))
	if err = b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if err = b.Import(copy, "new-epoch"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(ctx, "wrong-epoch", "new-epoch", b); err == nil {
		t.Fatal("unrelated owner adopted")
	}
	if err = c.AdoptLocal(ctx, image.Epoch, "new-epoch", b); err != nil {
		t.Fatal(err)
	}
	b.Resume()
	var retry Response
	if err = c.Call("acquire", q, &retry); err != nil || retry.Lease != out.Lease || executions.Load() != 1 {
		t.Fatalf("receipt was not replayed: %+v %v executions=%d", retry, err, executions.Load())
	}
	if err = c.Call("acquire", Request{ID: q.ID, Owner: "changed"}, nil); err == nil {
		t.Fatal("semantic conflict accepted")
	}
	release()
	release()
	if b.Count("") != 0 || c.Active() != 0 || newCount.Load() != 0 || oldCount.Load() != 1 {
		t.Fatal("lease was lost, released twice, or released on old owner")
	}
	if err = b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := b.Export()
	if err != nil {
		t.Fatal(err)
	}
	d := NewServer("token", transferBackend(&newCount, &executions))
	if err = d.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if err = d.Import(next, "third-epoch"); err != nil {
		t.Fatal("consecutive transfer", err)
	}
}

func TestUnacknowledgedReleaseIsRetriedAfterAdoption(t *testing.T) {
	var count, newCount, executions atomic.Int64
	a := NewServer("token", transferBackend(&count, &executions))
	c := NewLocalClient(a, "candidate")
	var acquired Response
	if err := c.Call("acquire", Request{}, &acquired); err != nil {
		t.Fatal(err)
	}
	release := c.Release(acquired.Lease)
	if err := a.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	release()
	if c.Active() != 1 {
		t.Fatal("unacknowledged release disappeared")
	}
	v, err := a.Export()
	if err != nil {
		t.Fatal(err)
	}
	b := NewServer("token", transferBackend(&newCount, &executions))
	if err = b.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = b.Import(v, "next"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(context.Background(), v.Epoch, "next", b); err != nil {
		t.Fatal(err)
	}
	b.Resume()
	if err = c.RetryReleases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = c.RetryReleases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Active() != 0 || b.Count("") != 0 || newCount.Load() != 0 {
		t.Fatal("pending release was lost or duplicated")
	}
}

func TestQuiescedClientQueuesAgainstAdoptedOwner(t *testing.T) {
	var oldCount, newCount, executions atomic.Int64
	a := NewServer("token", transferBackend(&oldCount, &executions))
	c := NewLocalClient(a, "candidate")
	defer c.Close()
	var original Response
	if err := c.Call("acquire", Request{}, &original); err != nil {
		t.Fatal(err)
	}
	release := c.Release(original.Lease)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() { release(); close(released) }()
	acquired := make(chan error, 1)
	go func() { acquired <- c.CallContext(ctx, "acquire", Request{}, nil) }()
	deadline := time.Now().Add(time.Second)
	for c.gate.Status().Waiting != 2 {
		if time.Now().After(deadline) {
			t.Fatal("state operations did not queue before transport selection")
		}
		time.Sleep(time.Millisecond)
	}
	if executions.Load() != 1 {
		t.Fatal("queued acquire reached old owner")
	}
	v, err := a.Export()
	if err != nil {
		t.Fatal(err)
	}
	b := NewServer("token", transferBackend(&newCount, &executions))
	if err = b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if err = b.Import(v, "adopted"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(ctx, v.Epoch, "adopted", b); err != nil {
		t.Fatal("queued calls blocked adoption", err)
	}
	if err = b.Resume(); err != nil {
		t.Fatal(err)
	}
	if err = c.ResumeCalls(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("queued lease release stuck")
	}
	if err = <-acquired; err != nil {
		t.Fatal(err)
	}
	if err = c.RetryReleases(ctx); err != nil {
		t.Fatal(err)
	}
	if newCount.Load() != 1 || oldCount.Load() != 1 || executions.Load() != 2 || c.Active() != 0 {
		t.Fatal("queued mutations lost or replayed", oldCount.Load(), newCount.Load(), executions.Load())
	}
}

func TestLostReleaseResponseIsRecoveredWithoutReleasingAgain(t *testing.T) {
	var count, newCount, executions atomic.Int64
	a := NewServer("token", transferBackend(&count, &executions))
	c := NewClient("unused", "token", "candidate")
	c.client.Transport = transferTransport(func(r *http.Request) (*http.Response, error) {
		var q Request
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			return nil, err
		}
		response := a.dispatch(r.Context(), q)
		if q.Action == "release" {
			return nil, fmt.Errorf("connection lost after commit")
		}
		raw, _ := json.Marshal(response)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	})
	defer c.Close()
	var out Response
	if err := c.Call("acquire", Request{}, &out); err != nil {
		t.Fatal(err)
	}
	c.Release(out.Lease)()
	if c.Active() != 1 || count.Load() != 0 || !c.Failed() {
		t.Fatal("lost response was not retained as uncertain")
	}
	if err := a.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := a.Export()
	if err != nil {
		t.Fatal(err)
	}
	b := NewServer("token", transferBackend(&newCount, &executions))
	if err = b.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = b.Import(v, "next"); err != nil {
		t.Fatal(err)
	}
	if err = c.AdoptLocal(context.Background(), v.Epoch, "next", b); err != nil {
		t.Fatal(err)
	}
	b.Resume()
	if err = c.RetryReleases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Active() != 0 || newCount.Load() != 0 || c.Failed() {
		t.Fatal("release receipt did not survive adoption")
	}
}

func TestBarrierWaitsForDiscoveryAndDoesNotExecuteExpiredQueuedCalls(t *testing.T) {
	discovering, complete := make(chan struct{}), make(chan struct{})
	var executed atomic.Int64
	s := NewServer("token", Backend{
		Discover: func(context.Context, Request) { close(discovering); <-complete },
		Execute:  func(Request, *Leases) Response { executed.Add(1); return Response{} },
	})
	c := NewLocalClient(s, "candidate")
	done := make(chan error, 1)
	go func() { done <- c.Call("acquire", Request{}, nil) }()
	<-discovering
	sealCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := s.Seal(sealCtx); err == nil {
		t.Fatal("sealed while discovery was still running")
	}
	close(complete)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Export(); err == nil {
		t.Fatal("cancelled seal left export enabled")
	}
	if err := s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := c.CallContext(ctx, "update", Request{}, nil); err == nil {
		t.Fatal("expired queued call executed")
	}
	s.Resume()
	if executed.Load() != 1 {
		t.Fatal("queued request executed after deadline")
	}
	if err := c.Call("update", Request{}, nil); err != nil {
		t.Fatal("handoff timeout poisoned client", err)
	}
}

func TestTransferRejectsCorruptionAndBudgets(t *testing.T) {
	if _, _, err := EncodeTransfer(map[string]string{"large": "data"}, 1); err == nil {
		t.Fatal("snapshot limit ignored")
	}
	var barrier Barrier
	if err := barrier.seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.enter(context.Background(), (16<<20)+1); err == nil {
		t.Fatal("queue byte limit ignored")
	}
	var count, executions atomic.Int64
	s := NewServer("token", transferBackend(&count, &executions))
	if err := s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, leases := range [][]LeaseRecord{{{ID: "duplicate", Worker: "candidate"}, {ID: "duplicate", Worker: "candidate"}}, {{ID: "missing-worker"}}} {
		if err := s.Import(Transfer{Epoch: "old", Leases: leases}, "next"); err == nil {
			t.Fatal("invalid lease accepted")
		}
	}
	if count.Load() != 0 {
		t.Fatal("invalid import changed backend")
	}
	s.backend.RestoreLease = func(l LeaseRecord) (func(), error) {
		if l.ID == "bad" {
			return nil, fmt.Errorf("invalid backend reference")
		}
		count.Add(1)
		return func() { count.Add(-1) }, nil
	}
	if err := s.Import(Transfer{Epoch: "old", Leases: []LeaseRecord{{ID: "good", Worker: "candidate"}, {ID: "bad", Worker: "candidate"}}}, "next"); err == nil {
		t.Fatal("backend failure ignored")
	}
	if count.Load() != 0 || s.Count("") != 0 {
		t.Fatal("failed import leaked accounting")
	}
}
