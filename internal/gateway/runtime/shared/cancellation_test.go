package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteCancellationOnlyFencesUncertainMutations(t *testing.T) {
	for _, action := range []string{"status", "mutation"} {
		t.Run(action, func(t *testing.T) {
			var mutations atomic.Int64
			s := NewServer("token", Backend{Execute: func(q Request, _ *Leases) Response {
				if q.Action == "mutation" {
					mutations.Add(1)
				}
				return Response{}
			}})
			c := NewClient("unused", "token", "candidate")
			defer c.Close()
			started := make(chan struct{})
			c.client.Transport = transferTransport(func(r *http.Request) (*http.Response, error) {
				var q Request
				if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
					return nil, err
				}
				out := s.dispatch(r.Context(), q)
				if q.Action == action {
					close(started)
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				raw, _ := json.Marshal(out)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.CallContext(ctx, action, Request{}, nil) }()
			<-started
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if c.Failed() != (action == "mutation") {
				t.Fatal("incorrect client fencing after cancellation", c.Failed())
			}
			err := c.Call("config", Request{}, nil)
			if action == "mutation" {
				if err == nil || mutations.Load() != 1 {
					t.Fatal("uncertain mutation was retried or unfenced", err, mutations.Load())
				}
			} else {
				if err != nil {
					t.Fatal("read cancellation poisoned subsequent calls", err)
				}
				s.mu.Lock()
				s.epoch = "unexpected-owner"
				s.mu.Unlock()
				if err := c.Call("config", Request{}, nil); err == nil || !c.Failed() {
					t.Fatal("read-only calls no longer fence an unexpected epoch", err)
				}
			}
		})
	}
}

func TestLocalDeadlineWhileWaitingForDispatcher(t *testing.T) {
	for _, discovery := range []bool{false, true} {
		name := "before_discovery"
		if discovery {
			name = "after_discovery"
		}
		t.Run(name, func(t *testing.T) {
			locked, release := make(chan struct{}), make(chan struct{})
			discovering, discovered := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			finishDiscovery := sync.OnceFunc(func() { close(discovered) })
			var executed atomic.Int64
			s := NewServer("token", Backend{
				Discover: func(context.Context, Request) { close(discovering); <-discovered },
				Execute: func(q Request, _ *Leases) Response {
					if q.Action == "slow" {
						close(locked)
						<-release
					} else {
						executed.Add(1)
					}
					return Response{}
				},
			})
			c := NewLocalClient(s, "candidate")
			defer c.Close()
			defer unblock()
			defer finishDiscovery()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done, slow := make(chan error, 1), make(chan error, 1)
			if discovery {
				go func() { done <- c.CallContext(ctx, "acquire", Request{}, nil) }()
				<-discovering
			}
			go func() { slow <- c.Call("slow", Request{}, nil) }()
			<-locked
			if discovery {
				finishDiscovery()
			} else {
				go func() { done <- c.CallContext(ctx, "config", Request{}, nil) }()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("deadline waited for unrelated execution to unlock")
			}
			unblock()
			if err := <-slow; err != nil || executed.Load() != 0 {
				t.Fatal("expired operation executed", err, executed.Load())
			}
		})
	}
}

func TestReleaseRetryDeadlineWhileOriginalReleaseIsRunning(t *testing.T) {
	var count, executions atomic.Int64
	backend := transferBackend(&count, &executions)
	execute := backend.Execute
	started, complete := make(chan struct{}), make(chan struct{})
	backend.Execute = func(q Request, leases *Leases) Response {
		if q.Action == "release" {
			close(started)
			<-complete
		}
		return execute(q, leases)
	}
	c := NewLocalClient(NewServer("token", backend), "candidate")
	defer c.Close()
	unblock := sync.OnceFunc(func() { close(complete) })
	defer unblock()
	var acquired Response
	if err := c.Call("acquire", Request{}, &acquired); err != nil {
		t.Fatal(err)
	}
	release := c.Release(acquired.Lease)
	released := make(chan struct{})
	go func() { release(); close(released) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	retried := make(chan error, 1)
	go func() { retried <- c.RetryReleases(ctx) }()
	select {
	case err := <-retried:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release compensation blocked past the resume deadline")
	}
	unblock()
	<-released
	if err := c.RetryReleases(context.Background()); err != nil || count.Load() != 0 || c.Active() != 0 {
		t.Fatal("cancelled compensation lost or duplicated the original release", err, count.Load(), c.Active())
	}
}
