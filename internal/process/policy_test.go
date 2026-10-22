package process

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
)

func TestHandoffPolicyEnforcesQueueAndSurvivesAdoption(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	policy := p.DefaultHandoffPolicy()
	policy.StateWaitMillis, policy.QueueRequests, policy.QueueBytes = 100, 1, 4096
	policy.DeltaBytes, policy.SnapshotBytes = 2<<20, 8<<20
	n, c, opts, _ := candidateFixtureWithPolicy(t, upstream.URL, policy, func(opts *CandidateOptions) {
		opts.MaxDeltaBytes = 1 << 20
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if n.HandoffPolicy() != policy {
		t.Fatal("effective policy was not retained")
	}
	if _, err := c.FollowHandoff(ctx, n, "oversized", "rollout", opts.MaxSnapshotBytes+1); err == nil || !strings.Contains(err.Error(), "INVALID_HANDOFF_SNAPSHOT_BUDGET") || n.owner.Decision().Phase != handoff.Stable {
		t.Fatal("attempt snapshot limit was not enforced before preparation", err)
	}
	if err := n.shared.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	client := shared.NewLocalClient(n.shared, "queued")
	defer client.Close()
	first := make(chan error, 1)
	go func() { first <- client.CallContext(ctx, "config", shared.Request{}, nil) }()
	for n.shared.BarrierStatus().Waiting == 0 {
		select {
		case err := <-first:
			t.Fatal("first call did not wait", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := client.CallContext(ctx, "config", shared.Request{}, nil); err == nil || !strings.Contains(err.Error(), "STATE_HANDOFF_QUEUE_FULL") {
		t.Fatal("configured request limit ignored", err)
	}
	select {
	case err := <-first:
		if err == nil || !strings.Contains(err.Error(), "STATE_HANDOFF_WAIT_TIMEOUT") {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("configured state wait budget ignored")
	}
	if err := n.shared.Resume(); err != nil {
		t.Fatal(err)
	}
	result, err := n.TransferHandoff(ctx, c, "policy", "rollout", opts.MaxSnapshotBytes)
	if err != nil || !result.Activated {
		t.Fatal(result, err)
	}
	reply, err := c.channel.call(ctx, peerMessage{Action: "identity"})
	if err != nil || reply.Policy == nil || *reply.Policy != policy || reply.State == nil {
		t.Fatal("candidate did not adopt effective policy", reply, err)
	}
	if limits := reply.State.Limits; limits.Requests != 1 || limits.Bytes != 4096 || limits.Wait != 100*time.Millisecond {
		t.Fatal("restored dispatcher reset queue policy", limits)
	}
}

func TestHandoffBarrierTimeoutKeepsSourceWritable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	policy := p.DefaultHandoffPolicy()
	policy.BarrierMillis = 25
	n, c, opts, _ := candidateFixtureWithPolicy(t, upstream.URL, policy)
	leave, err := n.shared.Admit()
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = n.CheckpointHandoff(ctx, "timeout", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err == nil || ctx.Err() != nil || n.owner.Decision().Phase != handoff.Aborted || !n.Ready() || n.shared.BarrierStatus().Sealed {
		t.Fatal("barrier policy did not abort before caller deadline", err)
	}
	leave()
	if res := processRequest(n, "/agents/test/ok", "after-timeout"); res.Code != 200 {
		t.Fatal("source unavailable after barrier timeout", res.Code)
	}
}

func TestRepeatedFollowDoesNotRestartPreparationDeadline(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := c.FollowHandoff(ctx, n, "deadline", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	deadline := c.followDeadline
	if _, err := c.FollowHandoff(ctx, n, "deadline", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if c.followDeadline != deadline {
		t.Fatal("checkpoint reset preparation budget")
	}
	c.followDeadline = time.Now().Add(-time.Second)
	if _, err := c.FollowHandoff(ctx, n, "deadline", "rollout", opts.MaxSnapshotBytes); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired preparation resumed", err)
	}
	if ctx.Err() != nil || !n.Ready() || n.owner.Decision().Phase != handoff.Aborted {
		t.Fatal("preparation deadline did not retain source")
	}
}
