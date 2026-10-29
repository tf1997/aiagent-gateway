package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	p "aiagent-gateway/internal/releaseprotocol"
)

func TestCandidateResumeRetriesExpiredLeaseRelease(t *testing.T) {
	source, cfg, _, _ := completeGateway(t, t.TempDir(), "source")
	configureGateway(t, source, "http://127.0.0.1:8001")
	client := shared.NewLocalClient(source.shared, "candidate")
	opts := cfg.Options()
	opts.StateClient, opts.WorkerID, opts.DeferBackground = client, "candidate", true
	business, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer business.Close()
	if err := client.ConfigureHandoffQueue(shared.QueueLimits{Wait: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	c := &Candidate{client: client, business: business, allocator: &candidateAllocator{}, identity: p.Identity{Incarnation: "candidate"}}
	var config, acquired shared.Response
	if err := client.Call("config", shared.Request{}, &config); err != nil {
		t.Fatal(err)
	}
	route := config.Config.Routes[0]
	q := shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Session: "resume", Entry: "proxy"}
	if err := client.Call("acquire", q, &acquired); err != nil {
		t.Fatal(err)
	}
	release := client.Release(acquired.Lease)
	if _, err := c.command(context.Background(), peerMessage{Action: "quiesce"}); err != nil {
		t.Fatal(err)
	}
	release()
	if client.Active() != 1 || source.shared.Count("candidate") != 1 {
		t.Fatal("release did not remain pending while quiesced")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.command(cancelled, peerMessage{Action: "resume_calls"}); !errors.Is(err, context.Canceled) {
		t.Fatal("resume hid a release retry error", err)
	}
	for i := 0; i < 2; i++ {
		reply, err := c.command(context.Background(), peerMessage{Action: "resume_calls"})
		if err != nil {
			t.Fatal(err)
		}
		if client.Active() != 0 || source.shared.Count("candidate") != 0 || reply.Identity.Inflight != 0 || c.callsSealed {
			t.Fatal("resume left lease accounting undrained", reply)
		}
	}
}
