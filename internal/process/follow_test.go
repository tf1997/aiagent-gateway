package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
)

type pauseSnapshotReply struct {
	net.Conn
	buffer       bytes.Buffer
	seen, resume chan struct{}
	once         sync.Once
}

func (c *pauseSnapshotReply) Read(raw []byte) (int, error) {
	if c.buffer.Len() == 0 {
		reply, err := readPeer(c.Conn)
		if err != nil {
			return 0, err
		}
		if reply.Action == "snapshot" {
			c.once.Do(func() { close(c.seen); <-c.resume })
		}
		if err = writePeer(&c.buffer, reply); err != nil {
			return 0, err
		}
	}
	return c.buffer.Read(raw)
}

func TestTransferPreviewTransmissionDoesNotBlockCandidateBusiness(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	policy := p.DefaultHandoffPolicy()
	policy.StateWaitMillis = 300
	n, c, opts, address := candidateFixtureWithPolicy(t, upstream.URL, policy)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	traffic := n.entry.Traffic()
	traffic.Revision++
	traffic.Candidate, traffic.Percent, traffic.Rollout = &c.target, 100, "preview"
	if err := n.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	blocked := &pauseSnapshotReply{Conn: c.channel.conn, seen: make(chan struct{}), resume: make(chan struct{})}
	c.channel.conn = blocked
	unblock := sync.OnceFunc(func() { close(blocked.resume) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := n.TransferHandoff(ctx, c, "preview-handoff", "preview", opts.MaxSnapshotBytes)
		done <- err
	}()
	select {
	case <-blocked.seen:
	case err := <-done:
		t.Fatalf("snapshot was not transmitted: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The ACK remains withheld while a real public request executes once.
	// A transport gate kept closed across transfer would exhaust its budget.
	response := publicRequest(t, address, "ok", "during-preview")
	_, err := io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err != nil || calls.Load() != 1 {
		t.Fatal("request failed or replayed", err, calls.Load())
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	d, err := handoff.ReadDecision(n.dataDir)
	if err != nil || d.Phase != handoff.Committed {
		t.Fatal("final handoff failed", d, err)
	}
}

func TestCandidateFollowsLiveStateBeforeFinalHandoff(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: started\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-release:
				fmt.Fprint(w, "data: [DONE]\n\n")
			case <-r.Context().Done():
			}
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	defer upstream.Close()
	n, c, opts, address := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream := candidateRequest(t, c, "/stream", "live")
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, "started") {
		t.Fatal(line, err)
	}
	first, err := c.FollowHandoff(ctx, n, "follow", "rollout", opts.MaxSnapshotBytes)
	if err != nil || first.Delta || !n.Ready() || n.shared.BarrierStatus().Sealed {
		t.Fatal("baseline failed or source remained sealed", first, err)
	}
	durable := n.owner.Decision()
	if durable.Phase != handoff.Prepared || durable.Snapshot != (handoff.Snapshot{}) {
		t.Fatal("preview authorized handoff", durable)
	}
	// B must reject even a direct private COMMIT after a preview. A also
	// refuses to yield against the returned, non-durable checkpoint metadata.
	preview := c.followDecision
	if _, err = c.channel.call(ctx, peerMessage{Action: "commit", Decision: preview}); err == nil {
		t.Fatal("read-only preview authorized commit")
	}
	if err = n.YieldHandoff(preview); err == nil {
		t.Fatal("read-only preview authorized source yield")
	}
	if res := processRequest(n, "/agents/test/ok", "after-baseline"); res.Code != 200 {
		t.Fatal("source did not resume after checkpoint", res.Code)
	}
	if err := n.releaseJournal.Apply(map[string]json.RawMessage{"follow-evidence": json.RawMessage(`{"complete":true}`)}, nil); err != nil {
		t.Fatal(err)
	}
	pauses := n.shared.BarrierStatus().PauseCount
	second, err := c.FollowHandoff(ctx, n, "follow", "rollout", opts.MaxSnapshotBytes)
	if err != nil || !second.Delta || !second.Operations || second.Cursor.Revision <= first.Cursor.Revision || second.Cursor.Digest == first.Cursor.Digest {
		t.Fatal("live mutations did not reach candidate", first, second, err)
	}
	if n.shared.BarrierStatus().PauseCount != pauses {
		t.Fatal("operation catchup captured another full checkpoint")
	}
	// Changes after the last acknowledgement must also reach the final view.
	if res := processRequest(n, "/agents/test/ok", "after-delta"); res.Code != 200 {
		t.Fatal(res.Code)
	}
	sealed, err := n.PrepareHandoff(ctx, "follow", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := c.ValidateHandoff(ctx, n, sealed, opts.MaxSnapshotBytes); err != nil {
			t.Fatal("repeated final validation lost its acknowledged base", i, err)
		}
	}
	result, err := n.TransferHandoff(ctx, c, "follow", "rollout", opts.MaxSnapshotBytes)
	if err != nil || !result.Activated || result.Source != nil || result.Decision.Snapshot.Revision <= second.Cursor.Revision {
		t.Fatal("final delta did not cover latest state", result, err)
	}
	close(release)
	if body, err := io.ReadAll(reader); err != nil || !strings.Contains(string(body), "[DONE]") {
		t.Fatal("candidate stream failed across delta adoption", string(body), err)
	}
	for {
		id, err := c.Identity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if id.Inflight == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("live lease was lost by delta import")
		}
		time.Sleep(10 * time.Millisecond)
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", address+"/agents/test/ok", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "after-delta")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || calls.Load() != 4 {
		t.Fatal("state was lost or business replayed", res.StatusCode, calls.Load())
	}
}

func TestGatewayImageDeltaPreservesUnchangedSharedData(t *testing.T) {
	// A one-byte insertion before a large unchanged shared-state field must
	// not recode the entire field through an outer base64 envelope.
	data := make([]byte, 2<<20)
	_, _ = rand.New(rand.NewSource(27)).Read(data)
	for i := range data {
		data[i] = 'a' + data[i]%26
	}
	makeImage := func(counter int) []byte {
		state, err := json.Marshal(struct {
			Counter int    `json:"counter"`
			Data    string `json:"data"`
		}{counter, string(data)})
		if err != nil {
			t.Fatal(err)
		}
		raw, _, err := shared.EncodeTransfer(gatewayImage{Format: gatewaySnapshotFormat, Shared: state}, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	base, next := makeImage(9), makeImage(10)
	from, to := shared.VersionOf(base, "owner", 9), shared.VersionOf(next, "owner", 10)
	raw, err := shared.EncodeDelta(context.Background(), base, next, from, to, 256<<10, 4<<20)
	if err != nil {
		t.Fatal("small shared-state change exceeded delta budget", err)
	}
	got, version, err := shared.ApplyDelta(context.Background(), base, from, raw, 256<<10, 4<<20)
	if err != nil || !bytes.Equal(got, next) || version != to {
		t.Fatal("delta changed the gateway image", version, err)
	}
}

func TestDeltaCapacityAbortsBeforeSourceYield(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := c.FollowHandoff(ctx, n, "overflow", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal("baseline should use the snapshot budget", err)
	}
	result, err := n.TransferHandoff(ctx, c, "overflow", "rollout", opts.MaxSnapshotBytes)
	if err == nil || !strings.Contains(err.Error(), "DELTA_CAPACITY") || result.Source != n || result.Activated || !n.Ready() || result.Decision.Phase != handoff.Aborted {
		t.Fatal("delta overflow bypassed the limit or yielded source", result, err)
	}
	if res := processRequest(n, "/agents/test/ok", "after-overflow"); res.Code != 200 {
		t.Fatal("source unavailable after delta capacity failure", res.Code)
	}
	if _, err := c.FollowHandoff(ctx, n, "retry", "new-rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal("fresh preparation could not replace rejected preview", err)
	}
}

func TestCandidateRejectsDeltaFromWrongAcknowledgement(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := c.FollowHandoff(ctx, n, "gap", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if res := processRequest(n, "/agents/test/ok", "changed"); res.Code != 200 {
		t.Fatal(res.Code)
	}
	raw, at, err := n.core.Recorder().Batch(c.followCursor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.FollowHandoff(ctx, n, "gap", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	// Replay an authenticated batch from the previous acknowledgement.
	snapshot, err := shared.NewSnapshot(raw, c.maxDeltaBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.channel.call(ctx, peerMessage{Action: "snapshot", Decision: c.followDecision, Manifest: snapshot.Manifest(), Preview: true, Operations: true, Cursor: &at}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < snapshot.Manifest().Chunks; i++ {
		chunk, _ := snapshot.Chunk(i)
		if _, err = c.channel.call(ctx, peerMessage{Action: "chunk", Chunk: &chunk}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.channel.call(ctx, peerMessage{Action: "validate", Decision: c.followDecision}); err == nil {
		t.Fatal("candidate accepted a revision gap")
	}
	if !n.Ready() || n.owner.Decision().Phase != handoff.Prepared {
		t.Fatal("rejected delta changed source authority")
	}
}

func TestDivergentOperationRecordRejectsFinalHandoff(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := c.FollowHandoff(ctx, n, "missing", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if err := n.AbortHandoff("unrelated"); err == nil || !n.core.Recorder().Active() {
		t.Fatal("unrelated abort stopped the current replication stream", err)
	}
	// Emulate a missing domain hook. The operation hash chain is valid, but
	// the mirror no longer matches the independently captured source image.
	n.core.Recorder().Put("runtime/requests", uint64(999))
	result, err := n.TransferHandoff(ctx, c, "missing", "rollout", opts.MaxSnapshotBytes)
	if err == nil || result.Activated || result.Source != n || result.Decision.Phase != handoff.Aborted || !n.Ready() {
		t.Fatal("incomplete replica was allowed to take ownership", result, err)
	}
	if res := processRequest(n, "/agents/test/ok", "after-rejected-image"); res.Code != 200 {
		t.Fatal("source did not resume", res.Code)
	}
	result, err = n.TransferHandoff(ctx, c, "fresh", "rollout", opts.MaxSnapshotBytes)
	if err != nil || !result.Activated || calls.Load() != 1 {
		t.Fatal("fresh baseline failed or replayed upstream work", result, err, calls.Load())
	}
}
