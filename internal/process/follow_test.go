package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
)

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
	second, err := c.FollowHandoff(ctx, n, "follow", "rollout", opts.MaxSnapshotBytes)
	if err != nil || !second.Delta || second.Version.Revision <= first.Version.Revision || second.Version.Digest == first.Version.Digest {
		t.Fatal("live mutations did not reach candidate", first, second, err)
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
	if err != nil || !result.Activated || result.Source != nil || result.Decision.Snapshot.Revision <= second.Version.Revision {
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
	base := append([]byte(nil), c.followRaw...)
	old := c.followDecision
	if res := processRequest(n, "/agents/test/ok", "changed"); res.Code != 200 {
		t.Fatal(res.Code)
	}
	if _, err := c.FollowHandoff(ctx, n, "gap", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	// Resend a syntactically valid old-base batch over the authenticated wire.
	raw, err := shared.EncodeDelta(ctx, base, c.followRaw, imageVersion(old), imageVersion(c.followDecision), c.maxDeltaBytes, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := shared.NewSnapshot(raw, c.maxDeltaBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.channel.call(ctx, peerMessage{Action: "snapshot", Decision: c.followDecision, Manifest: snapshot.Manifest(), Preview: true, Delta: true}); err != nil {
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
