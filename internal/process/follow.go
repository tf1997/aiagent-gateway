package process

import (
	"context"
	"errors"
	"fmt"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
)

type FollowStatus struct {
	Version shared.ImageVersion `json:"version"`
	Delta   bool                `json:"delta"`
	Bytes   int                 `json:"transfer_bytes"`
}

func imageVersion(d handoff.Decision) shared.ImageVersion {
	return shared.ImageVersion{Epoch: d.Epoch, Revision: d.Snapshot.Revision, Digest: d.Snapshot.Digest, Bytes: int(d.Snapshot.Bytes)}
}

func sameHandoff(a, b handoff.Decision) bool {
	a.Phase, b.Phase = handoff.Prepared, handoff.Prepared
	a.Snapshot, b.Snapshot = handoff.Snapshot{}, handoff.Snapshot{}
	return a == b
}

// FollowHandoff transfers a read-only baseline, then bounded deltas on later
// calls. The source resumes before network transfer and remains the only
// writer. A failed batch aborts preparation; it requires a new handoff ID.
func (c *CandidateProcess) FollowHandoff(ctx context.Context, source *Gateway, id, rollout string, maxBytes int) (_ FollowStatus, err error) {
	started := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	source.handoffMu.Lock()
	owned := source.candidate == c
	source.handoffMu.Unlock()
	if !owned || c.verified.Phase != "" {
		return FollowStatus{}, fmt.Errorf("CANDIDATE_CANNOT_FOLLOW")
	}
	if maxBytes <= 0 || maxBytes > c.maxSnapshotBytes {
		return FollowStatus{}, fmt.Errorf("INVALID_HANDOFF_SNAPSHOT_BUDGET")
	}
	if c.followDecision.ID == id && c.followErr != nil {
		return FollowStatus{}, c.followErr
	}
	deadline := started.Add(time.Duration(c.policy.PrepareSeconds) * time.Second)
	if c.followDecision.ID == id && !c.followDeadline.IsZero() {
		deadline = c.followDeadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err = ctx.Err(); err != nil {
		if d := source.owner.Decision(); d.ID == id && d.Phase == handoff.Prepared {
			c.followErr = err
			err = errors.Join(err, source.AbortHandoff(id))
		}
		return FollowStatus{}, err
	}
	// Only the consistent capture needs the candidate transport gate closed.
	// Snapshot transmission and validation can be slow and must not consume
	// the bounded wait budget of incoming business requests.
	captureCtx, stopCapture := context.WithTimeout(ctx, time.Duration(c.policy.BarrierMillis)*time.Millisecond)
	_, err = c.channel.call(captureCtx, peerMessage{Action: "quiesce"})
	if err != nil {
		stopCapture()
		return FollowStatus{}, err
	}
	raw, d, err := source.CheckpointHandoff(captureCtx, id, rollout, handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, maxBytes)
	stopCapture()
	if source.StorageHealthy() {
		resume, stopResume := context.WithTimeout(context.Background(), time.Duration(c.policy.StateWaitMillis)*time.Millisecond)
		_, resumeErr := c.channel.call(resume, peerMessage{Action: "resume_calls"})
		stopResume()
		err = errors.Join(err, resumeErr)
	}
	if err != nil {
		return FollowStatus{}, err
	}
	if c.followDecision.ID != id {
		c.followRaw, c.followErr = nil, nil
		c.followDecision = d
		c.followDeadline = deadline
	}
	defer func() {
		if err != nil {
			c.followErr = err
			err = errors.Join(err, source.AbortHandoff(id))
		}
	}()
	status, err := c.sendHandoffImage(ctx, d, raw, maxBytes, true)
	if err != nil {
		return status, err
	}
	c.followRaw, c.followDecision = raw, d
	return status, nil
}

// sendHandoffImage requires c.mu. Delta generation is tied to the last
// acknowledgement; retries must not invent a newer base or bypass capacity.
func (c *CandidateProcess) sendHandoffImage(ctx context.Context, d handoff.Decision, raw []byte, maxBytes int, preview bool) (FollowStatus, error) {
	status := FollowStatus{Version: imageVersion(d)}
	payload, budget := raw, maxBytes
	if c.followDecision.ID == d.ID && c.followErr != nil {
		return status, c.followErr
	}
	if c.followRaw != nil {
		if !sameHandoff(c.followDecision, d) {
			return status, handoff.ErrConflict
		}
		var err error
		payload, err = shared.EncodeDelta(ctx, c.followRaw, raw, imageVersion(c.followDecision), imageVersion(d), c.maxDeltaBytes, maxBytes)
		if err != nil {
			return status, err
		}
		budget, status.Delta = c.maxDeltaBytes, true
	}
	snapshot, err := shared.NewSnapshot(payload, budget)
	if err != nil {
		return status, err
	}
	status.Bytes = snapshot.Manifest().Bytes
	if _, err = c.channel.call(ctx, peerMessage{Action: "snapshot", Decision: d, Manifest: snapshot.Manifest(), Preview: preview, Delta: status.Delta}); err != nil {
		return status, err
	}
	for i := 0; i < snapshot.Manifest().Chunks; i++ {
		chunk, err := snapshot.Chunk(i)
		if err != nil {
			return status, err
		}
		if _, err = c.channel.call(ctx, peerMessage{Action: "chunk", Chunk: &chunk}); err != nil {
			return status, err
		}
	}
	reply, err := c.channel.call(ctx, peerMessage{Action: "validate", Decision: d})
	if err != nil {
		return status, err
	}
	if reply.View == nil || *reply.View != status.Version || c.checkIdentity(reply.Identity) != nil {
		return status, fmt.Errorf("CANDIDATE_VIEW_ACK_MISMATCH")
	}
	return status, nil
}

func (c *CandidateProcess) hasFollow(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.followDecision.ID == id
}
