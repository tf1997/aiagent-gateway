package process

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

const gatewaySnapshotFormat = "gateway-process-state-3"

type gatewayImage struct {
	Format  string           `json:"format"`
	Handoff string           `json:"handoff_id"`
	Rollout string           `json:"rollout_id"`
	Source  handoff.Identity `json:"source"`
	Target  handoff.Identity `json:"target"`
	// Embed canonical JSON directly so byte insertions do not shift the
	// base64 encoding of every subsequent unchanged shared-state field.
	Shared       json.RawMessage            `json:"shared"`
	SharedDigest string                     `json:"shared_digest"`
	Entry        entry.Transfer             `json:"entry"`
	Release      map[string]json.RawMessage `json:"release"`
}

// PrepareHandoff seals all currently assembled writable domains. The caller
// must stop the release controller before entering; this function owns the
// entry and shared admission barriers until AbortHandoff or YieldHandoff.
// The returned durable decision is also the candidate's validation contract.
func (n *Gateway) PrepareHandoff(ctx context.Context, id, rollout string, target handoff.Identity, maxBytes int) (_ handoff.Decision, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(n.policy.BarrierMillis)*time.Millisecond)
	defer cancel()
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if !n.StorageHealthy() {
		return handoff.Decision{}, fmt.Errorf("STATE_NOT_READY")
	}
	if maxBytes <= 0 || maxBytes > n.policy.SnapshotBytes {
		return handoff.Decision{}, fmt.Errorf("INVALID_HANDOFF_SNAPSHOT_BUDGET")
	}
	if d := n.owner.Decision(); d.Phase == handoff.Sealed {
		if d.ID != id || d.Rollout != rollout || d.Target != target || !n.shared.BarrierStatus().Sealed {
			return handoff.Decision{}, handoff.ErrConflict
		}
		if err = ctx.Err(); err != nil {
			return handoff.Decision{}, err
		}
		return d, nil
	}
	if n.business.WorkerIdentity(n.identity).Inflight != 0 || n.entry.LocalActive() != 0 {
		return handoff.Decision{}, fmt.Errorf("SOURCE_NOT_DRAINED")
	}
	if err = n.owner.Prepare(id, rollout, target); err != nil {
		return handoff.Decision{}, err
	}
	entrySealed, stateSealed := false, false
	defer func() {
		if err == nil {
			return
		}
		// Never reopen admission if the durable abort is uncertain.
		if abortErr := n.owner.Abort(id); abortErr != nil {
			n.StopIngress()
			err = errors.Join(err, abortErr)
			return
		}
		if stateSealed {
			err = errors.Join(err, n.shared.Resume())
		}
		if entrySealed {
			n.entry.Resume()
		}
	}()
	if err = n.entry.Seal(ctx); err != nil {
		return handoff.Decision{}, err
	}
	entrySealed = true
	if err = n.shared.Seal(ctx); err != nil {
		return handoff.Decision{}, err
	}
	stateSealed = true
	if err = ctx.Err(); err != nil {
		return handoff.Decision{}, err
	}
	encoded, _, err := n.encodeHandoffImage(n.owner.Decision(), maxBytes)
	if err != nil {
		return handoff.Decision{}, err
	}
	if err = ctx.Err(); err != nil {
		return handoff.Decision{}, err
	}
	snapshot, err := n.owner.PersistSnapshot(id, encoded, n.shared.BarrierStatus().Revision, maxBytes)
	if err != nil {
		return handoff.Decision{}, err
	}
	if err = ctx.Err(); err != nil {
		return handoff.Decision{}, err
	}
	if err = n.owner.Seal(id, snapshot); err != nil {
		return handoff.Decision{}, err
	}
	return n.owner.Decision(), nil
}

func (n *Gateway) encodeHandoffImage(d handoff.Decision, maxBytes int) ([]byte, string, error) {
	raw, digest, err := n.core.ExportState(maxBytes)
	if err != nil {
		return nil, "", err
	}
	e, err := n.entry.Export()
	if err != nil {
		return nil, "", err
	}
	image := gatewayImage{gatewaySnapshotFormat, d.ID, d.Rollout, d.Source, d.Target, raw, digest, e, n.releaseJournal.Snapshot()}
	return shared.EncodeTransfer(image, maxBytes)
}

// CheckpointHandoff briefly closes admission for a consistent read-only view,
// then resumes the source before the image is transferred. The durable record
// remains PREPARED: neither the returned metadata nor a preview acknowledgement
// grants permission to yield or commit. Calls may be repeated for ordered deltas.
func (n *Gateway) CheckpointHandoff(ctx context.Context, id, rollout string, target handoff.Identity, maxBytes int) (_ []byte, _ handoff.Decision, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(n.policy.BarrierMillis)*time.Millisecond)
	defer cancel()
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if !n.StorageHealthy() || n.owner.Decision().Phase == handoff.Sealed {
		return nil, handoff.Decision{}, fmt.Errorf("CHECKPOINT_SOURCE_UNAVAILABLE")
	}
	if maxBytes <= 0 || maxBytes > n.policy.SnapshotBytes {
		return nil, handoff.Decision{}, fmt.Errorf("INVALID_HANDOFF_SNAPSHOT_BUDGET")
	}
	if n.business.WorkerIdentity(n.identity).Inflight != 0 || n.entry.LocalActive() != 0 {
		return nil, handoff.Decision{}, fmt.Errorf("SOURCE_NOT_DRAINED")
	}
	if err = n.owner.Prepare(id, rollout, target); err != nil {
		return nil, handoff.Decision{}, err
	}
	entrySealed, stateSealed := false, false
	defer func() {
		if err != nil {
			if abortErr := n.owner.Abort(id); abortErr != nil {
				n.StopIngress()
				err = errors.Join(err, abortErr)
				return
			}
		}
		if stateSealed {
			if resumeErr := n.shared.Resume(); resumeErr != nil {
				n.StopIngress()
				err = errors.Join(err, resumeErr)
				return
			}
		}
		if entrySealed {
			n.entry.Resume()
		}
	}()
	if err = n.entry.Seal(ctx); err != nil {
		return nil, handoff.Decision{}, err
	}
	entrySealed = true
	if err = n.shared.Seal(ctx); err != nil {
		return nil, handoff.Decision{}, err
	}
	stateSealed = true
	d := n.owner.Decision()
	raw, digest, err := n.encodeHandoffImage(d, maxBytes)
	if err != nil {
		return nil, handoff.Decision{}, err
	}
	if err = ctx.Err(); err != nil {
		return nil, handoff.Decision{}, err
	}
	d.Snapshot = handoff.Snapshot{Digest: digest, Bytes: int64(len(raw)), Revision: n.shared.BarrierStatus().Revision}
	return raw, d, nil
}

// AbortHandoff is for a source which has not released owner.lock. A source
// which has yielded must reacquire the lock and inspect the durable decision.
func (n *Gateway) AbortHandoff(id string) error {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if err := n.owner.Abort(id); err != nil {
		return err
	}
	if err := n.shared.Resume(); err != nil {
		return err
	}
	n.entry.Resume()
	return nil
}

// HandoffSnapshot returns bounded chunks of the exact durable image. A peer
// must authenticate and bind the manifest to this decision before accepting it.
func (n *Gateway) HandoffSnapshot(id string, maxBytes int) (*shared.Snapshot, error) {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	raw, err := n.owner.LoadSnapshot(id, maxBytes)
	if err != nil {
		return nil, err
	}
	return shared.NewSnapshot(raw, maxBytes)
}

// ValidateHandoff is read-only and has no storage, locks, network, or timers.
func ValidateHandoff(raw []byte, decision handoff.Decision, maxBytes int, options gateway.Options) (*gateway.StateView, error) {
	_, view, err := decodeGatewayImage(raw, decision, maxBytes, options)
	return view, err
}

func decodeGatewayImage(raw []byte, d handoff.Decision, maxBytes int, options gateway.Options) (gatewayImage, *gateway.StateView, error) {
	var image gatewayImage
	if err := d.Validate(); err != nil {
		return image, nil, err
	}
	if d.Phase != handoff.Sealed || int64(len(raw)) != d.Snapshot.Bytes {
		return image, nil, handoff.ErrConflict
	}
	if err := shared.DecodeTransfer(raw, d.Snapshot.Digest, maxBytes, &image); err != nil {
		return image, nil, err
	}
	if image.Format != gatewaySnapshotFormat || image.Handoff != d.ID || image.Rollout != d.Rollout || image.Source != d.Source || image.Target != d.Target {
		return image, nil, fmt.Errorf("HANDOFF_IMAGE_IDENTITY_MISMATCH")
	}
	if err := image.Entry.Validate(); err != nil {
		return image, nil, err
	}
	view, err := gateway.OpenStateView(image.Shared, image.SharedDigest, maxBytes, options)
	if err != nil {
		return image, nil, err
	}
	if view.Metadata().LogicalRevision != d.Snapshot.Revision {
		return image, nil, fmt.Errorf("HANDOFF_IMAGE_REVISION_MISMATCH")
	}
	return image, view, nil
}

// YieldHandoff closes every writer before releasing owner.lock. It cannot be
// called with an unvalidated or stale candidate acknowledgement.
func (n *Gateway) YieldHandoff(decision handoff.Decision) error {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if decision != n.owner.Decision() || decision.Phase != handoff.Sealed || !n.shared.BarrierStatus().Sealed || n.entry.LocalActive() != 0 || n.business.WorkerIdentity(n.identity).Inflight != 0 {
		return handoff.ErrConflict
	}
	return n.Close()
}

// AdoptGateway binds snapshot import, all domain writer locks and COMMITTED.
// A business runtime already executing candidate streams may be supplied with
// its StateClient; those streams keep their leases through explicit adoption.
// A nil pair starts a new executor after commit. Callers must not retry this as
// a cold start when an error occurs: the decision may already be COMMITTED.
func AdoptGateway(ctx context.Context, cfg appconfig.Config, target p.Target, identity p.Identity, expected handoff.Decision, maxBytes int, client *shared.Client, business *gateway.Gateway) (_ *Gateway, err error) {
	return openHandoffGateway(ctx, cfg, target, identity, expected, maxBytes, client, business, false, p.DefaultHandoffPolicy())
}

// RestoreGateway recovers a yielded source without cold-starting or discarding
// live candidate leases. Acquiring the lock and rereading SEALED precede all
// reconstruction. COMMITTED can never be rolled back through this entry point.
func RestoreGateway(ctx context.Context, cfg appconfig.Config, target p.Target, identity p.Identity, expected handoff.Decision, maxBytes int) (*Gateway, error) {
	return openHandoffGateway(ctx, cfg, target, identity, expected, maxBytes, nil, nil, true, p.DefaultHandoffPolicy())
}

func openHandoffGateway(ctx context.Context, cfg appconfig.Config, target p.Target, identity p.Identity, expected handoff.Decision, maxBytes int, client *shared.Client, business *gateway.Gateway, restoreSource bool, policy p.HandoffPolicy) (_ *Gateway, err error) {
	policy = policy.Effective()
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if maxBytes <= 0 || maxBytes > policy.SnapshotBytes {
		return nil, fmt.Errorf("INVALID_HANDOFF_SNAPSHOT_BUDGET")
	}
	wanted := expected.Target
	if restoreSource {
		wanted = expected.Source
	}
	if identity.Incarnation != target.Incarnation || identity.Release != target.Release || identity.Instance != target.Instance || wanted != (handoff.Identity{Incarnation: identity.Incarnation, Release: identity.Release}) || (client == nil) != (business == nil) {
		return nil, handoff.ErrConflict
	}
	n := &Gateway{target: target, identity: identity, dataDir: cfg.DataDir, config: cfg, policy: policy, privateRoot: filepath.Join(filepath.Dir(cfg.StateSocket), "p")}
	defer func() {
		if err != nil {
			n.Close()
		}
	}()
	n.owner, err = handoff.Acquire(cfg.DataDir, wanted)
	if err != nil {
		return nil, err
	}
	actual := n.owner.Decision()
	if restoreSource && (actual.Phase == handoff.Committed || actual.Phase == handoff.Completed) {
		return nil, handoff.ErrCommitted
	}
	if actual != expected || expected.Phase != handoff.Sealed {
		return nil, handoff.ErrConflict
	}
	raw, err := n.owner.LoadSnapshot(expected.ID, maxBytes)
	if err != nil {
		return nil, err
	}
	opts := ownerOptions(cfg, policy)
	image, view, err := decodeGatewayImage(raw, expected, maxBytes, opts)
	if err != nil {
		return nil, err
	}
	n.journal, err = state.Open(filepath.Join(cfg.DataDir, "shared"))
	if err != nil {
		return nil, err
	}
	if restoreSource {
		n.core, n.shared, err = gateway.RestoreStateOwner(opts, view, n.journal)
	} else {
		n.core, n.shared, err = gateway.OpenStateOwner(opts, view, n.journal, expected.NextEpoch)
	}
	if err != nil {
		return nil, err
	}
	n.entryJournal, err = state.Open(filepath.Join(cfg.DataDir, "entry"))
	if err != nil {
		return nil, err
	}
	n.entry, err = entry.Import(n.entryJournal, cfg.AdminToken, n.shared.Resolve, image.Entry)
	if err != nil {
		return nil, err
	}
	n.entry.SetAdmission(n.shared.Admit)
	n.releaseJournal, err = state.Open(filepath.Join(cfg.DataDir, "release"))
	if err != nil {
		return nil, err
	}
	n.releaseJournal.SetAdmission(n.shared.Admit)
	a, _ := json.Marshal(image.Release)
	b, _ := json.Marshal(n.releaseJournal.Snapshot())
	if !bytes.Equal(a, b) {
		return nil, fmt.Errorf("RELEASE_CHANGED_AFTER_SEAL")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if restoreSource {
		err = n.owner.Abort(expected.ID)
	} else {
		err = n.owner.Commit(expected.ID, expected.Snapshot)
	}
	if err != nil {
		return nil, err
	}
	if client != nil {
		if err = client.AdoptLocal(ctx, view.Metadata().Epoch, expected.NextEpoch, n.shared); err != nil {
			return nil, err
		}
		n.business = business
	}
	if err = n.shared.Resume(); err != nil {
		return nil, err
	}
	if n.business == nil {
		opts.StateClient = shared.NewLocalClient(n.shared, identity.Incarnation)
		opts.WorkerID, opts.DeferBackground = identity.Incarnation, true
		n.business, err = gateway.Open(opts)
		if err != nil {
			return nil, err
		}
	} else if err = client.RetryReleases(ctx); err != nil {
		return nil, err
	}
	if err = n.entry.SetLocal(target, n.execute); err != nil {
		return nil, err
	}
	n.entry.Resume()
	// Keep rollout evidence through activation; only completion may clear it.
	traffic := n.entry.Traffic()
	if restoreSource {
		traffic.Stable, traffic.Percent = target, 0
	} else {
		traffic.Stable, traffic.Candidate, traffic.Percent = target, nil, 0
	}
	traffic.Revision++
	if err = n.entry.Apply(traffic.Revision-1, traffic); err != nil {
		return nil, err
	}
	n.execution.Store(true)
	n.ingress.Store(true)
	n.core.StartBackground()
	return n, nil
}
