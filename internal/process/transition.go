package process

import (
	"context"
	"errors"
	"fmt"
	"time"

	"aiagent-gateway/internal/handoff"
)

// Transition describes confirmed ownership, not completion of a rollout.
// Source is non-nil only while it retains or has reacquired all writer locks.
// Activated confirms candidate HTTP activation; MainPID, control ownership and
// the source's actual exit must still be confirmed by the lifecycle controller.
type Transition struct {
	Decision  handoff.Decision
	Source    *Gateway
	Activated bool
}

// TransferHandoff is called after the controller has stopped advancing and the
// source's accepted requests have drained. It validates the candidate before
// yielding, and resolves any uncertain commit through the ownership protocol.
// A returned error never authorizes callers to reopen the original runtime.
func (n *Gateway) TransferHandoff(ctx context.Context, candidate *CandidateProcess, id, rollout string, maxBytes int) (Transition, error) {
	result := Transition{Source: n}
	if !n.StorageHealthy() {
		return Transition{}, fmt.Errorf("SOURCE_NOT_READY")
	}
	if candidate == nil {
		return result, fmt.Errorf("MISSING_CANDIDATE")
	}
	n.handoffMu.Lock()
	owned := n.candidate == candidate
	n.handoffMu.Unlock()
	if !owned {
		return result, fmt.Errorf("CANDIDATE_NOT_OWNED")
	}
	if !candidate.hasFollow(id) && n.owner.Decision().Phase != handoff.Sealed {
		if _, err := candidate.FollowHandoff(ctx, n, id, rollout, maxBytes); err != nil {
			result.Decision = n.owner.Decision()
			return result, err
		}
	}
	// Drain the accumulated operation prefix while candidate calls remain open.
	// The final barrier only sends changes made after this acknowledgement.
	if candidate.operationStream && n.owner.Decision().Phase != handoff.Sealed {
		if _, err := candidate.FollowHandoff(ctx, n, id, rollout, maxBytes); err != nil {
			result.Decision = n.owner.Decision()
			return result, err
		}
	}
	quiesce, cancelQuiesce := context.WithTimeout(ctx, time.Duration(n.policy.BarrierMillis)*time.Millisecond)
	_, quiesceErr := candidate.channel.call(quiesce, peerMessage{Action: "quiesce"})
	cancelQuiesce()
	if quiesceErr != nil {
		return result, quiesceErr
	}
	defer func() {
		if n.StorageHealthy() {
			resume, cancel := context.WithTimeout(context.Background(), time.Duration(n.policy.StateWaitMillis)*time.Millisecond)
			defer cancel()
			_, _ = candidate.channel.call(resume, peerMessage{Action: "resume_calls"})
		}
	}()
	barrierCtx, cancel := context.WithTimeout(ctx, time.Duration(n.policy.BarrierMillis)*time.Millisecond)
	defer cancel()
	d, err := n.PrepareHandoff(barrierCtx, id, rollout, handoff.Identity{Incarnation: candidate.target.Incarnation, Release: candidate.target.Release}, maxBytes)
	result.Decision = n.owner.Decision()
	if err != nil {
		return result, err
	}
	if err = candidate.ValidateHandoff(barrierCtx, n, d, maxBytes); err != nil {
		abortErr := n.AbortHandoff(d.ID)
		result.Decision = n.owner.Decision()
		if abortErr != nil {
			n.StopIngress()
		}
		return result, errors.Join(err, abortErr)
	}
	if err = barrierCtx.Err(); err != nil {
		abortErr := n.AbortHandoff(d.ID)
		result.Decision = n.owner.Decision()
		if abortErr != nil {
			n.StopIngress()
		}
		return result, errors.Join(err, abortErr)
	}
	if err = n.YieldHandoff(d); err != nil {
		// Closing a domain can fail after other handles have already closed.
		// Never return a partially closed source as an available writer.
		result.Source = nil
		result.Decision = n.owner.Decision()
		if n.StorageHealthy() {
			result.Source = n // a conflicting decision can reject before Close
		}
		return result, err
	}
	result.Source = nil
	if committed, commitErr := candidate.Commit(barrierCtx, d); commitErr == nil {
		result.Decision = committed
		activation, stop := context.WithTimeout(ctx, time.Duration(n.policy.ActivationSeconds)*time.Second)
		defer stop()
		err = candidate.Activate(activation)
		if err == nil {
			result.Activated = true
			return result, nil
		}
		ctx = activation // recovery cannot restart the activation time budget
	} else {
		err = commitErr
	}
	resolved, recoveryErr := n.ResolveHandoff(ctx, candidate, d, maxBytes)
	if recoveryErr == nil && resolved.Activated {
		return resolved, nil
	}
	return resolved, errors.Join(err, recoveryErr)
}

// ResolveHandoff can be retried with a fresh deadline after an uncertain
// transition. A busy writer lock leaves Source nil. Only a locked, durable
// ABORTED decision can restore the source, and COMMITTED always selects B.
func (n *Gateway) ResolveHandoff(ctx context.Context, candidate *CandidateProcess, sealed handoff.Decision, maxBytes int) (Transition, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(n.policy.ActivationSeconds)*time.Second)
	defer cancel()
	result := Transition{}
	if candidate == nil || sealed.Validate() != nil || sealed.Phase != handoff.Sealed || sealed.Source != (handoff.Identity{Incarnation: n.identity.Incarnation, Release: n.identity.Release}) || sealed.Target != (handoff.Identity{Incarnation: candidate.target.Incarnation, Release: candidate.target.Release}) {
		return result, handoff.ErrConflict
	}
	d, err := handoff.ReadDecision(n.dataDir)
	result.Decision = d
	if err != nil {
		return result, err
	}
	if d.Phase == handoff.Committed || d.Phase == handoff.Completed {
		d, err = candidate.RecoverCommitted(ctx, n.dataDir, sealed)
		result.Decision, result.Activated = d, err == nil
		return result, err
	}
	aborted := sealed
	aborted.Phase = handoff.Aborted
	if d == aborted && n.owner.Decision() == d && n.StorageHealthy() {
		// The caller retained Source from an earlier recovery but its peer
		// acknowledgement failed. Reuse those locks; do not reopen writers.
		result.Source = n
		return result, candidate.RestoreSource(ctx, n, sealed)
	}
	if d != sealed {
		return result, handoff.ErrConflict
	}
	restored, err := openHandoffGateway(ctx, n.config, n.target, n.identity, sealed, maxBytes, nil, nil, true, n.policy)
	if errors.Is(err, handoff.ErrCommitted) {
		// The candidate won the lock after our read. Reconcile its decision;
		// never turn this race into a source activation.
		d, err = candidate.RecoverCommitted(ctx, n.dataDir, sealed)
		result.Decision, result.Activated = d, err == nil
		return result, err
	}
	if err != nil {
		return result, err
	}
	result.Source, result.Decision = restored, restored.owner.Decision()
	if err = candidate.RestoreSource(ctx, restored, sealed); err != nil {
		select {
		case <-candidate.Done():
			// A confirmed dead executor cannot finish its leases. Retire it
			// against the restored image before allowing another candidate.
			return result, candidate.Stop(ctx, restored)
		default:
			return result, err
		}
	}
	return result, nil
}
