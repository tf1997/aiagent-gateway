package process

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/service"
)

type ProcessReport struct {
	rollout.Report
	HandoffID          string               `json:"handoff_id,omitempty"`
	Decision           handoff.Decision     `json:"handoff_decision"`
	SourcePID          int                  `json:"source_pid,omitempty"`
	SampleSince        time.Time            `json:"sample_since,omitempty"`
	SourceIdentity     p.Identity           `json:"source_identity"`
	TargetIdentity     p.Identity           `json:"target_identity"`
	Committed          bool                 `json:"committed"`
	LastConfirmed      string               `json:"last_confirmed_phase"`
	Timeline           map[string]time.Time `json:"phase_times"`
	SnapshotBytes      int64                `json:"snapshot_bytes"`
	HandoffMillis      int64                `json:"handoff_milliseconds"`
	State              shared.BarrierStatus `json:"state_status"`
	StateQueue         shared.BarrierStatus `json:"state_queue"`
	AllocationQueue    shared.BarrierStatus `json:"allocation_queue"`
	IngressActive      int                  `json:"local_ingress_active"`
	LiveIdentity       p.Identity           `json:"live_identity"`
	StorageHealthy     bool                 `json:"storage_healthy"`
	ControllerOwner    string               `json:"controller_owner"`
	MainPID            int                  `json:"systemd_main_pid,omitempty"`
	LifecycleConfirmed time.Time            `json:"lifecycle_confirmed_at,omitempty"`
}

type ProcessController struct {
	mu        sync.Mutex
	service   *Service
	report    ProcessReport
	committed bool
	visible   atomic.Pointer[ProcessReport]
}

func readCurrentDecision(n *Gateway) (handoff.Decision, error) {
	return handoff.ReadDecision(n.dataDir)
}

func processTerminal(phase string) bool {
	return phase == "" || phase == "COMPLETED" || phase == "ROLLED_BACK" || phase == "FAILED"
}

func openProcessController(s *Service, inherited bool) (*ProcessController, error) {
	c := &ProcessController{service: s}
	n := s.node.Load()
	if raw := n.releaseJournal.Snapshot()["process/report"]; raw != nil {
		if err := json.Unmarshal(raw, &c.report); err != nil {
			return nil, err
		}
	}
	r := &c.report
	if inherited {
		d := n.owner.Decision()
		if d.Phase != handoff.Committed || d.Rollout != r.ID || d.ID != r.HandoffID || r.Candidate == nil || *r.Candidate != n.target {
			return nil, fmt.Errorf("INVALID_INHERITED_CONTROLLER")
		}
		old := r.Stable
		r.Retiring = &old
		r.Stable = n.target
		r.Candidate = nil
		r.Intent = nil
		r.Revision = n.entry.Traffic().Revision
		r.Decision = d
		r.Committed = true
		r.TargetIdentity = n.Identity()
		r.SnapshotBytes = d.Snapshot.Bytes
		r.HandoffMillis = time.Since(r.Since).Milliseconds()
		c.phase("ACTIVATING")
	} else {
		if r.ID == "" {
			r.Report = rollout.Report{ID: p.ID("rollout_"), Started: time.Now(), Policy: s.opts.Policy}
			c.phase("COMPLETED")
		} else if !processTerminal(r.Phase) {
			if n.previous.ColdStart == r.ArtifactRelease() {
				c.phase("ACTIVATING")
			} else {
				r.Reason = "process_restarted_before_commit"
				c.phase("ROLLED_BACK")
			}
		}
		r.Stable = n.target
		r.Candidate = nil
		r.Retiring = nil
		r.Intent = nil
		r.SourcePID = 0
		r.Revision = n.entry.Traffic().Revision
		r.Decision = n.previous
		r.Committed = (n.previous.Phase == handoff.Committed || n.previous.Phase == handoff.Completed) && n.previous.Rollout == r.ID
		if r.Artifact == "" {
			r.Artifact = n.target.Artifact
		}
		if err := n.shared.Freeze(!processTerminal(r.Phase)); err != nil {
			return nil, err
		}
	}
	return c, c.save()
}

func (r ProcessReport) ArtifactRelease() string {
	if r.Candidate != nil && r.Candidate.Artifact == r.Artifact {
		return r.Candidate.Release
	}
	if r.Stable.Artifact == r.Artifact {
		return r.Stable.Release
	}
	return ""
}

func (c *ProcessController) save() error {
	raw, err := json.Marshal(c.report)
	if err != nil {
		return err
	}
	if err = c.service.node.Load().releaseJournal.Replace("process/", map[string]json.RawMessage{"process/report": raw}); err != nil {
		return err
	}
	var published ProcessReport
	if err = json.Unmarshal(raw, &published); err != nil {
		return err
	}
	c.visible.Store(&published)
	return nil
}

func (c *ProcessController) Report() ProcessReport {
	// Status must remain readable while a transition waits for snapshots,
	// drainage or lifecycle acknowledgements. Publish only persisted reports.
	raw, _ := json.Marshal(c.visible.Load())
	var result ProcessReport
	_ = json.Unmarshal(raw, &result)
	n := c.service.node.Load()
	result.State = n.shared.BarrierStatus()
	if c.service.queueStatus != nil {
		result.StateQueue, result.AllocationQueue = c.service.queueStatus()
	}
	result.IngressActive = n.entry.LocalActive()
	result.LiveIdentity = n.Identity()
	result.StorageHealthy = n.StorageHealthy()
	if d, err := readCurrentDecision(n); err == nil {
		result.Decision = d
		result.Committed = (d.Phase == handoff.Committed || d.Phase == handoff.Completed) && d.Rollout == result.ID
	}
	if result.StorageHealthy {
		result.ControllerOwner = n.target.Incarnation
	}
	return result
}

func (c *ProcessController) phase(phase string) {
	c.report.Phase = phase
	c.report.Since = time.Now()
	if c.report.Timeline == nil {
		c.report.Timeline = map[string]time.Time{}
	}
	c.report.Timeline[phase] = c.report.Since
	if phase != "RECOVERY_REQUIRED" {
		c.report.LastConfirmed = phase
	}
	if phase == "VERIFYING" || phase == "OBSERVING" {
		c.report.SampleSince = c.report.Since
	}
}

func (c *ProcessController) Begin(artifact string, policy p.Policy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	policy.Handoff = policy.Handoff.Effective()
	if err := policy.Validate(); err != nil {
		return err
	}
	n := c.service.node.Load()
	if !n.Ready() {
		return fmt.Errorf("OWNER_NOT_READY")
	}
	if !processTerminal(c.report.Phase) {
		if c.report.Artifact == artifact && c.report.Phase != "RECOVERY_REQUIRED" {
			return nil
		}
		return fmt.Errorf("ROLLOUT_IN_PROGRESS")
	}
	if artifact == n.target.Artifact {
		return nil
	}
	if err := service.ValidateArtifact(c.service.opts.ReleasesDir, artifact, c.service.opts.OwnerUID); err != nil {
		return err
	}
	if err := n.shared.ReconfigureQueue(shared.QueueLimits{Requests: policy.Handoff.QueueRequests, Bytes: policy.Handoff.QueueBytes, Wait: time.Duration(policy.Handoff.StateWaitMillis) * time.Millisecond}); err != nil {
		return err
	}
	n.handoffMu.Lock()
	n.policy = policy.Handoff
	n.handoffMu.Unlock()
	c.report = ProcessReport{Report: rollout.Report{ID: p.ID("rollout_"), Stable: n.target, Artifact: artifact, Policy: policy, Started: time.Now(), Revision: n.entry.Traffic().Revision}, HandoffID: p.ID("handoff_"), SourcePID: os.Getpid(), SourceIdentity: n.Identity()}
	c.phase("PREPARING")
	return c.save()
}

func (c *ProcessController) Resume() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.report.Phase != "RECOVERY_REQUIRED" || c.report.ResumePhase == "" {
		return fmt.Errorf("NOTHING_TO_RESUME")
	}
	sampleSince := c.report.SampleSince
	c.phase(c.report.ResumePhase)
	c.report.SampleSince = sampleSince
	c.report.ResumePhase = ""
	c.report.Reason = ""
	return c.save()
}

func (c *ProcessController) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, err := readCurrentDecision(c.service.node.Load())
	if err != nil {
		return err
	}
	if c.committed || d.Phase == handoff.Committed || d.Phase == handoff.Completed && d.Rollout == c.report.ID {
		return handoff.ErrCommitted
	}
	if processTerminal(c.report.Phase) || c.report.Phase == "ACTIVATING" {
		return fmt.Errorf("NO_ROLLBACK_TARGET")
	}
	c.report.Reason = "operator_requested"
	c.phase("ROLLING_BACK")
	return c.save()
}

func (c *ProcessController) recovery(reason, phase string) error {
	c.report.Reason = reason
	c.report.ResumePhase = phase
	c.phase("RECOVERY_REQUIRED")
	return c.save()
}

func (c *ProcessController) rollback(reason string) {
	c.report.Reason = reason
	c.phase("ROLLING_BACK")
}

// route durably publishes an intent before changing the allocator revision.
// Recovery can repeat this metadata transition, never a business request.
func (c *ProcessController) route(percent int, finished bool) error {
	n := c.service.node.Load()
	r := &c.report
	current := n.entry.Traffic()
	if r.Intent == nil {
		next := p.Traffic{Revision: current.Revision + 1, Rollout: r.ID, Stable: r.Stable, Candidate: r.Candidate, Percent: percent, Frozen: true}
		if finished {
			next.Candidate = nil
			next.Percent = 0
			next.Rollout = ""
			next.Frozen = false
		}
		same := next
		same.Revision = current.Revision
		a, _ := json.Marshal(same)
		b, _ := json.Marshal(current)
		if string(a) == string(b) {
			r.Revision = current.Revision
			return nil
		}
		r.Intent = &next
		if err := c.save(); err != nil {
			return err
		}
	}
	intent := *r.Intent
	if current.Revision == intent.Revision {
		a, _ := json.Marshal(current)
		b, _ := json.Marshal(intent)
		if string(a) != string(b) {
			return fmt.Errorf("TRAFFIC_INTENT_CONFLICT")
		}
	} else if current.Revision+1 == intent.Revision {
		if err := n.entry.Apply(current.Revision, intent); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("TRAFFIC_REVISION_CONFLICT")
	}
	r.Revision = intent.Revision
	r.Intent = nil
	return c.save()
}

func (c *ProcessController) gate(ctx context.Context, min int, direct bool) (bool, string) {
	n := c.service.node.Load()
	r := &c.report
	if n.candidate == nil || r.Candidate == nil {
		return false, "candidate_missing"
	}
	id, err := n.candidate.Identity(ctx)
	if err != nil || !id.Ready || id.Incarnation != r.Candidate.Incarnation || id.ConfigRevision != n.Identity().ConfigRevision {
		return false, "candidate_unhealthy"
	}
	return c.resultGate(min, direct)
}

// resultGate only reads owner evidence, so it can also run with state calls
// sealed immediately before the source yields its writer lock.
func (c *ProcessController) resultGate(min int, direct bool) (bool, string) {
	n := c.service.node.Load()
	r := &c.report
	stats := n.entry.Stats(r.SampleSince, r.Candidate.Incarnation)
	r.Stats = stats
	if stats.Lost {
		return false, "result_evidence_lost"
	}
	if stats.Failures > r.Policy.MaxFailures {
		return false, "proxy_verification_failed"
	}
	if r.Policy.AutoProfiles {
		for profile, counts := range stats.Counts {
			if counts.Success == 0 || profile != "http" && profile != "sse" && profile != "a2a" && profile != "a2a_sse" {
				continue
			}
			found := false
			for _, v := range r.Policy.Profiles {
				if v == profile {
					found = true
				}
			}
			if !found {
				r.Policy.Profiles = append(r.Policy.Profiles, profile)
			}
		}
	}
	for _, counts := range stats.Counts {
		if counts.Unknown > 0 {
			return false, ""
		}
	}
	for _, profile := range r.Policy.Profiles {
		if stats.Counts[profile].Success < min {
			return false, ""
		}
		if direct && stats.IngressCounts[profile].Success < min {
			return false, ""
		}
	}
	return true, ""
}

func (c *ProcessController) Tick(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.service
	n := s.node.Load()
	r := &c.report
	if processTerminal(r.Phase) || r.Phase == "RECOVERY_REQUIRED" {
		return nil
	}
	if c.committed {
		return c.finishTransfer(ctx)
	}
	if !n.StorageHealthy() {
		return fmt.Errorf("OWNER_STORAGE_UNAVAILABLE")
	}
	switch r.Phase {
	case "PREPARING":
		if err := n.shared.Freeze(true); err != nil {
			return c.recovery(err.Error(), "PREPARING")
		}
		if r.Policy.AutoProfiles {
			policy, err := service.PolicyForConfig(r.Policy, filepath.Join(n.dataDir, "config.json"))
			if err != nil {
				return c.recovery(err.Error(), "PREPARING")
			}
			r.Policy = policy
		}
		c.phase("STARTING")
	case "STARTING":
		startCtx, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.ReadinessSeconds)*time.Second)
		candidate, err := n.StartCandidate(startCtx, CandidateOptions{ConfigPath: s.opts.ConfigPath, Artifact: r.Artifact, ReleasesDir: s.opts.ReleasesDir, OwnerUID: s.opts.OwnerUID, Listeners: s.sockets, MaxSnapshotBytes: n.policy.SnapshotBytes, MaxDeltaBytes: n.policy.DeltaBytes, Output: s.opts.Output, Service: true, Unit: s.opts.Unit})
		cancel()
		if err != nil {
			c.rollback("candidate_start_failed: " + err.Error())
			break
		}
		target := candidate.Target()
		r.Candidate = &target
		r.TargetIdentity, _ = candidate.Identity(ctx)
		if err = c.route(r.Policy.CanaryPercent, false); err != nil {
			return c.recovery(err.Error(), "STARTING")
		}
		c.phase("VERIFYING")
	case "VERIFYING":
		ok, why := c.gate(ctx, r.Policy.MinSuccess, false)
		if why != "" {
			c.rollback(why)
			break
		}
		elapsed := time.Since(r.Since)
		if ok && elapsed >= time.Duration(r.Policy.VerifyMinSeconds)*time.Second {
			c.phase("PROMOTING")
		} else if elapsed >= time.Duration(r.Policy.VerifyTimeoutSeconds)*time.Second {
			c.rollback("insufficient_successful_proxy_samples")
		}
	case "PROMOTING":
		if err := c.route(100, false); err != nil {
			return c.recovery(err.Error(), "PROMOTING")
		}
		if err := n.candidate.Accept(ctx); err != nil {
			c.rollback("candidate_ingress_unavailable")
			break
		}
		s.pausePublic()
		c.phase("OBSERVING")
	case "OBSERVING":
		ok, why := c.gate(ctx, r.Policy.ObserveMinSuccess, true)
		if why != "" {
			c.rollback(why)
			break
		}
		if time.Since(r.Since) >= time.Duration(r.Policy.ObserveSeconds)*time.Second {
			if ok {
				c.phase("DRAINING")
			} else {
				return c.recovery("direct_ingress_observation_incomplete", "OBSERVING")
			}
		}
	case "DRAINING":
		ok, why := c.gate(ctx, 0, false)
		if why != "" {
			c.rollback(why)
			break
		}
		if ok && n.entry.LocalActive() == 0 && n.Identity().Inflight == 0 {
			r.SourceIdentity = n.Identity()
			c.phase("HANDOFF")
		} else if time.Since(r.Since) >= time.Duration(r.Policy.DrainSeconds)*time.Second {
			if !ok {
				return c.recovery("result_observation_incomplete", "DRAINING")
			}
			return c.recovery("source_drain_timeout", "DRAINING")
		}
	case "HANDOFF":
		ok, why := c.gate(ctx, 0, false)
		if why != "" {
			c.rollback(why)
			break
		}
		if !ok {
			return c.recovery("result_observation_incomplete", "DRAINING")
		}
		transition, err := n.transferHandoff(ctx, n.candidate, r.HandoffID, r.ID, n.policy.SnapshotBytes, func() error {
			ok, why := c.resultGate(0, false)
			if why != "" {
				return fmt.Errorf("%s", why)
			}
			if !ok {
				return fmt.Errorf("result_observation_incomplete")
			}
			return nil
		})
		if transition.Source != nil {
			s.setOwner(transition.Source)
			if err != nil {
				c.rollback("handoff_aborted: " + err.Error())
				return c.save()
			}
		}
		if transition.Decision.Phase == handoff.Committed || transition.Decision.Phase == handoff.Completed {
			c.committed = true
			r.Committed = true
			r.Decision = transition.Decision
			return c.finishTransfer(ctx)
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("HANDOFF_OUTCOME_UNRESOLVED")
	case "ACTIVATING":
		// Reparenting is kernel evidence that this exact source has exited.
		// A cold restart starts only after the service manager removed its group.
		if r.SourcePID != 0 && os.Getppid() == r.SourcePID {
			return nil
		}
		if !n.Ready() {
			return fmt.Errorf("COMMITTED_OWNER_NOT_READY")
		}
		if err := c.route(0, true); err != nil {
			return err
		}
		if err := n.shared.Freeze(false); err != nil {
			return err
		}
		if d := n.owner.Decision(); d.Phase == handoff.Committed {
			if err := n.owner.Complete(d.ID); err != nil {
				return err
			}
		}
		r.Retiring = nil
		r.Decision = n.owner.Decision()
		c.phase("COMPLETED")
	case "ROLLING_BACK":
		if err := c.route(0, false); err != nil {
			return c.recovery(err.Error(), "ROLLING_BACK")
		}
		if err := s.acceptPublic(); err != nil {
			return c.recovery(err.Error(), "ROLLING_BACK")
		}
		if candidate := n.candidate; candidate != nil {
			select {
			case <-candidate.Done():
			default:
				if err := candidate.Pause(ctx); err != nil {
					return c.recovery(err.Error(), "ROLLING_BACK")
				}
				if n.entry.IngressActive(candidate.target.Incarnation) > 0 || n.shared.Count(candidate.target.Incarnation) > 0 {
					if time.Since(r.Since) >= time.Duration(r.Policy.DrainSeconds)*time.Second {
						return c.recovery("candidate_drain_timeout", "ROLLING_BACK")
					}
					return nil
				}
			}
			if err := candidate.Stop(ctx, n); err != nil {
				return c.recovery(err.Error(), "ROLLING_BACK")
			}
		}
		if d := n.owner.Decision(); d.Phase == handoff.Prepared || d.Phase == handoff.Sealed {
			if err := n.AbortHandoff(d.ID); err != nil {
				return err
			}
		}
		r.Candidate = nil
		if err := c.route(0, true); err != nil {
			return err
		}
		if err := n.shared.Freeze(false); err != nil {
			return err
		}
		c.phase("ROLLED_BACK")
	default:
		return fmt.Errorf("UNKNOWN_PROCESS_PHASE: %s", r.Phase)
	}
	return c.save()
}

func (c *ProcessController) finishTransfer(ctx context.Context) error {
	s := c.service
	n := s.node.Load()
	candidate := n.candidate
	if candidate == nil {
		return fmt.Errorf("COMMITTED_PROCESS_MISSING")
	}
	s.pauseControl()
	activate, cancel := context.WithTimeout(ctx, time.Duration(c.report.Policy.Handoff.ActivationSeconds)*time.Second)
	defer cancel()
	if s.opts.Unit != "" {
		if err := (Notifier{Socket: os.Getenv("NOTIFY_SOCKET")}).MainPID(candidate.PID()); err != nil {
			return err
		}
	}
	if err := candidate.Takeover(activate); err != nil {
		return err
	}
	s.retire()
	return nil
}

func (c *ProcessController) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		switch {
		case r.Method == "GET" && r.URL.Path == "/control/v1/rollout":
		case r.Method == "POST" && r.URL.Path == "/control/v1/rollout":
			var q struct {
				Artifact string   `json:"artifact"`
				Policy   p.Policy `json:"policy"`
			}
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			d.DisallowUnknownFields()
			if d.Decode(&q) != nil || d.Decode(new(any)) != io.EOF {
				w.WriteHeader(400)
				return
			}
			err = c.Begin(q.Artifact, q.Policy)
		case r.Method == "POST" && r.URL.Path == "/control/v1/rollout/resume":
			err = c.Resume()
		case r.Method == "POST" && r.URL.Path == "/control/v1/rollout/rollback":
			err = c.Rollback()
		default:
			w.WriteHeader(404)
			return
		}
		if err != nil {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(c.Report())
	})
}
