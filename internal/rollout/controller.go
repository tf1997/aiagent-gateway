// Package rollout implements the product-independent automatic release state machine.
package rollout

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Driver interface {
	Start(context.Context, string, string) (p.Target, error)
	Identity(context.Context, p.Target) (p.Identity, error)
	Stop(context.Context, p.Target) error
	Freeze(context.Context, bool) error
}
type Entry interface {
	Traffic(context.Context) (p.Traffic, error)
	Apply(context.Context, uint64, p.Traffic) error
	Stats(context.Context, time.Time, string) (p.Statistics, error)
}
type Report struct {
	Retiring    *p.Target    `json:"retiring,omitempty"`
	ID          string       `json:"rollout_id"`
	Phase       string       `json:"phase"`
	Stable      p.Target     `json:"stable"`
	Candidate   *p.Target    `json:"candidate,omitempty"`
	Artifact    string       `json:"artifact,omitempty"`
	Policy      p.Policy     `json:"policy"`
	Started     time.Time    `json:"started_at"`
	Since       time.Time    `json:"phase_since"`
	Reason      string       `json:"reason,omitempty"`
	Stats       p.Statistics `json:"statistics"`
	Intent      *p.Traffic   `json:"traffic_intent,omitempty"`
	Revision    uint64       `json:"routing_revision"`
	ResumePhase string       `json:"resume_phase,omitempty"`
}
type Controller struct {
	mu     sync.Mutex
	j      *state.Journal
	driver Driver
	entry  Entry
	report Report
	now    func() time.Time
}

func Open(j *state.Journal, d Driver, e Entry) (*Controller, error) {
	c := &Controller{j: j, driver: d, entry: e, now: time.Now}
	if raw := j.Snapshot()["controller/report"]; raw != nil {
		if err := json.Unmarshal(raw, &c.report); err != nil {
			return nil, err
		}
	}
	return c, nil
}
func (c *Controller) save() error {
	b, _ := json.Marshal(c.report)
	return c.j.Replace("controller/", map[string]json.RawMessage{"controller/report": b})
}
func (c *Controller) Report() Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	raw, _ := json.Marshal(c.report)
	var r Report
	_ = json.Unmarshal(raw, &r)
	return r
}
func terminal(phase string) bool {
	return phase == "COMPLETED" || phase == "ROLLED_BACK" || phase == "FAILED" || phase == ""
}
func (c *Controller) Begin(artifact string, policy p.Policy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := policy.Validate(); err != nil {
		return err
	}
	if !terminal(c.report.Phase) {
		return fmt.Errorf("rollout %s is %s", c.report.ID, c.report.Phase)
	}
	if artifact == "" {
		return fmt.Errorf("artifact required")
	}
	now := c.now()
	c.report = Report{ID: p.ID("rollout_"), Phase: "PREPARING", Stable: c.report.Stable, Artifact: artifact, Policy: policy, Started: now, Since: now}
	return c.save()
}
func (c *Controller) phase(phase string) { c.report.Phase = phase; c.report.Since = c.now() }
func (c *Controller) recoverRequired(reason, resume string) error {
	c.report.Reason = reason
	c.report.ResumePhase = resume
	c.phase("RECOVERY_REQUIRED")
	return c.save()
}
func (c *Controller) rollback(reason string) { c.report.Reason = reason; c.phase("ROLLING_BACK") }
func (c *Controller) Resume() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.report.Phase != "RECOVERY_REQUIRED" || c.report.ResumePhase == "" {
		return fmt.Errorf("nothing to resume")
	}
	c.phase(c.report.ResumePhase)
	c.report.ResumePhase = ""
	return c.save()
}
func (c *Controller) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.report.Candidate == nil || c.report.Stable.Incarnation == "" {
		return fmt.Errorf("no rollback target")
	}
	c.rollback("operator_requested")
	return c.save()
}

// route persists an intent before applying it; retries reconcile actual revision.
func (c *Controller) route(ctx context.Context, percent int, commit bool) error {
	current, err := c.entry.Traffic(ctx)
	if err != nil {
		return err
	}
	if c.report.Intent == nil {
		next := current
		next.Revision++
		next.Rollout = c.report.ID
		next.Stable = c.report.Stable
		next.Candidate = c.report.Candidate
		next.Percent = percent
		next.Frozen = true
		if commit {
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
			c.report.Revision = current.Revision
			return nil
		}
		c.report.Intent = &next
		if err = c.save(); err != nil {
			return err
		}
	}
	intent := *c.report.Intent
	if current.Revision == intent.Revision {
		a, _ := json.Marshal(current)
		b, _ := json.Marshal(intent)
		if string(a) != string(b) {
			return fmt.Errorf("traffic intent conflict")
		}
	} else if current.Revision+1 == intent.Revision {
		if err = c.entry.Apply(ctx, current.Revision, intent); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("traffic revision diverged")
	}
	c.report.Revision = intent.Revision
	c.report.Intent = nil
	return c.save()
}
func (c *Controller) ready(ctx context.Context, t p.Target) (bool, error) {
	id, err := c.driver.Identity(ctx, t)
	if err != nil {
		return false, err
	}
	if id.Release != t.Release || id.Incarnation != t.Incarnation || id.Protocol != p.Version || id.StateProtocol != 1 {
		return false, fmt.Errorf("worker identity/protocol changed")
	}
	return id.Ready, nil
}
func (c *Controller) gate(ctx context.Context, min int) (bool, string) {
	r := &c.report
	ok, err := c.ready(ctx, *r.Candidate)
	if err != nil || !ok {
		return false, "candidate_unhealthy"
	}
	stats, err := c.entry.Stats(ctx, r.Since, r.Candidate.Incarnation)
	if err != nil {
		return false, "results_unavailable"
	}
	r.Stats = stats
	if r.Policy.AutoProfiles {
		for profile, counts := range stats.Counts {
			if counts.Success == 0 {
				continue
			}
			if profile != "http" && profile != "sse" && profile != "a2a" && profile != "a2a_sse" {
				continue
			}
			found := false
			for _, required := range r.Policy.Profiles {
				if required == profile {
					found = true
				}
			}
			if !found {
				r.Policy.Profiles = append(r.Policy.Profiles, profile)
			}
		}
	}

	if stats.Lost {
		return false, "result_evidence_lost"
	}
	if stats.Failures > r.Policy.MaxFailures {
		return false, "proxy_verification_failed"
	}
	for _, counts := range stats.Counts {
		if counts.Unknown > 0 {
			return false, ""
		}
	}
	for _, profile := range r.Policy.Profiles {
		n := stats.Counts[profile]
		if n.Success < min || n.Unknown > 0 {
			return false, ""
		}
	}
	return true, ""
}
func (c *Controller) Tick(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := &c.report
	if c.j.Healthy() != nil {
		return c.j.Healthy()
	}
	if terminal(r.Phase) {
		if r.Stable.Artifact != "" {
			id, err := c.driver.Identity(ctx, r.Stable)
			if err != nil || !id.Ready || id.Incarnation != r.Stable.Incarnation {
				return c.recoverStable(ctx)
			}
		}
		return nil
	}
	if r.Phase == "RECOVERY_REQUIRED" {
		return nil
	}
	switch r.Phase {
	case "RECOVERING":
		return c.recoverStable(ctx)
	case "PREPARING":
		if r.Stable.Incarnation != "" {
			ok, err := c.ready(ctx, r.Stable)
			if err != nil || !ok {
				r.Reason = "stable_unhealthy"
				c.phase("FAILED")
				return c.save()
			}
		}
		if err := c.driver.Freeze(ctx, true); err != nil {
			return c.recoverRequired(err.Error(), "PREPARING")
		}
		c.phase("STARTING")
		return c.save()
	case "STARTING":
		slot := "a"
		if r.Stable.Instance == "a" {
			slot = "b"
		}
		// Driver Start is idempotent for a release/slot and returns the running identity.
		startCtx, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.ReadinessSeconds)*time.Second)
		candidate, err := c.driver.Start(startCtx, r.Artifact, slot)
		cancel()
		if err != nil {
			r.Reason = "candidate_start_failed: " + err.Error()
			if r.Stable.Incarnation == "" {
				c.phase("FAILED")
				_ = c.driver.Freeze(ctx, false)
			} else {
				c.rollback(r.Reason)
			}
			return c.save()
		}
		r.Candidate = &candidate
		if r.Stable.Incarnation == "" {
			r.Stable = candidate
			r.Candidate = nil
			if err = c.route(ctx, 0, true); err != nil {
				return c.recoverRequired(err.Error(), "STARTING")
			}
			_ = c.driver.Freeze(ctx, false)
			c.phase("COMPLETED")
			return c.save()
		}
		a, e1 := c.driver.Identity(ctx, r.Stable)
		b, e2 := c.driver.Identity(ctx, candidate)
		if e1 != nil || e2 != nil || a.ConfigRevision != b.ConfigRevision || !a.Ready || !b.Ready {
			c.rollback("configuration_or_readiness_mismatch")
			return c.save()
		}
		if err = c.route(ctx, r.Policy.CanaryPercent, false); err != nil {
			return c.recoverRequired(err.Error(), "STARTING")
		}
		c.phase("VERIFYING")
	case "VERIFYING":
		ok, why := c.gate(ctx, r.Policy.MinSuccess)
		if why != "" {
			c.rollback(why)
			break
		}
		elapsed := c.now().Sub(r.Since)
		if ok && elapsed >= time.Duration(r.Policy.VerifyMinSeconds)*time.Second {
			c.phase("PROMOTING")
		} else if elapsed >= time.Duration(r.Policy.VerifyTimeoutSeconds)*time.Second {
			c.rollback("insufficient_successful_proxy_samples")
		}
	case "PROMOTING":
		if err := c.route(ctx, 100, false); err != nil {
			return c.recoverRequired(err.Error(), "PROMOTING")
		}
		c.phase("OBSERVING")
	case "OBSERVING":
		ok, why := c.gate(ctx, r.Policy.ObserveMinSuccess)
		if why != "" {
			c.rollback(why)
			break
		}
		if c.now().Sub(r.Since) >= time.Duration(r.Policy.ObserveSeconds)*time.Second {
			if ok {
				c.phase("DRAINING")
			} else {
				c.rollback("observation_evidence_incomplete")
			}
		}
	case "DRAINING":
		_, why := c.gate(ctx, 0)
		if why != "" {
			c.rollback(why)
			break
		}
		old, err := c.driver.Identity(ctx, r.Stable)
		if err != nil {
			return c.recoverRequired("cannot_confirm_old_worker_drained", "DRAINING")
		}
		stats, err := c.entry.Stats(ctx, r.Started, r.Stable.Incarnation)
		if err != nil {
			return err
		}
		if old.Inflight == 0 && stats.Active == 0 {
			oldTarget := r.Stable
			r.Retiring = &oldTarget
			r.Stable = *r.Candidate
			r.Candidate = nil
			c.phase("COMMITTING")

		} else if c.now().Sub(r.Since) >= time.Duration(r.Policy.DrainSeconds)*time.Second {
			return c.recoverRequired("old_worker_drain_timeout", "DRAINING")
		}
	case "COMMITTING":
		if r.Retiring != nil {
			if err := c.driver.Stop(ctx, *r.Retiring); err != nil {
				return c.recoverRequired(err.Error(), "COMMITTING")
			}
			r.Retiring = nil
			if err := c.save(); err != nil {
				return err
			}
		}
		if err := c.route(ctx, 0, true); err != nil {
			return err
		}
		if err := c.driver.Freeze(ctx, false); err != nil {
			return err
		}
		c.phase("COMPLETED")
	case "ROLLING_BACK":
		if r.Stable.Incarnation == "" {
			c.phase("FAILED")
			break
		}
		ok, err := c.ready(ctx, r.Stable)
		if (err != nil || !ok) && r.Stable.Artifact != "" {
			old := r.Stable
			next, e := c.driver.Start(ctx, old.Artifact, old.Instance)
			if e == nil {
				if d, yes := c.driver.(interface {
					Retire(context.Context, p.Target) error
				}); yes && next.Incarnation != old.Incarnation {
					e = d.Retire(ctx, old)
				}
				if e == nil {
					r.Stable = next
					ok, err = c.ready(ctx, next)
				}
			}
		}
		if err != nil || !ok {
			return c.recoverRequired("rollback_target_unavailable", "ROLLING_BACK")
		}

		if err = c.route(ctx, 0, false); err != nil {
			return c.recoverRequired(err.Error(), "ROLLING_BACK")
		}
		if r.Candidate != nil {
			stats, er := c.entry.Stats(ctx, r.Started, r.Candidate.Incarnation)
			if er != nil {
				return er
			}
			candidate, er := c.driver.Identity(ctx, *r.Candidate)
			if er == nil && candidate.Incarnation != r.Candidate.Incarnation {
				old := *r.Candidate
				if d, yes := c.driver.(interface {
					Retire(context.Context, p.Target) error
				}); yes {
					if e := d.Retire(ctx, old); e != nil {
						return c.recoverRequired(e.Error(), "ROLLING_BACK")
					}
				}
				r.Candidate.Incarnation = candidate.Incarnation
				if err = c.save(); err != nil {
					return err
				}
			}

			if stats.Active > 0 || (er == nil && candidate.Inflight > 0) {
				if c.now().Sub(r.Since) >= time.Duration(r.Policy.DrainSeconds)*time.Second {
					return c.recoverRequired("candidate_drain_timeout", "ROLLING_BACK")
				}
				return c.save()
			}
			if err = c.driver.Stop(ctx, *r.Candidate); err != nil {
				return c.recoverRequired(err.Error(), "ROLLING_BACK")
			}
		}
		r.Candidate = nil
		if err = c.route(ctx, 0, true); err != nil {
			return c.recoverRequired(err.Error(), "ROLLING_BACK")
		}
		if err = c.driver.Freeze(ctx, false); err != nil {
			return c.recoverRequired(err.Error(), "ROLLING_BACK")
		}
		c.phase("ROLLED_BACK")
	default:
		return fmt.Errorf("unknown rollout phase %s", r.Phase)
	}
	return c.save()
}

func (c *Controller) recoverStable(ctx context.Context) error {
	old := c.report.Stable
	if old.Artifact == "" {
		return c.recoverRequired("stable artifact missing", "RECOVERING")
	}
	next, err := c.driver.Start(ctx, old.Artifact, old.Instance)
	if err != nil {
		return c.recoverRequired("stable restart failed: "+err.Error(), "RECOVERING")
	}
	if next.Incarnation != old.Incarnation {
		if d, ok := c.driver.(interface {
			Retire(context.Context, p.Target) error
		}); ok {
			if err = d.Retire(ctx, old); err != nil {
				return c.recoverRequired(err.Error(), "RECOVERING")
			}
		}
	}
	c.report.Stable = next
	if err = c.route(ctx, 0, true); err != nil {
		return c.recoverRequired(err.Error(), "RECOVERING")
	}
	c.phase("COMPLETED")
	return c.save()
}
