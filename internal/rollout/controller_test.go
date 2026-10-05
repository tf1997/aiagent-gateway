package rollout

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
	"context"
	"fmt"
	"testing"
	"time"
)

type fakeDriver struct {
	workers   map[string]p.Identity
	targets   map[string]p.Target
	stopped   []string
	frozen    bool
	startFail bool
}

func (d *fakeDriver) Start(_ context.Context, artifact, slot string) (p.Target, error) {
	if d.startFail {
		return p.Target{}, fmt.Errorf("bind failed")
	}
	t := d.targets[artifact]
	return t, nil
}
func (d *fakeDriver) Identity(_ context.Context, t p.Target) (p.Identity, error) {
	id, ok := d.workers[t.Instance]
	if !ok {
		return id, fmt.Errorf("stopped")
	}
	return id, nil
}
func (d *fakeDriver) Stop(_ context.Context, t p.Target) error {
	d.stopped = append(d.stopped, t.Instance)
	delete(d.workers, t.Instance)
	return nil
}
func (d *fakeDriver) Freeze(_ context.Context, b bool) error { d.frozen = b; return nil }

type fakeEntry struct {
	traffic        p.Traffic
	stats          map[string]p.Statistics
	failAfterApply bool
}

func (e *fakeEntry) Traffic(context.Context) (p.Traffic, error) { return e.traffic, nil }
func (e *fakeEntry) Apply(_ context.Context, expected uint64, t p.Traffic) error {
	if e.traffic.Revision != expected {
		return fmt.Errorf("conflict")
	}
	e.traffic = t
	if e.failAfterApply {
		e.failAfterApply = false
		return fmt.Errorf("reply lost")
	}
	return nil
}
func (e *fakeEntry) Stats(_ context.Context, _ time.Time, w string) (p.Statistics, error) {
	return e.stats[w], nil
}
func controllerFixture(t *testing.T) (*Controller, *fakeDriver, *fakeEntry, *time.Time) {
	t.Helper()
	old := p.Target{Artifact: "old", Instance: "a", Release: "old", Incarnation: "old-boot", URL: "http://127.0.0.1:18080"}
	next := p.Target{Artifact: "new", Instance: "b", Release: "new", Incarnation: "new-boot", URL: "http://127.0.0.1:18081"}
	d := &fakeDriver{workers: map[string]p.Identity{"a": {Protocol: 1, StateProtocol: 1, Release: "old", Instance: "a", Incarnation: "old-boot", Ready: true, ConfigRevision: 1}, "b": {Protocol: 1, StateProtocol: 1, Release: "new", Instance: "b", Incarnation: "new-boot", Ready: true, ConfigRevision: 1}}, targets: map[string]p.Target{"old": old, "new": next}}
	e := &fakeEntry{traffic: p.Traffic{Revision: 1, Stable: old}, stats: map[string]p.Statistics{}}
	j, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	c, err := Open(j, d, e)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.now = func() time.Time { return now }
	c.report.Stable = old
	c.report.Phase = "COMPLETED"
	policy := p.DefaultPolicy()
	policy.VerifyMinSeconds = 0
	policy.VerifyTimeoutSeconds = 2
	policy.ObserveSeconds = 1
	policy.MinSuccess = 1
	policy.ObserveMinSuccess = 1
	policy.DrainSeconds = 2
	if err = c.Begin("new", policy); err != nil {
		t.Fatal(err)
	}
	return c, d, e, &now
}
func tick(t *testing.T, c *Controller, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestAutomaticPromotionAndDrain(t *testing.T) {
	c, d, e, now := controllerFixture(t)
	tick(t, c, 2)
	if c.Report().Phase != "VERIFYING" || e.traffic.Percent != 5 || !d.frozen {
		t.Fatal(c.Report())
	}
	e.stats["new-boot"] = p.Statistics{Counts: map[string]p.Counts{"http": {Success: 1}}}
	tick(t, c, 2)
	if c.Report().Phase != "OBSERVING" || e.traffic.Percent != 100 {
		t.Fatal(c.Report())
	}
	*now = now.Add(2 * time.Second)
	tick(t, c, 1)
	old := d.workers["a"]
	old.Inflight = 1
	d.workers["a"] = old
	tick(t, c, 1)
	if len(d.stopped) != 0 {
		t.Fatal("old active request stopped")
	}
	old.Inflight = 0
	d.workers["a"] = old
	tick(t, c, 2)
	r := c.Report()
	if r.Phase != "COMPLETED" || r.Stable.Release != "new" || d.frozen || e.traffic.Stable.Release != "new" || len(d.stopped) != 1 {
		t.Fatalf("did not complete: %+v", r)
	}
}
func TestAutomaticRollbackOnFailureAndMissingSamples(t *testing.T) {
	for _, mode := range []string{"failure", "no_samples", "lost_results", "restart", "missing_profile"} {
		t.Run(mode, func(t *testing.T) {
			c, d, e, now := controllerFixture(t)
			if mode == "missing_profile" {
				c.report.Policy.Profiles = []string{"http", "sse"}
			}
			tick(t, c, 2)
			switch mode {
			case "failure":
				e.stats["new-boot"] = p.Statistics{Failures: 1}
			case "lost_results":
				e.stats["new-boot"] = p.Statistics{Lost: true}
			case "restart":
				id := d.workers["b"]
				id.Incarnation = "other"
				d.workers["b"] = id
			case "missing_profile":
				e.stats["new-boot"] = p.Statistics{Counts: map[string]p.Counts{"http": {Success: 10}}}
			}
			*now = now.Add(3 * time.Second)
			tick(t, c, 2)
			if c.Report().Phase != "ROLLED_BACK" || e.traffic.Stable.Release != "old" || e.traffic.Candidate != nil {
				t.Fatal(c.Report())
			}
			for _, slot := range d.stopped {
				if slot == "a" {
					t.Fatal("stopped stable worker")
				}
			}
		})
	}
}
func TestObservationFailureAndRollbackDrain(t *testing.T) {
	c, d, e, now := controllerFixture(t)
	tick(t, c, 2)
	e.stats["new-boot"] = p.Statistics{Counts: map[string]p.Counts{"http": {Success: 1}}}
	tick(t, c, 2)
	e.stats["new-boot"] = p.Statistics{Failures: 1, Active: 1}
	tick(t, c, 2)
	if e.traffic.Percent != 0 || len(d.stopped) != 0 || c.Report().Phase != "ROLLING_BACK" {
		t.Fatal(c.Report())
	}
	*now = now.Add(3 * time.Second)
	tick(t, c, 1)
	if c.Report().Phase != "RECOVERY_REQUIRED" {
		t.Fatal(c.Report())
	}
	e.stats["new-boot"] = p.Statistics{Failures: 1}
	if err := c.Resume(); err != nil {
		t.Fatal(err)
	}
	tick(t, c, 1)
	if c.Report().Phase != "ROLLED_BACK" {
		t.Fatal(c.Report())
	}
}
func TestLostCutoverReplyRecoveredByRevision(t *testing.T) {
	c, d, e, _ := controllerFixture(t)
	tick(t, c, 1)
	e.failAfterApply = true
	tick(t, c, 1)
	if c.Report().Phase != "RECOVERY_REQUIRED" || e.traffic.Percent != 5 {
		t.Fatal(c.Report())
	}
	recovered, err := Open(c.j, d, e)
	if err != nil {
		t.Fatal(err)
	}
	if err = recovered.Resume(); err != nil {
		t.Fatal(err)
	}
	tick(t, recovered, 1)
	if recovered.Report().Phase != "VERIFYING" || e.traffic.Revision != 2 {
		t.Fatal(recovered.Report(), e.traffic)
	}
}
func TestCandidateStartFailureKeepsStable(t *testing.T) {
	c, d, e, _ := controllerFixture(t)
	d.startFail = true
	tick(t, c, 3)
	if c.Report().Phase != "ROLLED_BACK" || e.traffic.Stable.Release != "old" || len(d.stopped) > 0 {
		t.Fatal(c.Report())
	}
}
