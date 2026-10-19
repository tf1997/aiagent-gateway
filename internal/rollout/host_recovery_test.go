package rollout

import (
	"context"
	"reflect"
	"testing"

	p "aiagent-gateway/internal/releaseprotocol"
)

func TestHostRecoverySelectsDurableStable(t *testing.T) {
	for _, phase := range []string{"PREPARING", "STARTING", "VERIFYING", "PROMOTING", "OBSERVING", "DRAINING", "ROLLING_BACK", "COMMITTING", "RECOVERY_REQUIRED", "COMPLETED"} {
		t.Run(phase, func(t *testing.T) {
			c, d, e, _ := controllerFixture(t)
			tick(t, c, 2)
			c.report.Phase = phase
			wantArtifact, wantPhase := "old", "ROLLED_BACK"
			if phase == "RECOVERY_REQUIRED" {
				c.report.ResumePhase = "DRAINING"
			}
			if phase == "COMMITTING" || phase == "COMPLETED" {
				old := c.report.Stable
				c.report.Retiring = &old
				c.report.Stable = *c.report.Candidate
				c.report.Candidate = nil
				wantArtifact, wantPhase = "new", "COMPLETED"
			}
			for key, target := range d.targets {
				target.Incarnation += "-restarted"
				d.targets[key] = target
				id := d.workers[target.Instance]
				id.Incarnation = target.Incarnation
				d.workers[target.Instance] = id
			}
			// Only the selected release is recreated in the new host.
			for slot := range d.workers {
				if slot != d.targets[wantArtifact].Instance {
					delete(d.workers, slot)
				}
			}
			if err := c.RecoverHost(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !d.frozen {
				t.Fatal("recovery did not freeze configuration")
			}
			// The recovery decision must survive another restart before Tick.
			reopened, err := Open(c.j, d, e)
			if err != nil {
				t.Fatal(err)
			}
			tick(t, reopened, 1)
			r := reopened.Report()
			if r.Phase != wantPhase || r.Stable != d.targets[wantArtifact] || r.Candidate != nil || r.Retiring != nil || d.frozen {
				t.Fatalf("recovery: %+v, frozen=%v", r, d.frozen)
			}
			if e.traffic.Stable != r.Stable || e.traffic.Candidate != nil || e.traffic.Frozen {
				t.Fatalf("stale traffic after recovery: %+v", e.traffic)
			}
		})
	}
}

func TestHostRecoveryReconcilesLostTrafficReply(t *testing.T) {
	for _, applied := range []bool{false, true} {
		c, _, e, _ := controllerFixture(t)
		tick(t, c, 2)
		next := e.traffic
		next.Revision++
		next.Percent = 100
		c.report.Intent = &next
		if applied {
			e.traffic = next
		}
		if err := c.RecoverHost(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.report.Intent != nil || c.report.Revision != e.traffic.Revision || c.report.Phase != "ROLLING_BACK" {
			t.Fatal(c.Report())
		}
	}
}

func TestHostRecoveryRejectsDivergedJournal(t *testing.T) {
	for _, withIntent := range []bool{false, true} {
		c, d, e, _ := controllerFixture(t)
		tick(t, c, 2)
		if withIntent {
			intent := e.traffic
			intent.Percent = 100
			c.report.Intent = &intent
		} else {
			e.traffic.Revision += 2
		}
		before := c.Report()
		if err := c.RecoverHost(context.Background()); err == nil {
			t.Fatal("accepted inconsistent journals")
		}
		if !reflect.DeepEqual(before, c.Report()) || len(d.stopped) != 0 {
			t.Fatal("rejected recovery changed deployment")
		}
	}
}

func TestHostRecoveryRetriesInitialInstall(t *testing.T) {
	c, _, e, _ := controllerFixture(t)
	c.report.Stable = p.Target{}
	c.report.Phase = "STARTING"
	e.traffic = p.Traffic{}
	if err := c.RecoverHost(context.Background()); err != nil {
		t.Fatal(err)
	}
	tick(t, c, 2)
	if r := c.Report(); r.Phase != "COMPLETED" || r.Stable.Artifact != "new" {
		t.Fatal(r)
	}
}
