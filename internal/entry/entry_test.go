package entry

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
	"testing"
	"time"
)

func TestEntryCASAffinityAndRestartEvidence(t *testing.T) {
	dir := t.TempDir()
	j, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(j, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	old := p.Target{Release: "old", Incarnation: "old-boot", Instance: "a", URL: "http://127.0.0.1:18080"}
	next := p.Target{Release: "new", Incarnation: "new-boot", Instance: "b", URL: "http://127.0.0.1:18081"}
	traffic := p.Traffic{Revision: 1, Stable: old, Candidate: &next, Percent: 5, Rollout: "test"}
	if err = e.Apply(0, traffic); err != nil {
		t.Fatal(err)
	}
	first, r, err := e.start("same", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Incarnation != "new-boot" {
		t.Fatal("candidate allocation missing")
	}
	r.Completed = time.Now()
	r.Profile = "http"
	r.Verdict = "success"
	e.finish(r)
	traffic.Revision = 2
	traffic.Percent = 40
	if err = e.Apply(0, traffic); err == nil {
		t.Fatal("stale CAS accepted")
	}
	if e.Traffic().Revision != 1 {
		t.Fatal("failed CAS changed state")
	}
	if err = e.Apply(1, traffic); err != nil {
		t.Fatal(err)
	}
	again, _, err := e.start("same", "")
	if err != nil || again.Incarnation != first.Incarnation {
		t.Fatal("weight update moved affinity", err)
	}
	j.Close()
	j, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	recovered, err := Open(j, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	stats := recovered.Stats(time.Time{}, "new-boot")
	if stats.Failures != 1 || stats.Counts["http"].Success != 1 || stats.Active != 0 {
		t.Fatalf("restart fabricated success: %+v", stats)
	}
}
func TestEntryRejectsInvalidTargetAndClosedStorage(t *testing.T) {
	j, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e, _ := Open(j, "token", nil)
	target := p.Target{Release: "old", Incarnation: "boot", URL: "http://remote.example:8080"}
	if err = e.Apply(0, p.Traffic{Revision: 1, Stable: target}); err == nil {
		t.Fatal("remote worker target accepted")
	}
	target.URL = "http://127.0.0.1:18080"
	if err = e.Apply(0, p.Traffic{Revision: 1, Stable: target}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	if _, _, err = e.start("session", ""); err == nil {
		t.Fatal("accepted request after storage loss")
	}
}
