package entry

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

func TestEntryTransferPreservesAffinityAndEvidence(t *testing.T) {
	j, err := state.Open(filepath.Join(t.TempDir(), "entry"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	e, err := Open(j, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	target := p.Target{Release: "a", Incarnation: "a", URL: "http://127.0.0.1:80"}
	candidate := p.Target{Release: "b", Incarnation: "b", URL: "http://127.0.0.1:81"}
	if err = e.Apply(0, p.Traffic{Revision: 1, Rollout: "r", Stable: target, Candidate: &candidate, Percent: 5}); err != nil {
		t.Fatal(err)
	}
	_, result, err := e.start("session", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err = e.Seal(ctx); err == nil {
		t.Fatal("active request did not block entry transfer")
	}
	result.Profile, result.Verdict, result.Completed = "http", "success", time.Now()
	e.finish(result)
	if err = e.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := e.Export()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.start("late", ""); err == nil {
		t.Fatal("sealed entry admitted new request")
	}
	nextJournal, err := state.Open(filepath.Join(t.TempDir(), "entry"))
	if err != nil {
		t.Fatal(err)
	}
	defer nextJournal.Close()
	before := nextJournal.Snapshot()
	next, err := Import(nextJournal, "secret", nil, v)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if !reflect.DeepEqual(before, nextJournal.Snapshot()) {
		t.Fatal("import wrote before commit")
	}
	got, err := next.Export()
	if err != nil || !reflect.DeepEqual(v, got) {
		t.Fatal("entry state changed", err)
	}
	stats := next.Stats(time.Time{}, result.Incarnation)
	if stats.Counts["http"].Success != 1 || stats.Active != 0 || next.counter != e.counter {
		t.Fatal("evidence or allocator sequence lost")
	}
	// Neither the exported image nor the caller's import input may share maps
	// or the candidate target with either live allocator.
	v.Traffic.Candidate.Release = "mutated"
	v.Results[result.ID] = p.Result{}
	v.Active["injected"] = p.Result{}
	for key := range v.Bindings {
		delete(v.Bindings, key)
	}
	for _, allocator := range []*Entry{e, next} {
		unchanged, err := allocator.Export()
		if err != nil || !reflect.DeepEqual(unchanged, got) {
			t.Fatal("transfer image aliases a live allocator", err)
		}
	}
}
