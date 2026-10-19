package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func digest(s string) string     { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
func identity(s string) Identity { return Identity{Incarnation: s, Release: digest(s)} }
func acquire(t *testing.T, dir string, who Identity) *Owner {
	t.Helper()
	o, err := Acquire(dir, who)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	return o
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestHandoffSingleWriterCommitAndLostReply(t *testing.T) {
	dir := t.TempDir()
	a, b := identity("A"), identity("B")
	o := acquire(t, dir, a)
	must(t, o.Bootstrap())
	if _, err := Acquire(dir, b); err == nil {
		t.Fatal("second owner acquired lock")
	}
	must(t, o.Prepare("handoff_ab", "rollout_ab", b))
	snapshot := Snapshot{Digest: digest("state"), Revision: 42, Bytes: 5}
	must(t, o.Seal("handoff_ab", snapshot))
	d, err := ReadDecision(dir)
	must(t, err)
	if d.ColdStart != a.Release || d.Phase != Sealed {
		t.Fatalf("wrong precommit choice: %+v", d)
	}
	must(t, o.Close())
	next := acquire(t, dir, b)
	bad := snapshot
	bad.Revision++
	if err := next.Commit("handoff_ab", bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong snapshot accepted: %v", err)
	}
	must(t, next.Commit("handoff_ab", snapshot))
	// Simulate committed state with a lost IPC reply and process exit.
	must(t, next.Close())
	d, err = ReadDecision(dir)
	must(t, err)
	if d.Owner != b || d.ColdStart != b.Release || d.Epoch != d.NextEpoch {
		t.Fatalf("commit lost: %+v", d)
	}
	old := acquire(t, dir, a)
	if err = old.Abort("handoff_ab"); !errors.Is(err, ErrCommitted) {
		t.Fatalf("rollback after commit: %v", err)
	}
	if _, err = old.Recover(); !errors.Is(err, ErrConflict) {
		t.Fatalf("old release cold start accepted: %v", err)
	}
	must(t, old.Close())
	next = acquire(t, dir, b)
	must(t, next.Commit("handoff_ab", snapshot)) // same decision is idempotent
	must(t, next.Complete("handoff_ab"))
	c := identity("C")
	must(t, next.Prepare("handoff_bc", "rollout_bc", c))
	must(t, next.Seal("handoff_bc", Snapshot{Digest: digest("next"), Revision: 73, Bytes: 4}))
	must(t, next.Close())
	last := acquire(t, dir, c)
	must(t, last.Commit("handoff_bc", Snapshot{Digest: digest("next"), Revision: 73, Bytes: 4}))
	if last.Decision().ColdStart != c.Release {
		t.Fatal("continuous upgrade lost selected artifact")
	}
}

func TestAbortFencesLateCandidateAndColdStartChangesEpoch(t *testing.T) {
	dir := t.TempDir()
	a, b := identity("A"), identity("B")
	o := acquire(t, dir, a)
	must(t, o.Bootstrap())
	epoch := o.Decision().Epoch
	must(t, o.Prepare("handoff_ab", "rollout_ab", b))
	snapshot := Snapshot{Digest: digest("state")}
	must(t, o.Seal("handoff_ab", snapshot))
	must(t, o.Close())
	o = acquire(t, dir, a)
	must(t, o.Abort("handoff_ab"))
	must(t, o.Close())
	late := acquire(t, dir, b)
	if err := late.Commit("handoff_ab", snapshot); !errors.Is(err, ErrConflict) {
		t.Fatalf("late candidate committed: %v", err)
	}
	must(t, late.Close())
	a.Incarnation = "A-restarted"
	o = acquire(t, dir, a)
	previous, err := o.Recover()
	must(t, err)
	if previous.Phase != Aborted || o.Decision().Epoch == epoch || o.Decision().Owner != a {
		t.Fatal("cold recovery lost identity boundary")
	}
}

func TestHandoffClosedOwnerAndCorruptDecision(t *testing.T) {
	dir := t.TempDir()
	o := acquire(t, dir, identity("A"))
	must(t, o.Bootstrap())
	must(t, o.Close())
	if err := o.Prepare("h", "r", identity("B")); err == nil {
		t.Fatal("closed owner mutated decision")
	}
	path := filepath.Join(dir, "handoff", "state.log")
	raw, err := os.ReadFile(path)
	must(t, err)
	raw[len(raw)/2] ^= 1
	must(t, os.WriteFile(path, raw, 0600))
	if _, err := ReadDecision(dir); err == nil {
		t.Fatal("corruption used as startup choice")
	}
	if _, err := Acquire(dir, identity("B")); err == nil {
		t.Fatal("corrupt handoff reopened")
	}
}

func TestOwnerLockCannotBeRedirected(t *testing.T) {
	dir := t.TempDir()
	other := filepath.Join(t.TempDir(), "other.lock")
	must(t, os.WriteFile(other, nil, 0600))
	must(t, os.Symlink(other, filepath.Join(dir, "owner.lock")))
	if _, err := Acquire(dir, identity("A")); err == nil {
		t.Fatal("followed owner lock symlink")
	}
}
