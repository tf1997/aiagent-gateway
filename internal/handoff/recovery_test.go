package handoff

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Build real records through Owner, then model a crash anywhere in the final
// append. No incomplete transition may change the cold-start selection. A
// complete COMMITTED is binding even when its caller never received a reply.
func TestDecisionRecoveryAtEveryTransition(t *testing.T) {
	for _, transition := range []string{"bootstrap", "prepare", "seal", "abort_prepared", "abort_sealed", "commit", "complete"} {
		t.Run(transition, func(t *testing.T) {
			dir := t.TempDir()
			a, b := identity("A"), identity("B")
			o := acquire(t, dir, a)
			if transition != "bootstrap" {
				must(t, o.Bootstrap())
			}
			if transition != "bootstrap" && transition != "prepare" {
				must(t, o.Prepare("h", "r", b))
			}
			var snapshot Snapshot
			if transition == "seal" || transition == "abort_sealed" || transition == "commit" || transition == "complete" {
				var err error
				snapshot, err = o.PersistSnapshot("h", []byte("latest state"), 42, 1024)
				must(t, err)
			}
			if transition == "abort_sealed" || transition == "commit" || transition == "complete" {
				must(t, o.Seal("h", snapshot))
			}
			if transition == "commit" || transition == "complete" {
				must(t, o.Close())
				o = acquire(t, dir, b)
			}
			if transition == "complete" {
				must(t, o.Commit("h", snapshot))
			}
			previous := o.Decision()
			path := filepath.Join(dir, "handoff", "state.log")
			before, err := os.ReadFile(path)
			must(t, err)
			switch transition {
			case "bootstrap":
				err = o.Bootstrap()
			case "prepare":
				err = o.Prepare("h", "r", b)
			case "seal":
				err = o.Seal("h", snapshot)
			case "abort_prepared", "abort_sealed":
				err = o.Abort("h")
			case "commit":
				err = o.Commit("h", snapshot)
			case "complete":
				err = o.Complete("h")
			}
			must(t, err)
			committed := o.Decision()
			after, err := os.ReadFile(path)
			must(t, err)
			must(t, o.Close())
			if !bytes.HasPrefix(after, before) || len(after) == len(before) {
				t.Fatal("transition did not append a journal record")
			}
			frame := after[len(before):]
			for _, size := range []int{0, 1, len(frame) / 2, len(frame) - 1, len(frame)} {
				t.Run(fmt.Sprintf("prefix_%d", size), func(t *testing.T) {
					raw := append(bytes.Clone(before), frame[:size]...)
					must(t, os.WriteFile(path, raw, 0600))
					want := previous
					if size == len(frame) {
						want = committed
					}
					d, err := ReadDecision(dir)
					if want.Phase == "" {
						if !errors.Is(err, ErrNoBaseline) {
							t.Fatal("partial initial record became a baseline", d, err)
						}
					} else if err != nil || d != want {
						t.Fatal("startup chose an incomplete decision", d, want, err)
					}
					unchanged, err := os.ReadFile(path)
					must(t, err)
					if !bytes.Equal(raw, unchanged) {
						t.Fatal("startup reader repaired the journal")
					}
					who := want.Owner
					if want.Phase == "" {
						who = a
					}
					locked := acquire(t, dir, who)
					defer locked.Close()
					if locked.Decision() != want {
						t.Fatal("locked recovery disagrees with startup selection")
					}
					if other, err := Acquire(dir, b); err == nil {
						other.Close()
						t.Fatal("recovery admitted two owners")
					}
					if want.Phase == Committed || want.Phase == Completed {
						if err = locked.Abort("h"); !errors.Is(err, ErrCommitted) {
							t.Fatal("committed record allowed rollback", err)
						}
					} else if want.Phase == Prepared || want.Phase == Sealed || want.Phase == Aborted {
						must(t, locked.Abort("h"))
						must(t, locked.Close())
						late := acquire(t, dir, b)
						defer late.Close()
						if err = late.Commit("h", snapshot); !errors.Is(err, ErrConflict) {
							t.Fatal("late candidate committed after recovered abort", err)
						}
					}
				})
			}
			// A complete but corrupt last record is never a recoverable tail.
			corrupt := bytes.Clone(after)
			corrupt[len(before)+len(frame)/2] ^= 1
			must(t, os.WriteFile(path, corrupt, 0600))
			if _, err := ReadDecision(dir); err == nil || errors.Is(err, ErrNoBaseline) {
				t.Fatal("corruption fell back to an earlier decision", err)
			}
			if reopened, err := Acquire(dir, a); err == nil {
				reopened.Close()
				t.Fatal("corrupt decision opened a writer")
			}
		})
	}
}
