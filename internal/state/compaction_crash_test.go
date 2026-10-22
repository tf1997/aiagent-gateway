package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestJournalCompactionCrashHelper(t *testing.T) {
	args := os.Args
	if len(args) < 3 || args[len(args)-3] != "compaction-crash" {
		t.Skip("subprocess entry")
	}
	dir, point := args[len(args)-2], args[len(args)-1]
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	crash := func(stage string) {
		if point == stage {
			if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			select {}
		}
	}
	j.compactIO.createTemp = func(dir, pattern string) (compactionFile, error) {
		crash("before_create")
		f, err := os.CreateTemp(dir, pattern)
		if err != nil {
			return nil, err
		}
		crash("after_create")
		probe := &failingJournalFile{journalFile: f}
		probe.write = func(b []byte) (int, error) {
			if point == "during_write" {
				if _, err := f.Write(b[:len(b)/2]); err != nil {
					return 0, err
				}
				crash("during_write")
			}
			n, err := f.Write(b)
			if err == nil {
				crash("after_write")
			}
			return n, err
		}
		probe.sync = func() error {
			if err := f.Sync(); err != nil {
				return err
			}
			crash("after_file_sync")
			return nil
		}
		return &probedCompactionFile{probe, f.Name()}, nil
	}
	j.compactIO.rename = func(from, to string) error {
		if err := os.Rename(from, to); err != nil {
			return err
		}
		crash("after_rename")
		return nil
	}
	j.compactIO.syncDir = func(dir string) error {
		if err := systemCompactionIO().syncDir(dir); err != nil {
			return err
		}
		crash("after_directory_sync")
		return nil
	}
	old := j.f
	j.f = &failingJournalFile{journalFile: old, close: func() error {
		if err := old.Close(); err != nil {
			return err
		}
		crash("after_old_close")
		return nil
	}}
	j.commits = 1023
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("2")}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("requested crash point was not reached")
}

func TestJournalCompactionSurvivesProcessKill(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		point   string
		renamed bool
	}{
		{"before_create", false}, {"after_create", false}, {"during_write", false},
		{"after_write", false}, {"after_file_sync", false}, {"after_rename", true},
		{"after_directory_sync", true}, {"after_old_close", true},
	} {
		t.Run(tc.point, func(t *testing.T) {
			j, dir := compactionFixture(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "state.log")
			reader, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestJournalCompactionCrashHelper$", "--", "compaction-crash", dir, tc.point)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exit) {
				t.Fatalf("helper did not reach a crash: %v %s", err, output)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("helper was not killed at the requested boundary: %v %s", err, output)
			}
			original, err := reader.Stat()
			if err != nil {
				t.Fatal(err)
			}
			current, err := os.Stat(path)
			if err != nil || os.SameFile(original, current) == tc.renamed {
				t.Fatal("crash did not reach expected rename boundary", err)
			}
			// A reader opened before replacement keeps a valid committed
			// prefix even after the old inode loses its pathname.
			oldView, _, _, partial, err := readSnapshot(reader)
			if err != nil || partial {
				t.Fatal("old reader saw an invalid prefix", err)
			}
			assertCompactedValues(t, oldView)
			view, err := ReadSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			assertCompactedValues(t, view)
			recovered, err := Open(dir)
			if err != nil {
				t.Fatal("crashed writer did not release its lock", err)
			}
			defer recovered.Close()
			assertCompactedValues(t, recovered.Snapshot())
			if other, err := Open(dir); err == nil {
				other.Close()
				t.Fatal("recovery admitted another writer")
			}
			// Crash-orphaned temporary files must never become state. A new
			// successful compaction must preserve the recovered deletion.
			recovered.commits = 1023
			if err = recovered.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("3")}); err != nil {
				t.Fatal(err)
			}
			view, err = ReadSnapshot(dir)
			if err != nil || len(view) != 2 || string(view["a/value"]) != "3" || string(view["b/keep"]) != `"retained"` {
				t.Fatal("post-crash compaction lost state or sequence", view, err)
			}
		})
	}
}
