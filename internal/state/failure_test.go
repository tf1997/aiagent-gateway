package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type failingJournalFile struct {
	journalFile
	write func([]byte) (int, error)
	sync  func() error
}

func (f *failingJournalFile) Write(b []byte) (int, error) {
	if f.write != nil {
		return f.write(b)
	}
	return f.journalFile.Write(b)
}

func (f *failingJournalFile) Sync() error {
	if f.sync != nil {
		return f.sync()
	}
	return f.journalFile.Sync()
}

// Every failure must fence the original handle. Reopening uses only complete
// checksummed records, even when the failed call actually wrote its decision.
// The two pre-sync cases model either storage outcome after a crash.
func TestJournalUncertainWriteRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, point string
		discard     bool
		want        string
		err         error
	}{
		{"write_rejected", "rejected", false, "1", syscall.ENOSPC},
		{"write_partial", "partial", false, "1", syscall.ENOSPC},
		{"write_short", "short", false, "1", io.ErrShortWrite},
		{"write_complete_error", "complete", false, "2", syscall.EIO},
		{"sync_error_retained", "sync", false, "2", syscall.EIO},
		{"sync_error_lost", "sync", true, "1", syscall.EIO},
		{"sync_completed_error", "synced", false, "2", syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			j, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { j.Close() })
			if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("1")}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "state.log")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			original := j.f
			fault := &failingJournalFile{journalFile: original}
			switch tc.point {
			case "rejected":
				fault.write = func([]byte) (int, error) { return 0, syscall.ENOSPC }
			case "partial", "short", "complete":
				fault.write = func(b []byte) (int, error) {
					size := len(b) / 2
					if tc.point == "complete" {
						size = len(b)
					}
					n, err := original.Write(b[:size])
					if err != nil || tc.point == "short" {
						return n, err
					}
					return n, tc.err
				}
			case "sync", "synced":
				fault.sync = func() error {
					if tc.point == "synced" {
						if err := original.Sync(); err != nil {
							return err
						}
					}
					return syscall.EIO
				}
			}
			j.f = fault
			if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("2")}); !errors.Is(err, tc.err) {
				t.Fatalf("failed commit returned %v, want %v", err, tc.err)
			}
			if !errors.Is(j.Healthy(), tc.err) || string(j.Snapshot()["a/value"]) != "1" {
				t.Fatal("uncertain write was published or writer remained healthy")
			}
			failed, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("3")}); !errors.Is(err, tc.err) {
				t.Fatal("poisoned writer accepted a later transaction", err)
			}
			later, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(failed, later) {
				t.Fatal("poisoned writer changed storage", err)
			}
			if other, err := Open(dir); err == nil {
				other.Close()
				t.Fatal("failed writer released its exclusive lock early")
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			if tc.discard {
				if err = os.Truncate(path, int64(len(before))); err != nil {
					t.Fatal(err)
				}
			}
			unrepaired, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			view, err := ReadSnapshot(dir)
			if err != nil || string(view["a/value"]) != tc.want {
				t.Fatal("read-only recovery selected the wrong prefix", view, err)
			}
			afterRead, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(unrepaired, afterRead) {
				t.Fatal("read-only recovery modified storage", err)
			}
			reopened, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if string(reopened.Snapshot()["a/value"]) != tc.want {
				t.Fatal("locked recovery disagrees with read-only recovery")
			}
			if err = reopened.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("4")}); err != nil {
				t.Fatal("repaired writer could not append", err)
			}
			view, err = ReadSnapshot(dir)
			if err != nil || string(view["a/value"]) != "4" {
				t.Fatal("recovered journal has a sequence gap", view, err)
			}
		})
	}
}

func TestJournalCompactionFailureRetainsCommittedTransaction(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	j.commits = 1023
	// The transaction reaches the real journal and fsync first; only the
	// subsequent compaction cannot create its temporary file.
	j.path = filepath.Join(dir, "missing", "state.log")
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("1")}); err == nil || j.Healthy() == nil {
		t.Fatal("compaction failure did not fence the writer", err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if string(reopened.Snapshot()["a/value"]) != "1" {
		t.Fatal("compaction failure lost a durable transaction")
	}
}
