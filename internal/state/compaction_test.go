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

type probedCompactionFile struct {
	*failingJournalFile
	path string
}

func (f *probedCompactionFile) Name() string { return f.path }

func assertCompactedValues(t *testing.T, values map[string]json.RawMessage) {
	t.Helper()
	if len(values) != 2 || string(values["a/value"]) != "2" || string(values["b/keep"]) != `"retained"` {
		t.Fatal("compaction lost committed values, deletions or another domain", values)
	}
}

func compactionFixture(t *testing.T) (*Journal, string) {
	t.Helper()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("1"), "a/delete": json.RawMessage("true")}); err != nil {
		t.Fatal(err)
	}
	if err = j.Replace("b/", map[string]json.RawMessage{"b/keep": json.RawMessage(`"retained"`)}); err != nil {
		t.Fatal(err)
	}
	j.commits = 1023
	return j, dir
}

func TestJournalCompactionFailureBoundaries(t *testing.T) {
	for _, tc := range []struct {
		point      string
		err        error
		renamed    bool
		restoreOld bool
	}{
		{"create", syscall.ENOSPC, false, false},
		{"write_rejected", syscall.ENOSPC, false, false},
		{"write_partial", syscall.ENOSPC, false, false},
		{"write_short", io.ErrShortWrite, false, false},
		{"write_completed_error", syscall.EIO, false, false},
		{"file_sync", syscall.EIO, false, false},
		{"file_synced_error", syscall.EIO, false, false},
		{"rename", syscall.EIO, false, false},
		{"renamed_error", syscall.EIO, true, false},
		{"directory_sync", syscall.EIO, true, false},
		{"directory_synced_error", syscall.EIO, true, false},
		{"directory_sync_old_name_survives", syscall.EIO, true, true},
		{"old_close", syscall.EIO, true, false},
		{"cleanup_close", syscall.ENOSPC, false, false},
	} {
		t.Run(tc.point, func(t *testing.T) {
			j, dir := compactionFixture(t)
			path := filepath.Join(dir, "state.log")
			reader, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			old := j.f
			var temporary *os.File
			j.compactIO.createTemp = func(dir, pattern string) (compactionFile, error) {
				if tc.point == "create" {
					return nil, syscall.ENOSPC
				}
				f, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				temporary = f
				probe := &failingJournalFile{journalFile: f}
				probe.write = func(b []byte) (int, error) {
					if tc.point == "write_rejected" || tc.point == "cleanup_close" {
						return 0, syscall.ENOSPC
					}
					size := len(b)
					if tc.point == "write_partial" || tc.point == "write_short" {
						size /= 2
					}
					n, err := f.Write(b[:size])
					if err == nil && (tc.point == "write_partial" || tc.point == "write_completed_error") {
						err = tc.err
					}
					return n, err
				}
				probe.sync = func() error {
					if tc.point == "file_sync" {
						return syscall.EIO
					}
					if err := f.Sync(); err != nil {
						return err
					}
					if tc.point == "file_synced_error" {
						return syscall.EIO
					}
					return nil
				}
				probe.close = func() error {
					if err := f.Close(); err != nil {
						return err
					}
					if tc.point == "cleanup_close" {
						return syscall.EIO
					}
					return nil
				}
				return &probedCompactionFile{probe, f.Name()}, nil
			}
			j.compactIO.rename = func(from, to string) error {
				if tc.point == "rename" {
					return syscall.EIO
				}
				if err := os.Rename(from, to); err != nil {
					return err
				}
				if tc.point == "renamed_error" {
					return syscall.EIO
				}
				return nil
			}
			j.compactIO.syncDir = func(dir string) error {
				if tc.point == "directory_sync" || tc.restoreOld {
					return syscall.EIO
				}
				if err := systemCompactionIO().syncDir(dir); err != nil {
					return err
				}
				if tc.point == "directory_synced_error" {
					return syscall.EIO
				}
				return nil
			}
			if tc.point == "old_close" {
				j.f = &failingJournalFile{journalFile: old, close: func() error {
					if err := old.Close(); err != nil {
						return err
					}
					return syscall.EIO
				}}
			}
			err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("2")})
			if !errors.Is(err, tc.err) || !errors.Is(j.Healthy(), tc.err) {
				t.Fatal("compaction failure left a writable handle", err, j.Healthy())
			}
			if tc.point == "cleanup_close" && !errors.Is(err, syscall.EIO) {
				t.Fatal("cleanup error was lost", err)
			}
			assertCompactedValues(t, j.Snapshot())
			oldRaw, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			oldView, _, _, _, err := readSnapshot(bytes.NewReader(oldRaw))
			if err != nil {
				t.Fatal(err)
			}
			assertCompactedValues(t, oldView)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, sequence, _, _, err := readSnapshot(bytes.NewReader(raw))
			if err != nil || (sequence == 1) != tc.renamed {
				t.Fatal("fault did not reach the expected publication boundary", sequence, err)
			}
			if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("3")}); !errors.Is(err, tc.err) {
				t.Fatal("failed compaction allowed a subsequent transaction", err)
			}
			unchanged, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(raw, unchanged) {
				t.Fatal("failed writer changed the published journal", err)
			}
			if other, err := Open(dir); err == nil {
				other.Close()
				t.Fatal("failed compaction released its writer lock")
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			if temporary != nil {
				if _, err = temporary.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("compaction leaked its temporary handle", err)
				}
			}
			if _, err = old.Write(nil); !errors.Is(err, os.ErrClosed) {
				t.Fatal("compaction leaked its old handle", err)
			}
			pending, err := filepath.Glob(filepath.Join(dir, ".snapshot-*"))
			if err != nil || len(pending) != 0 {
				t.Fatal("failed compaction left temporary paths", pending, err)
			}
			if tc.restoreOld {
				// Directory sync failed: model a crash retaining the old name.
				// Its append was synced before compaction and must not be lost.
				if err = os.WriteFile(path, oldRaw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			view, err := ReadSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			assertCompactedValues(t, view)
			reopened, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertCompactedValues(t, reopened.Snapshot())
			reopened.commits = 1023
			if err = reopened.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("4")}); err != nil {
				t.Fatal("recovered journal could not compact again", err)
			}
			view, err = ReadSnapshot(dir)
			if err != nil || len(view) != 2 || string(view["a/value"]) != "4" || string(view["b/keep"]) != `"retained"` {
				t.Fatal("recovery introduced a sequence gap or lost a domain", view, err)
			}
		})
	}
}

func TestJournalCompactionPropagatesOldHandleCloseFailure(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	old := j.f
	j.f = &failingJournalFile{journalFile: old, close: func() error {
		if err := old.Close(); err != nil {
			return err
		}
		return syscall.EIO
	}}
	j.commits = 1023
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("1")}); !errors.Is(err, syscall.EIO) {
		t.Fatal("old handle close error was ignored", err)
	}
	if !errors.Is(j.Healthy(), syscall.EIO) {
		t.Fatal("failed handle replacement left the writer healthy")
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
		t.Fatal("handle close failure lost the durable transaction")
	}
}

func TestJournalCompactionRetainsSyncedFile(t *testing.T) {
	j, dir := compactionFixture(t)
	path := filepath.Join(dir, "state.log")
	oldReader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer oldReader.Close()
	var compacted *os.File
	j.compactIO.createTemp = func(dir, pattern string) (compactionFile, error) {
		f, err := os.CreateTemp(dir, pattern)
		compacted = f
		return f, err
	}
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("2")}); err != nil {
		t.Fatal(err)
	}
	if compacted == nil || j.f != compacted {
		t.Fatal("writer reopened the path instead of retaining the synced file")
	}
	if err = j.Replace("a/", map[string]json.RawMessage{"a/value": json.RawMessage("3")}); err != nil {
		t.Fatal("retained file could not accept the next transaction", err)
	}
	view, err := ReadSnapshot(dir)
	if err != nil || len(view) != 2 || string(view["a/value"]) != "3" || string(view["b/keep"]) != `"retained"` {
		t.Fatal("append after compaction overwrote the image", view, err)
	}
	oldView, _, _, _, err := readSnapshot(oldReader)
	if err != nil {
		t.Fatal(err)
	}
	assertCompactedValues(t, oldView)
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = compacted.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("journal close leaked the retained descriptor", err)
	}
}
