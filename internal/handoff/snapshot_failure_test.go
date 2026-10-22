package handoff

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type failingSnapshotFile struct {
	snapshotFile
	point string
}

func (f *failingSnapshotFile) Write(b []byte) (int, error) {
	switch f.point {
	case "write_partial", "write_short":
		n, err := f.snapshotFile.Write(b[:len(b)/2])
		if err != nil || f.point == "write_short" {
			return n, err
		}
		return n, syscall.ENOSPC
	case "write_complete_error":
		n, err := f.snapshotFile.Write(b)
		if err != nil {
			return n, err
		}
		return n, syscall.EIO
	default:
		return f.snapshotFile.Write(b)
	}
}

func (f *failingSnapshotFile) Sync() error {
	if f.point == "file_sync" {
		return syscall.EIO
	}
	if err := f.snapshotFile.Sync(); err != nil {
		return err
	}
	if f.point == "file_sync_completed_error" {
		return syscall.EIO
	}
	return nil
}

func (f *failingSnapshotFile) Close() error {
	if err := f.snapshotFile.Close(); err != nil {
		return err
	}
	if f.point == "file_close" {
		return syscall.EIO
	}
	return nil
}

func TestSnapshotPublicationFailureDoesNotAuthorizeHandoff(t *testing.T) {
	for _, point := range []string{
		"parent_sync", "create_temp", "write_partial", "write_short", "write_complete_error",
		"file_sync", "file_sync_completed_error", "file_close", "publish", "publish_completed_error",
		"directory_sync", "directory_sync_completed_error",
	} {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			a, b := identity("A"), identity("B")
			o := acquire(t, dir, a)
			must(t, o.Bootstrap())
			must(t, o.Prepare("h", "r", b))
			before := o.Decision()
			storage := systemSnapshotIO()
			storage.createTemp = func(dir, pattern string) (snapshotFile, error) {
				if point == "create_temp" {
					return nil, syscall.ENOSPC
				}
				f, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				return &failingSnapshotFile{snapshotFile: f, point: point}, nil
			}
			storage.link = func(from, to string) error {
				if point == "publish" {
					return syscall.EIO
				}
				if err := os.Link(from, to); err != nil {
					return err
				}
				if point == "publish_completed_error" {
					return syscall.EIO
				}
				return nil
			}
			storage.syncDir = func(path string) error {
				parent := filepath.Base(path) == "handoff"
				if (parent && point == "parent_sync") || (!parent && point == "directory_sync") {
					return syscall.EIO
				}
				if err := syncDirectory(path); err != nil {
					return err
				}
				if !parent && point == "directory_sync_completed_error" {
					return syscall.EIO
				}
				return nil
			}
			raw := []byte("latest state including live candidate leases")
			snapshot, err := o.persistSnapshot("h", raw, 42, 1024, storage)
			if err == nil || snapshot != (Snapshot{}) || o.Decision() != before {
				t.Fatal("failed snapshot granted handoff authority", snapshot, err)
			}
			if point == "write_short" && !errors.Is(err, io.ErrShortWrite) {
				t.Fatal("short snapshot write was not detected", err)
			}
			d, err := ReadDecision(dir)
			must(t, err)
			if d != before || d.ColdStart != a.Release {
				t.Fatal("publication failure changed startup choice", d)
			}
			pending, err := filepath.Glob(filepath.Join(dir, "handoff", "snapshots", ".incoming-*"))
			must(t, err)
			if len(pending) != 0 {
				t.Fatal("failed publication left a temporary file", pending)
			}
			// The coordinator may safely abort because the decision journal is
			// still writable. A new attempt can reuse a fully published image,
			// but must sync it again before recording SEALED.
			must(t, o.Abort("h"))
			must(t, o.Prepare("retry", "r2", b))
			snapshot, err = o.PersistSnapshot("retry", raw, 43, 1024)
			must(t, err)
			must(t, o.Seal("retry", snapshot))
			must(t, o.Close())
			next := acquire(t, dir, b)
			got, err := next.LoadSnapshot("retry", 1024)
			must(t, err)
			if !bytes.Equal(got, raw) {
				t.Fatal("retry published an incomplete image")
			}
			must(t, next.Commit("retry", snapshot))
		})
	}
}
