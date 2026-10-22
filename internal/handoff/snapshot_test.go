package handoff

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotDurabilityAndIdentityBinding(t *testing.T) {
	dir := t.TempDir()
	a := acquire(t, dir, identity("A"))
	must(t, a.Bootstrap())
	must(t, a.Prepare("h", "r", identity("B")))
	raw := bytes.Repeat([]byte("state"), 300000)
	if _, err := a.PersistSnapshot("other", raw, 10, len(raw)); err == nil {
		t.Fatal("unrelated handoff wrote snapshot")
	}
	if _, err := a.PersistSnapshot("h", raw, 10, len(raw)-1); err == nil {
		t.Fatal("oversize snapshot")
	}
	s, err := a.PersistSnapshot("h", raw, 10, len(raw))
	must(t, err)
	same, err := a.PersistSnapshot("h", raw, 10, len(raw))
	must(t, err)
	if same != s {
		t.Fatal("snapshot publication not idempotent")
	}
	must(t, a.Seal("h", s))
	must(t, a.Close())
	b := acquire(t, dir, identity("B"))
	got, err := b.LoadSnapshot("h", len(raw))
	must(t, err)
	if !bytes.Equal(raw, got) {
		t.Fatal("snapshot changed across owner lock transfer")
	}
	path := filepath.Join(dir, "handoff", "snapshots", s.Digest+".snapshot")
	raw[0] ^= 1
	must(t, os.WriteFile(path, raw, 0600))
	if _, err = b.LoadSnapshot("h", len(raw)); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
	if b.Decision().Phase != Sealed {
		t.Fatal("failed import changed ownership")
	}
}

func TestSnapshotRejectsSymlinkAndIncompletePublication(t *testing.T) {
	dir := t.TempDir()
	a := acquire(t, dir, identity("A"))
	must(t, a.Bootstrap())
	must(t, a.Prepare("h", "r", identity("B")))
	dest := filepath.Join(dir, "handoff", "snapshots")
	must(t, os.Symlink(t.TempDir(), dest))
	if _, err := a.PersistSnapshot("h", []byte("state"), 1, 100); err == nil {
		t.Fatal("followed snapshot directory symlink")
	}
	must(t, os.Remove(dest))
	s, err := a.PersistSnapshot("h", []byte("state"), 1, 100)
	must(t, err)
	must(t, a.Seal("h", s))
	path := filepath.Join(dest, s.Digest+".snapshot")
	must(t, os.Remove(path))
	other := filepath.Join(t.TempDir(), "snapshot")
	must(t, os.WriteFile(other, []byte("state"), 0600))
	must(t, os.Symlink(other, path))
	if _, err = a.LoadSnapshot("h", 100); err == nil {
		t.Fatal("followed snapshot file symlink")
	}
	must(t, os.Remove(path))
	must(t, os.WriteFile(path, []byte("stat"), 0600))
	if _, err = a.LoadSnapshot("h", 100); err == nil {
		t.Fatal("accepted truncated snapshot")
	}
}
