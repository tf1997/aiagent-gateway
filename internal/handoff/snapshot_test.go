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

func TestMatchSnapshotChecksDurableBytesAndDigest(t *testing.T) {
	dir := t.TempDir()
	a := acquire(t, dir, identity("A"))
	must(t, a.Bootstrap())
	must(t, a.Prepare("h", "r", identity("B")))
	raw := bytes.Repeat([]byte("state"), 20000)
	snapshot, err := a.PersistSnapshot("h", raw, 1, len(raw))
	must(t, err)
	must(t, a.Seal("h", snapshot))
	must(t, a.MatchSnapshot("h", len(raw), raw))
	if err = a.MatchSnapshot("other", len(raw), raw); err == nil {
		t.Fatal("wrong handoff accepted")
	}
	if err = a.MatchSnapshot("h", len(raw)-1, raw); err == nil {
		t.Fatal("budget ignored")
	}
	changed := bytes.Clone(raw)
	changed[32769] ^= 1
	if err = a.MatchSnapshot("h", len(raw), changed); err == nil {
		t.Fatal("changed cached image accepted")
	}
	path := filepath.Join(dir, "handoff", "snapshots", snapshot.Digest+".snapshot")
	must(t, os.WriteFile(path, changed, 0600))
	if err = a.MatchSnapshot("h", len(raw), raw); err == nil {
		t.Fatal("changed disk image accepted")
	}
	if err = a.MatchSnapshot("h", len(raw), changed); err == nil {
		t.Fatal("matching corruption bypassed independent digest")
	}
	if err = VerifySnapshot(dir, snapshot); err == nil {
		t.Fatal("corrupt retained snapshot accepted")
	}
	must(t, os.WriteFile(path, raw, 0600))
	must(t, VerifySnapshot(dir, snapshot))
	must(t, os.Remove(path))
	outside := filepath.Join(t.TempDir(), "image")
	must(t, os.WriteFile(outside, raw, 0600))
	must(t, os.Symlink(outside, path))
	if err = a.MatchSnapshot("h", len(raw), raw); err == nil {
		t.Fatal("followed snapshot symlink")
	}
}

func BenchmarkPreparedSnapshotVerification(b *testing.B) {
	a, err := Acquire(b.TempDir(), identity("A"))
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close()
	if err = a.Bootstrap(); err != nil {
		b.Fatal(err)
	}
	if err = a.Prepare("h", "r", identity("B")); err != nil {
		b.Fatal(err)
	}
	raw := bytes.Repeat([]byte("snapshot"), 1<<20)
	snapshot, err := a.PersistSnapshot("h", raw, 1, len(raw))
	if err != nil {
		b.Fatal(err)
	}
	if err = a.Seal("h", snapshot); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"load_and_compare", "stream_and_compare"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if mode == "load_and_compare" {
					copy, err := a.LoadSnapshot("h", len(raw))
					if err != nil || !bytes.Equal(copy, raw) {
						b.Fatal(err)
					}
				} else if err := a.MatchSnapshot("h", len(raw), raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
