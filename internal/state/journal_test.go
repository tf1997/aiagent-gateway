package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalRecoveryAndLock(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("second writer accepted")
	}
	if err = j.Replace("a/", map[string]json.RawMessage{"a/one": json.RawMessage(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if err = j.Replace("b/", map[string]json.RawMessage{"b/two": json.RawMessage(`2`)}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "state.log"), os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString(`{"payload":`)
	f.Close()
	j, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if string(j.Snapshot()["b/two"]) != "2" {
		t.Fatal("lost committed state")
	}
	if err = j.Replace("a/", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := j.Snapshot()["a/one"]; ok {
		t.Fatal("delete lost")
	}
	if _, ok := j.Snapshot()["b/two"]; !ok {
		t.Fatal("cross-domain deletion")
	}
}
func TestJournalCorruptionAndCompaction(t *testing.T) {
	dir := t.TempDir()
	j, _ := Open(dir)
	j.commits = 1023
	if err := j.Replace("a/", map[string]json.RawMessage{"a/one": json.RawMessage(`1`)}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Snapshot()) != 1 {
		t.Fatal("compaction lost state")
	}
	j.Close()
	raw, _ := os.ReadFile(filepath.Join(dir, "state.log"))
	raw[20] ^= 1
	os.WriteFile(filepath.Join(dir, "state.log"), raw, 0600)
	if _, err = Open(dir); err == nil {
		t.Fatal("corruption accepted")
	}
}
