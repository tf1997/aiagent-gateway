package state

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"syscall"
	"testing"
)

func TestApplyPreservesOtherKeysAndRecoversAtomically(t *testing.T) {
	for _, failure := range []string{"none", "partial-write", "synced-error"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			j, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if j != nil {
					j.Close()
				}
			})
			initial := map[string]json.RawMessage{"session/a": json.RawMessage(`1`), "session/b": json.RawMessage(`2`), "other/value": json.RawMessage(`3`)}
			if err = j.Apply(initial, nil); err != nil {
				t.Fatal(err)
			}
			before := j.Snapshot()
			initial["other/value"][0] = '9'
			if !reflect.DeepEqual(j.Snapshot(), before) {
				t.Fatal("caller retained a mutable alias to journal state")
			}
			original := j.f
			fault := &failingJournalFile{journalFile: original}
			switch failure {
			case "partial-write":
				fault.write = func(raw []byte) (int, error) { return original.Write(raw[:len(raw)/2]) }
			case "synced-error":
				fault.sync = func() error { return errors.Join(original.Sync(), syscall.EIO) }
			}
			j.f = fault
			err = j.Apply(map[string]json.RawMessage{"session/a": json.RawMessage(`4`)}, []string{"session/b", "session/b", "absent"})
			if failure == "none" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				wanted := error(syscall.EIO)
				if failure == "partial-write" {
					wanted = io.ErrShortWrite
				}
				if !errors.Is(err, wanted) || !errors.Is(j.Apply(nil, nil), wanted) || !reflect.DeepEqual(j.Snapshot(), before) {
					t.Fatal("uncertain patch became visible or accepted later writes", err)
				}
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			view, err := ReadSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]json.RawMessage{"session/a": json.RawMessage(`4`), "other/value": json.RawMessage(`3`)}
			if failure == "partial-write" {
				want = before
			}
			j, err = Open(dir)
			if err != nil || !reflect.DeepEqual(view, want) || !reflect.DeepEqual(j.Snapshot(), want) {
				t.Fatal("patch update/delete was not atomic or lost another key", view, err)
			}
			j.commits = 1023
			if err = j.Apply(map[string]json.RawMessage{"other/after": json.RawMessage(`5`)}, nil); err != nil {
				t.Fatal(err)
			}
			want["other/after"] = json.RawMessage(`5`)
			view, err = ReadSnapshot(dir)
			if err != nil || !reflect.DeepEqual(view, want) {
				t.Fatal("compaction lost patch state", view, err)
			}
		})
	}
}

func TestApplyAdmissionAndInvalidPatch(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	sealed := errors.New("sealed")
	j.SetAdmission(func() (func(), error) { return nil, sealed })
	if err = j.Apply(map[string]json.RawMessage{"a": json.RawMessage(`1`)}, nil); !errors.Is(err, sealed) || len(j.Snapshot()) != 0 {
		t.Fatal("patch bypassed admission", err)
	}
	leaves := 0
	j.SetAdmission(func() (func(), error) { return func() { leaves++ }, nil })
	for _, tc := range []struct {
		values map[string]json.RawMessage
		remove []string
	}{
		{map[string]json.RawMessage{"a": json.RawMessage(`{`)}, nil},
		{map[string]json.RawMessage{"a": json.RawMessage(`1`)}, []string{"a"}},
		{nil, []string{""}},
	} {
		if err = j.Apply(tc.values, tc.remove); err == nil || len(j.Snapshot()) != 0 || j.Healthy() != nil {
			t.Fatal("invalid patch modified or poisoned the journal", err)
		}
	}
	if leaves != 3 {
		t.Fatal("invalid patch did not release admission", leaves)
	}
}
