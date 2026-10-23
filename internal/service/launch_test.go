package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"aiagent-gateway/internal/handoff"
)

func TestLaunchSelectsDurableReleaseWithoutFallback(t *testing.T) {
	root := filepath.Join(t.TempDir(), "releases")
	stage := func(body string) string {
		digest := sha256.Sum256([]byte(body))
		path := filepath.Join(root, hex.EncodeToString(digest[:]), "gateway")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	initial, selected := stage("initial"), stage("selected")
	data := filepath.Join(t.TempDir(), "data")
	uid := uint32(os.Geteuid())
	if got, err := SelectArtifact(data, initial, root, uid); err != nil || got != initial {
		t.Fatal(got, err)
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Fatal("read-only launch created state directory", err)
	}
	o, err := handoff.Acquire(data, handoff.Identity{Incarnation: "owner", Release: filepath.Base(filepath.Dir(selected))})
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := SelectArtifact(data, initial, root, uid); err != nil || got != selected {
		t.Fatal(got, err)
	}
	if err = os.Remove(selected); err != nil {
		t.Fatal(err)
	}
	if _, err = SelectArtifact(data, initial, root, uid); err == nil {
		t.Fatal("missing selected release fell back to initial")
	}
	if err = os.WriteFile(filepath.Join(data, "handoff", "state.log"), []byte("corrupt complete journal\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = SelectArtifact(data, initial, root, uid); err == nil {
		t.Fatal("corrupt metadata fell back to initial")
	}
	if err = os.Mkdir(filepath.Join(data, "shared"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{true, false} {
		path := filepath.Join(data, "handoff", "state.log")
		if missing {
			err = os.Remove(path)
		} else {
			err = os.WriteFile(path, nil, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = SelectArtifact(data, initial, root, uid); !errors.Is(err, handoff.ErrLostBaseline) {
			t.Fatal("lost metadata with existing business state selected initial release", missing, err)
		}
		if owner, err := handoff.Acquire(data, handoff.Identity{Incarnation: "restart", Release: filepath.Base(filepath.Dir(initial))}); !errors.Is(err, handoff.ErrLostBaseline) {
			if owner != nil {
				owner.Close()
			}
			t.Fatal("writer accepted a new baseline over existing state", missing, err)
		}
	}
}
