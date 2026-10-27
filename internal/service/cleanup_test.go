package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/state"
)

func cleanupFixture(t *testing.T) (*manager, []string, handoff.Decision) {
	t.Helper()
	m := &manager{root: t.TempDir(), output: &bytes.Buffer{}}
	write := func(path string, raw []byte, mode os.FileMode) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, mode); err != nil {
			t.Fatal(err)
		}
	}
	var artifacts []string
	for i := 0; i < 7; i++ {
		raw := []byte{byte(i), 42}
		sum := sha256.Sum256(raw)
		artifact := filepath.Join(ReleasesDir, hex.EncodeToString(sum[:]), "gateway")
		write(m.path(artifact), raw, 0755)
		stamp := time.Unix(int64(100+i), 0)
		if err := os.Chtimes(filepath.Dir(m.path(artifact)), stamp, stamp); err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, artifact)
	}
	m.executable = m.path(artifacts[0])
	raw, err := credentials()
	if err != nil {
		t.Fatal(err)
	}
	write(m.path(configPath), raw, 0600)
	raw, err = json.Marshal(Deployment{InitialRelease: artifacts[0], Policy: p.DefaultPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	write(m.path(DeploymentPath), raw, 0600)
	dir := m.path(filepath.Dir(dataPath))
	aid := handoff.Identity{Incarnation: "a", Release: filepath.Base(filepath.Dir(artifacts[0]))}
	bid := handoff.Identity{Incarnation: "b", Release: filepath.Base(filepath.Dir(artifacts[1]))}
	a, err := handoff.Acquire(dir, aid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	for _, err := range []error{a.Bootstrap(), a.Prepare("h", "r", bid)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := a.PersistSnapshot("h", []byte("retained-state"), 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Seal("h", snapshot); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := handoff.Acquire(dir, bid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err = b.Commit("h", snapshot); err != nil {
		t.Fatal(err)
	}
	if err = b.Complete("h"); err != nil {
		t.Fatal(err)
	}
	decision := b.Decision()
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := state.Open(filepath.Join(dir, "release"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })
	raw, _ = json.Marshal(rollout.Report{Phase: "COMPLETED", Artifact: artifacts[1], Stable: p.Target{Release: bid.Release, Artifact: artifacts[1]}})
	if err = journal.Replace("process/", map[string]json.RawMessage{"process/report": raw}); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	return m, artifacts, decision
}

func TestCleanupPreservesRecoveryAndRetainsRecentReleases(t *testing.T) {
	m, artifacts, decision := cleanupFixture(t)
	dir := m.path(filepath.Dir(dataPath))
	snapshots := filepath.Join(dir, "handoff", "snapshots")
	orphan := strings.Repeat("c", 64) + ".snapshot"
	for _, name := range []string{orphan, ".incoming-orphan", "notes"} {
		if err := os.WriteFile(filepath.Join(snapshots, name), []byte("keep-or-remove"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("never-delete"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(snapshots, ".incoming-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "handoff", ".snapshot-orphan"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(m.path(unitPath)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.path(unitPath), []byte("ExecStartPre=+"+artifacts[2]+" cleanup-startup\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := m.cleanup(); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, 1, 2, 5, 6} {
		if _, err := os.Stat(m.path(artifacts[index])); err != nil {
			t.Fatal("retained release removed", index, err)
		}
	}
	for _, index := range []int{3, 4} {
		if _, err := os.Stat(m.path(artifacts[index])); !os.IsNotExist(err) {
			t.Fatal("old release retained", index, err)
		}
	}
	for _, path := range []string{filepath.Join(snapshots, decision.Snapshot.Digest+".snapshot"), filepath.Join(snapshots, "notes"), filepath.Join(snapshots, ".incoming-link"), outside} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatal("protected file removed", path, err)
		}
	}
	for _, path := range []string{filepath.Join(snapshots, orphan), filepath.Join(snapshots, ".incoming-orphan"), filepath.Join(dir, "handoff", ".snapshot-orphan")} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("orphan retained", path, err)
		}
	}
	if err := handoff.VerifySnapshot(dir, decision.Snapshot); err != nil {
		t.Fatal(err)
	}
	if err := m.cleanup(); err != nil {
		t.Fatal("repeat cleanup", err)
	}
	if selected, err := SelectArtifact(dir, m.path(artifacts[0]), m.path(ReleasesDir), uint32(os.Geteuid())); err != nil || selected != m.path(artifacts[1]) {
		t.Fatal("cleanup broke cold start", selected, err)
	}
}

func TestCleanupRefusesActiveRecoveryOrDamagedState(t *testing.T) {
	for _, mode := range []string{"active owner", "pending handoff", "pending rollout", "corrupt decision", "missing binary", "corrupt snapshot"} {
		t.Run(mode, func(t *testing.T) {
			m, artifacts, decision := cleanupFixture(t)
			dir := m.path(filepath.Dir(dataPath))
			switch mode {
			case "active owner", "pending handoff":
				o, err := handoff.Acquire(dir, decision.Owner)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { o.Close() })
				if mode == "pending handoff" {
					if err = o.Prepare("next", "next", handoff.Identity{Incarnation: "c", Release: filepath.Base(filepath.Dir(artifacts[2]))}); err != nil {
						t.Fatal(err)
					}
					if err = o.Close(); err != nil {
						t.Fatal(err)
					}
				}
			case "pending rollout":
				j, err := state.Open(filepath.Join(dir, "release"))
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(rollout.Report{Phase: "VERIFYING"})
				if err = j.Replace("process/", map[string]json.RawMessage{"process/report": raw}); err != nil {
					t.Fatal(err)
				}
				if err = j.Close(); err != nil {
					t.Fatal(err)
				}
			case "corrupt decision":
				if err := os.WriteFile(filepath.Join(dir, "handoff", "state.log"), []byte("bad\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing binary":
				if err := os.Remove(m.path(artifacts[1])); err != nil {
					t.Fatal(err)
				}
			case "corrupt snapshot":
				if err := os.WriteFile(filepath.Join(dir, "handoff", "snapshots", decision.Snapshot.Digest+".snapshot"), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.cleanup(); err == nil {
				t.Fatal("unsafe cleanup accepted")
			}
			if _, err := os.Stat(m.path(artifacts[3])); err != nil {
				t.Fatal("refused cleanup still deleted a release", err)
			}
		})
	}
}

func TestCleanupKeepsUnknownAndSymlinkReleaseDirectories(t *testing.T) {
	m, artifacts, _ := cleanupFixture(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(m.path(artifacts[3])), "user-file"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	path := filepath.Join(m.path(ReleasesDir), strings.Repeat("e", 64))
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if err := m.cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(m.path(artifacts[3])); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreManagedCleanupHookUsesInstallerButKeepsLaunchSeed(t *testing.T) {
	m, artifacts, _ := cleanupFixture(t)
	m.command = func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	if err := m.writeProcessUnits(artifacts[0]); err != nil {
		t.Fatal(err)
	}
	// Existing installs created before housekeeping have no maintenance hook.
	raw, err := os.ReadFile(m.path(unitPath))
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("ExecStartPre=+"+artifacts[0]+" cleanup-startup\n"), nil)
	if err = os.WriteFile(m.path(unitPath), raw, 0644); err != nil {
		t.Fatal(err)
	}
	m.executable = m.path(artifacts[1])
	if err = m.restoreRolloutUnits(Deployment{InitialRelease: artifacts[0]}); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(m.path(unitPath))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("ExecStart="+artifacts[0]+" launch")) || !bytes.Contains(raw, []byte("ExecStartPre=+"+artifacts[1]+" cleanup-startup")) {
		t.Fatal("wrong launcher or maintenance artifact", string(raw))
	}
	custom := append(raw, []byte("\n# operator customization\n")...)
	if err = os.WriteFile(m.path(unitPath), custom, 0644); err != nil {
		t.Fatal(err)
	}
	m.executable = m.path(artifacts[2])
	if err = m.restoreRolloutUnits(Deployment{InitialRelease: artifacts[0]}); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(m.path(unitPath))
	if err != nil || !bytes.Equal(raw, custom) {
		t.Fatal("customized unit overwritten", err)
	}
}
