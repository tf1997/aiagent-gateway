package service

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"aiagent-gateway/internal/handoff"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/state"
)

func releaseDigest(name string) bool {
	_, err := hex.DecodeString(name)
	return err == nil && len(name) == 64 && strings.ToLower(name) == name
}

// cleanup runs only while the service is stopped, under the install and owner
// locks. An unresolved release is left intact for cold-start recovery. Keep the
// immutable launcher, the current decision/report references, this executable,
// and two additional recent releases for an operator to publish again.
func (m *manager) cleanup() error {
	deployment, err := LoadDeployment(m.path(DeploymentPath))
	if err != nil {
		return err
	}
	if err = validateSystemConfig(m.path(configPath)); err != nil {
		return err
	}
	dir := m.path(filepath.Dir(dataPath))
	fd, err := syscall.Open(filepath.Join(dir, "owner.lock"), syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return nil // A new installation has nothing to collect.
	}
	if err != nil {
		return err
	}
	lock := os.NewFile(uintptr(fd), "cleanup-owner")
	defer lock.Close()
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("cleanup requires a stopped gateway: %w", err)
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	decision, err := handoff.ReadDecision(dir)
	if err != nil {
		return err
	}
	if decision.Phase != handoff.Stable && decision.Phase != handoff.Completed && decision.Phase != handoff.Aborted {
		return fmt.Errorf("cleanup refused: handoff %s requires recovery", decision.Phase)
	}
	values, err := state.ReadSnapshot(filepath.Join(dir, "release"))
	if err != nil {
		return err
	}
	var report rollout.Report
	if raw := values["process/report"]; len(raw) == 0 {
		return fmt.Errorf("cleanup requires a durable process report")
	} else if err = json.Unmarshal(raw, &report); err != nil {
		return err
	}
	if (report.Phase != "COMPLETED" && report.Phase != "ROLLED_BACK" && report.Phase != "FAILED") || report.Candidate != nil || report.Retiring != nil || report.Intent != nil || report.Stable.Release != decision.ColdStart {
		return fmt.Errorf("cleanup refused: rollout requires recovery")
	}
	root := m.path(ReleasesDir)
	keep := map[string]bool{decision.ColdStart: true, decision.Source.Release: true, decision.Target.Release: true}
	retained := []string{deployment.InitialRelease, report.Stable.Artifact, report.Artifact}
	if raw, err := os.ReadFile(m.path(unitPath)); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "ExecStartPre=+"+ReleasesDir+"/") && strings.HasSuffix(line, "/gateway cleanup-startup") {
				retained = append(retained, strings.TrimSuffix(strings.TrimPrefix(line, "ExecStartPre=+"), " cleanup-startup"))
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, artifact := range retained {
		if artifact == "" {
			continue
		}
		if filepath.Clean(artifact) != artifact || filepath.Dir(filepath.Dir(artifact)) != ReleasesDir || filepath.Base(artifact) != "gateway" || !releaseDigest(filepath.Base(filepath.Dir(artifact))) {
			return fmt.Errorf("cleanup refused: invalid retained artifact")
		}
		keep[filepath.Base(filepath.Dir(artifact))] = true
	}
	if filepath.Dir(filepath.Dir(m.executable)) == root {
		keep[filepath.Base(filepath.Dir(m.executable))] = true
	}
	// Validate all required binaries before deleting anything. Missing/corrupt
	// recovery references must never be obscured by housekeeping.
	for digest := range keep {
		if digest != "" {
			if err = ValidateArtifact(root, filepath.Join(root, digest, "gateway"), uint32(os.Geteuid())); err != nil {
				return err
			}
		}
	}
	if decision.Snapshot.Digest != "" {
		if err = handoff.VerifySnapshot(dir, decision.Snapshot); err != nil {
			return fmt.Errorf("cleanup refused: referenced snapshot is invalid: %w", err)
		}
	}
	count, err := pruneReleases(root, keep)
	if err != nil {
		return err
	}
	files := 0
	for _, sub := range []string{"shared", "entry", "release", "handoff", "handoff/snapshots"} {
		n, err := pruneFiles(filepath.Join(dir, sub), func(name string) bool {
			if sub == "handoff/snapshots" {
				return strings.HasPrefix(name, ".incoming-") || strings.HasSuffix(name, ".snapshot") && releaseDigest(strings.TrimSuffix(name, ".snapshot")) && name != decision.Snapshot.Digest+".snapshot"
			}
			return strings.HasPrefix(name, ".snapshot-")
		})
		files += n
		if err != nil {
			return err
		}
	}
	fmt.Fprintf(m.output, "cleanup removed %d obsolete releases and %d unreferenced state files\n", count, files)
	return nil
}

func pruneReleases(path string, keep map[string]bool) (int, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return 0, err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return 0, err
	}
	type release struct {
		name string
		info os.FileInfo
	}
	var eligible []release
	for _, entry := range entries {
		if keep[entry.Name()] || !releaseDigest(entry.Name()) || !entry.IsDir() {
			continue
		}
		info, err := root.Lstat(entry.Name())
		if err != nil {
			return 0, err
		}
		if err = ValidateArtifact(path, filepath.Join(path, entry.Name(), "gateway"), uint32(os.Geteuid())); err != nil {
			continue // Unknown or damaged content is retained for diagnosis.
		}
		child, err := root.Open(entry.Name())
		if err != nil {
			return 0, err
		}
		names, readErr := child.Readdirnames(-1)
		err = errors.Join(readErr, child.Close())
		if err != nil {
			return 0, err
		}
		if len(names) == 1 && names[0] == "gateway" {
			eligible = append(eligible, release{entry.Name(), info})
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].info.ModTime().Equal(eligible[j].info.ModTime()) {
			return eligible[i].name < eligible[j].name
		}
		return eligible[i].info.ModTime().After(eligible[j].info.ModTime())
	})
	removed := 0
	for i := 2; i < len(eligible); i++ {
		name := eligible[i].name
		if err = root.Remove(name + "/gateway"); err != nil {
			return removed, err
		}
		if err = root.Remove(name); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, dir.Sync()
}

func pruneFiles(path string, eligible func(string) bool) (int, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("cleanup refused non-directory: %s", path)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return 0, err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if !eligible(entry.Name()) {
			continue
		}
		info, err := root.Lstat(entry.Name())
		if err != nil {
			return removed, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err = root.Remove(entry.Name()); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, dir.Sync()
}
