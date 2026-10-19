package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/state"
)

// Updating the host requires a stopped service. Ordinary install only replaces
// business workers and leaves the serving host and state owner in place.
func (m *manager) updateHost(ctx context.Context, artifact string, old Deployment) error {
	unit := Name + ".service"
	out, err := m.command(ctx, "systemctl", "show", "--property=ActiveState", "--value", unit)
	if err != nil {
		return fmt.Errorf("cannot confirm %s is stopped: %w", unit, err)
	}
	switch strings.TrimSpace(string(out)) {
	case "inactive", "failed":
	default:
		return fmt.Errorf("host update requires the service stopped after traffic drain; %s is %s", unit, strings.TrimSpace(string(out)))
	}

	controlDir := filepath.Join(filepath.Dir(dataPath), "release")
	j, err := state.Open(m.path(controlDir))
	if err != nil {
		return err
	}
	var report rollout.Report
	if raw := j.Snapshot()["controller/report"]; raw != nil {
		if err = json.Unmarshal(raw, &report); err != nil {
			j.Close()
			return err
		}
	}
	j.Close()
	switch report.Phase {
	case "", "COMPLETED", "ROLLED_BACK", "FAILED":
	default:
		return fmt.Errorf("resolve rollout %s (%s) before host update", report.ID, report.Phase)
	}
	backup := m.path(filepath.Join("/var/lib/aiagent-gateway-backups", time.Now().UTC().Format("20060102T150405.000000000Z")))
	if err = os.MkdirAll(backup, 0700); err != nil {
		return err
	}
	// Preserve a complete stopped-state backup, including recovery transactions.
	for _, path := range []string{"/etc/aiagent-gateway", filepath.Dir(dataPath), binaryPath} {
		if err = backupPath(m.path(path), filepath.Join(backup, strings.TrimPrefix(path, "/"))); err != nil {
			return fmt.Errorf("backup: %w", err)
		}
	}
	for _, name := range []string{Name + ".service", Name + "-init.service"} {
		for _, suffix := range []string{"", ".d"} {
			path := "/etc/systemd/system/" + name + suffix
			if err = backupPath(m.path(path), filepath.Join(backup, strings.TrimPrefix(path, "/"))); err != nil {
				return err
			}
		}
	}
	fmt.Fprintf(m.output, "Maintenance backup: %s\n", backup)
	if err = m.account(ctx); err != nil {
		return err
	}
	binary, err := os.ReadFile(m.executable)
	if err != nil {
		return err
	}
	infra := filepath.Join("/opt/aiagent-gateway/infrastructure", filepath.Base(filepath.Dir(artifact)), "gateway")
	if err = atomicWrite(m.path(infra), binary, 0755); err != nil {
		return err
	}
	// Establish the uploaded binary as the new baseline. Archive stale process
	// identities while retaining configuration, business state and unit overrides.
	for _, name := range []string{"entry", "release"} {
		path := m.path(filepath.Join(filepath.Dir(dataPath), name))
		if _, e := os.Lstat(path); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return e
		}
		if err = os.Rename(path, filepath.Join(backup, "previous-"+name)); err != nil {
			return err
		}
	}
	if err = m.writeHostUnits(infra); err != nil {
		return err
	}
	if err = atomicWrite(m.path(binaryPath), binary, 0755); err != nil {
		return err
	}
	next := Deployment{Infrastructure: infra, InitialRelease: artifact, Policy: old.Policy}
	raw, _ := json.MarshalIndent(next, "", "  ")
	if err = atomicWrite(m.path(DeploymentPath), raw, 0600); err != nil {
		return err
	}
	if err = m.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if err = m.run(ctx, "systemctl", "enable", "--now", Name+".service"); err != nil {
		return fmt.Errorf("host start failed; stopped-state backup is %s: %w", backup, err)
	}
	token, err := adminToken(m.path(configPath))
	if err != nil {
		return err
	}
	client := rollout.NewClient(m.path(ControllerSocket), token)
	defer client.HTTP.CloseIdleConnections()
	if err = waitRolloutFor(ctx, client, m.output, "", artifact); err != nil {
		return fmt.Errorf("host update incomplete; backup is %s: %w", backup, err)
	}
	return nil
}

func backupPath(source, dest string) error {
	info, err := os.Lstat(source)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err = os.MkdirAll(dest, 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = backupPath(filepath.Join(source, entry.Name()), filepath.Join(dest, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing non-regular backup source %s", source)
	}
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return atomicWrite(dest, raw, 0600)
}

func (m *manager) writeHostUnits(infrastructure string) error {
	for _, name := range []string{Name + ".service", Name + "-init.service"} {
		raw, err := units.ReadFile("units/" + name)
		if err != nil {
			return err
		}
		raw = []byte(strings.ReplaceAll(string(raw), "@BINARY@", infrastructure))
		if err := atomicWrite(m.path("/etc/systemd/system/"+name), raw, 0644); err != nil {
			return err
		}
	}
	return nil
}
