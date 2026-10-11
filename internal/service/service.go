// Package service installs and manages this executable as a Linux systemd service.
package service

import (
	"aiagent-gateway/internal/appconfig"
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"go.yaml.in/yaml/v3"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//go:embed units/*.service
var units embed.FS

const Name = "aiagent-gateway"
const binaryPath = "/usr/local/bin/aiagent-gateway"
const configPath = appconfig.SystemPath
const dataPath = "/var/lib/aiagent-gateway/config.json"
const unitPath = "/etc/systemd/system/aiagent-gateway.service"
const initPath = "/etc/systemd/system/aiagent-gateway-init.service"

type runner func(context.Context, string, ...string) ([]byte, error)
type manager struct {
	root, executable string
	command          runner
	output           io.Writer
}

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
func (m *manager) path(p string) string {
	if m.root == "" {
		return p
	}
	return filepath.Join(m.root, p)
}
func (m *manager) run(ctx context.Context, name string, args ...string) error {
	out, err := m.command(ctx, name, args...)
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Manage never executes shell snippets or sources configuration as shell code.
func Manage(action string, output io.Writer) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("systemd service management requires Linux")
	}
	if action != "status" && os.Geteuid() != 0 {
		return fmt.Errorf("run this command with sudo or as root")
	}
	if _, err := os.Stat("/run/systemd/system"); err != nil {
		return fmt.Errorf("systemd must be running as the service manager")
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	m := &manager{executable: executable, command: command, output: output}
	if action == "status" {
		out, err := m.command(ctx, "systemctl", "status", "--no-pager", "--full", Name+".service")
		_, _ = output.Write(out)
		return err
	}
	lock, err := os.OpenFile("/run/lock/aiagent-gateway-install.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("another service management command is running: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	switch action {
	case "install", "migrate":
		return m.installRollout(ctx, action == "migrate")
	case "uninstall":
		if _, err := os.Stat(m.path(DeploymentPath)); err == nil {
			return m.uninstallRollout(ctx)
		}
		return m.uninstall(ctx)
	case "start", "stop", "restart":
		return m.run(ctx, "systemctl", action, Name+".service")
	default:
		return fmt.Errorf("unknown service command %q", action)
	}
}

// A newly installed unit may not yet be loaded, so reset-failed is only
// appropriate when systemd actually reports a failed unit.
func (m *manager) clearFailed(ctx context.Context) error {
	if _, err := m.command(ctx, "systemctl", "is-failed", "--quiet", Name+".service"); err != nil {
		return nil
	}
	return m.run(ctx, "systemctl", "reset-failed", Name+".service")
}
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}
func (m *manager) account(ctx context.Context) error {
	for _, kind := range []string{"group", "passwd"} {
		out, err := m.command(ctx, "getent", kind, Name)
		if err == nil {
			fields := strings.Split(strings.TrimSpace(string(out)), ":")
			if len(fields) < 3 {
				return fmt.Errorf("invalid %s entry for %s", kind, Name)
			}
			id, e := strconv.Atoi(fields[2])
			if e != nil || id <= 0 {
				return fmt.Errorf("%s must have a non-root %s ID", Name, kind)
			}
			continue
		}
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 {
			return fmt.Errorf("getent %s: %w", kind, err)
		}
		if kind == "group" {
			err = m.run(ctx, "groupadd", "--system", Name)
		} else {
			err = m.run(ctx, "useradd", "--system", "--gid", Name, "--home-dir", "/var/lib/aiagent-gateway", "--no-create-home", "--shell", "/usr/sbin/nologin", Name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type savedFile struct {
	path   string
	data   []byte
	mode   os.FileMode
	exists bool
}

func snapshot(path string) (savedFile, error) {
	s := savedFile{path: path}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	if !info.Mode().IsRegular() {
		return s, fmt.Errorf("refusing to replace non-regular file: %s", path)
	}
	s.mode = info.Mode().Perm()
	s.exists = true
	s.data, err = os.ReadFile(path)
	return s, err
}
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".gateway-install-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func credentials() ([]byte, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	c := appconfig.Defaults()
	c.DataDir = "/var/lib/aiagent-gateway"
	c.PublicURL = "http://localhost:8080"
	c.AdminToken = hex.EncodeToString(b[:])
	return yaml.Marshal(c)
}
func (m *manager) install(ctx context.Context) (result error) {
	configExists, err := exists(m.path(configPath))
	if err != nil {
		return err
	}
	dataExists, err := exists(m.path(dataPath))
	if err != nil {
		return err
	}
	binExists, err := exists(m.path(binaryPath))
	if err != nil {
		return err
	}
	fresh := !configExists
	if fresh && (dataExists || binExists) {
		return fmt.Errorf("existing installation has no %s; restore it before upgrading", configPath)
	}
	if !fresh && !dataExists {
		return fmt.Errorf("existing installation has no %s; restore it before upgrading", dataPath)
	}
	if configExists {
		if _, err := snapshot(m.path(configPath)); err != nil {
			return err
		}
	}
	binary, err := os.ReadFile(m.executable)
	if err != nil {
		return err
	}
	saved := []savedFile{}
	for _, path := range []string{binaryPath, unitPath, initPath} {
		s, err := snapshot(m.path(path))
		if err != nil {
			return err
		}
		saved = append(saved, s)
	}
	mainUnit, err := units.ReadFile("units/aiagent-gateway.service")
	if err != nil {
		return err
	}
	initUnit, err := units.ReadFile("units/aiagent-gateway-init.service")
	if err != nil {
		return err
	}
	if err = m.account(ctx); err != nil {
		return err
	}
	_, activeErr := m.command(ctx, "systemctl", "is-active", "--quiet", Name+".service")
	wasActive := activeErr == nil
	_, enabledErr := m.command(ctx, "systemctl", "is-enabled", "--quiet", Name+".service")
	wasEnabled := enabledErr == nil
	changed := false
	defer func() {
		if result == nil || !changed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		rollbackErrors := []error{}
		// Surface stop failures along with any file restoration errors.
		if err := m.run(rollbackCtx, "systemctl", "stop", Name+".service"); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
		if !wasEnabled {
			if present, _ := exists(m.path(unitPath)); present {
				if err := m.run(rollbackCtx, "systemctl", "disable", Name+".service"); err != nil {
					rollbackErrors = append(rollbackErrors, err)
				}
			}
		}
		for _, s := range saved {
			var err error
			if s.exists {
				err = atomicWrite(s.path, s.data, s.mode)
			} else {
				err = os.Remove(s.path)
				if errors.Is(err, os.ErrNotExist) {
					err = nil
				}
			}
			if err != nil {
				rollbackErrors = append(rollbackErrors, err)
			}
		}
		if fresh {
			if present, err := exists(m.path(dataPath)); err == nil && !present {
				if err := os.Remove(m.path(configPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
					rollbackErrors = append(rollbackErrors, err)
				}
			}
		}
		if err := m.run(rollbackCtx, "systemctl", "daemon-reload"); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
		if wasActive {
			if err := m.clearFailed(rollbackCtx); err != nil {
				rollbackErrors = append(rollbackErrors, err)
			}
			if err := m.run(rollbackCtx, "systemctl", "restart", Name+".service"); err != nil {
				rollbackErrors = append(rollbackErrors, err)
			}
		}
		if len(rollbackErrors) > 0 {
			result = errors.Join(result, fmt.Errorf("rollback errors: %w", errors.Join(rollbackErrors...)))
		} else {
			result = fmt.Errorf("%w (previous binary and units restored; existing data retained)", result)
		}
	}()
	if fresh {
		raw, err := credentials()
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Dir(m.path(configPath)), 0700); err != nil {
			return err
		}
		changed = true
		if err = atomicWrite(m.path(configPath), raw, 0600); err != nil {
			return err
		}
	}
	if wasActive {
		if err = m.run(ctx, "systemctl", "stop", Name+".service"); err != nil {
			return err
		}
	}
	changed = true
	for i, data := range [][]byte{binary, mainUnit, initUnit} {
		mode := os.FileMode(0644)
		if i == 0 {
			mode = 0755
		}
		if err = atomicWrite(saved[i].path, data, mode); err != nil {
			return err
		}
	}
	if err = m.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if err = m.clearFailed(ctx); err != nil {
		return err
	}
	if fresh {
		if err = m.run(ctx, "systemctl", "start", Name+"-init.service"); err != nil {
			return err
		}
	}
	if err = m.run(ctx, "systemctl", "restart", Name+".service"); err != nil {
		return err
	}
	if err = m.run(ctx, "systemctl", "is-active", "--quiet", Name+".service"); err != nil {
		return err
	}
	if err = m.run(ctx, "systemctl", "enable", Name+".service"); err != nil {
		return err
	}
	fmt.Fprintf(m.output, "Installed and enabled %s.service\nConfiguration and credentials: %s (root only)\nState: /var/lib/aiagent-gateway\nLogs: journalctl -u %s -f\n", Name, configPath, Name)
	return nil
}
func (m *manager) uninstall(ctx context.Context) error {
	if err := m.run(ctx, "systemctl", "disable", "--now", Name+".service"); err != nil {
		return err
	}
	for _, path := range []string{unitPath, initPath, binaryPath} {
		if err := os.Remove(m.path(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := m.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	fmt.Fprintln(m.output, "Service removed; credentials, route data, and service account retained.")
	return nil
}
