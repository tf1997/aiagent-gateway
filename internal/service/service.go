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
	case "install", "update-host":
		return m.installRollout(ctx, action == "update-host")
	case "uninstall":
		return m.uninstallRollout(ctx)
	case "start", "stop", "restart":
		return m.run(ctx, "systemctl", action, Name+".service")
	default:
		return fmt.Errorf("unknown service command %q", action)
	}
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
