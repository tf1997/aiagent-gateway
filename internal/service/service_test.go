package service

import (
	"aiagent-gateway/internal/appconfig"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeSystem struct {
	m                                    *manager
	active, enabled, failRestart, failed bool
	calls                                []string
}

func (f *fakeSystem) run(_ context.Context, cmd string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, cmd+" "+strings.Join(args, " "))
	if cmd == "getent" {
		return []byte("aiagent-gateway:x:999:999::/var/lib/aiagent-gateway:/usr/sbin/nologin\n"), nil
	}
	if cmd != "systemctl" {
		return nil, errors.New("unexpected command")
	}
	switch args[0] {
	case "is-active":
		if !f.active {
			return nil, errors.New("inactive")
		}
	case "is-enabled":
		if !f.enabled {
			return nil, errors.New("disabled")
		}
	case "is-failed":
		if !f.failed {
			return nil, errors.New("not failed or not loaded")
		}
	case "start":
		if args[1] == Name+"-init.service" {
			p := f.m.path(dataPath)
			if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
				return nil, err
			}
			if err := os.WriteFile(p, []byte(`{"schema_version":1,"routes":[]}`), 0600); err != nil {
				return nil, err
			}
		}
	case "restart":
		if f.failRestart {
			f.failRestart = false
			f.failed = true
			return nil, errors.New("start failed")
		}
		f.active = true
	case "stop":
		f.active = false
	case "enable":
		f.enabled = true
	case "disable":
		f.enabled = false
		if len(args) > 1 && args[1] == "--now" {
			f.active = false
		}
	case "reset-failed":
		if !f.failed {
			return nil, errors.New("unit not loaded")
		}
		f.failed = false
	case "daemon-reload":
	default:
		return nil, errors.New("unexpected systemctl action")
	}
	return nil, nil
}
func setupManager(t *testing.T) (*manager, *fakeSystem) {
	t.Helper()
	dir := t.TempDir()
	exe := filepath.Join(dir, "downloaded-gateway")
	if err := os.WriteFile(exe, []byte("binary-v1"), 0755); err != nil {
		t.Fatal(err)
	}
	m := &manager{root: filepath.Join(dir, "root"), executable: exe, output: &bytes.Buffer{}}
	f := &fakeSystem{m: m}
	m.command = f.run
	return m, f
}
func read(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestInstallUpgradeRollback(t *testing.T) {
	m, f := setupManager(t)
	ctx := context.Background()
	if err := m.install(ctx); err != nil {
		t.Fatal(err)
	}
	if !f.active || !f.enabled {
		t.Fatal("service not enabled and active")
	}
	originalEnv := read(t, m.path(configPath))
	originalState := read(t, m.path(dataPath))
	if bytes.Contains(originalEnv, []byte("GATEWAY_INIT=")) {
		t.Fatal("persistent initialization enabled")
	}
	info, _ := os.Stat(m.path(configPath))
	if info.Mode().Perm() != 0600 {
		t.Fatal("insecure credential permissions")
	}
	if !bytes.Contains(read(t, m.path(unitPath)), []byte("ExecStartPost=/usr/local/bin/aiagent-gateway --check-ready")) {
		t.Fatal("missing readiness check")
	}
	os.WriteFile(m.executable, []byte("binary-v2"), 0755)
	if err := m.install(ctx); err != nil {
		t.Fatal(err)
	}
	if string(read(t, m.path(binaryPath))) != "binary-v2" {
		t.Fatal("binary not upgraded")
	}
	if !bytes.Equal(originalEnv, read(t, m.path(configPath))) || !bytes.Equal(originalState, read(t, m.path(dataPath))) {
		t.Fatal("upgrade changed credentials or data")
	}
	os.WriteFile(m.executable, []byte("binary-v3"), 0755)
	f.failRestart = true
	if err := m.install(ctx); err == nil {
		t.Fatal("failed startup reported success")
	}
	if string(read(t, m.path(binaryPath))) != "binary-v2" || !f.active || !f.enabled {
		t.Fatal("rollback did not restore service")
	}
	if !bytes.Equal(originalEnv, read(t, m.path(configPath))) || !bytes.Equal(originalState, read(t, m.path(dataPath))) {
		t.Fatal("rollback changed state")
	}
}
func TestMissingDataRejected(t *testing.T) {
	m, _ := setupManager(t)
	if err := m.install(context.Background()); err != nil {
		t.Fatal(err)
	}
	os.Remove(m.path(dataPath))
	if err := m.install(context.Background()); err == nil {
		t.Fatal("missing state recreated")
	}
	if ok, _ := exists(m.path(dataPath)); ok {
		t.Fatal("data recreated")
	}
}
func TestUninstallRetainsData(t *testing.T) {
	m, f := setupManager(t)
	ctx := context.Background()
	if err := m.install(ctx); err != nil {
		t.Fatal(err)
	}
	original := read(t, m.path(dataPath))
	if err := m.uninstall(ctx); err != nil {
		t.Fatal(err)
	}
	if f.active || f.enabled {
		t.Fatal("service still running")
	}
	for _, path := range []string{binaryPath, unitPath, initPath} {
		if ok, _ := exists(m.path(path)); ok {
			t.Fatal("installed file retained", path)
		}
	}
	if !bytes.Equal(original, read(t, m.path(dataPath))) {
		t.Fatal("data lost")
	}
	read(t, m.path(configPath))
	// Reinstall after uninstall reuses the retained data and credentials.
	if err := m.install(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestFreshStartFailureCanRetry(t *testing.T) {
	m, f := setupManager(t)
	f.failRestart = true
	if err := m.install(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if f.enabled {
		t.Fatal("failed fresh install was enabled")
	}
	if err := m.install(context.Background()); err != nil {
		t.Fatal("cannot retry failed fresh install", err)
	}
}
func TestCredentialTokensDistinct(t *testing.T) {
	a, err := credentials()
	if err != nil {
		t.Fatal(err)
	}
	b, err := credentials()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("credentials reused")
	}
	c, err := appconfig.Parse(a)
	if err != nil || c.AdminToken == "" || bytes.Contains(a, []byte("callers:")) {
		t.Fatal("invalid YAML credentials", err)
	}
}

func TestUnitsUsePrivateYAMLConfig(t *testing.T) {
	files, err := units.ReadDir("units")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		raw, err := units.ReadFile("units/" + file.Name())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("EnvironmentFile=")) || !bytes.Contains(raw, []byte("LoadCredential=config.yaml:"+configPath)) || !bytes.Contains(raw, []byte("--config=%d/config.yaml")) {
			t.Fatal("unit does not use private YAML configuration", file.Name())
		}
	}
}
func TestSymlinkRejected(t *testing.T) {
	m, _ := setupManager(t)
	p := m.path(unitPath)
	os.MkdirAll(filepath.Dir(p), 0755)
	os.Symlink(m.executable, p)
	if err := m.install(context.Background()); err == nil {
		t.Fatal("symlink replaced")
	}
}

func TestFirstInstallDoesNotResetUnloadedUnit(t *testing.T) {
	m, f := setupManager(t)
	if err := m.install(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.calls {
		if strings.Contains(call, "reset-failed") {
			t.Fatal("reset unloaded unit", call)
		}
	}
}
func TestAccountCannotBeRoot(t *testing.T) {
	m, f := setupManager(t)
	m.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "getent" {
			return []byte("aiagent-gateway:x:0:"), nil
		}
		return f.run(ctx, name, args...)
	}
	if err := m.install(context.Background()); err == nil {
		t.Fatal("root account accepted")
	}
	if ok, _ := exists(m.path(binaryPath)); ok {
		t.Fatal("installed before account validation")
	}
}
