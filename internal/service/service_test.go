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
	m               *manager
	active, enabled bool
	calls           []string
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
	case "show":
		if f.active {
			return []byte("active\n"), nil
		}
		return []byte("inactive\n"), nil
	case "is-active":
		if !f.active {
			return nil, errors.New("inactive")
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
	case "stop":
		f.active = false
	case "enable":
		f.enabled = true
	case "disable":
		f.enabled = false
		if len(args) > 1 && args[1] == "--now" {
			f.active = false
		}
	case "daemon-reload":
	default:
		return nil, errors.New("unexpected systemctl action")
	}
	return nil, nil
}
func read(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestMissingDataRejected(t *testing.T) {
	m, _ := rolloutManager(t)
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	completedInstallServer(t, m, artifact)
	if err := m.installRollout(context.Background()); err != nil {
		t.Fatal(err)
	}
	os.Remove(m.path(dataPath))
	if err := m.installRollout(context.Background()); err == nil {
		t.Fatal("missing state recreated")
	}
	if ok, _ := exists(m.path(dataPath)); ok {
		t.Fatal("data recreated")
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
	m, _ := rolloutManager(t)
	p := m.path(unitPath)
	os.MkdirAll(filepath.Dir(p), 0755)
	os.Symlink(m.executable, p)
	if err := m.installRollout(context.Background()); err == nil {
		t.Fatal("symlink replaced")
	}
}

func TestFirstInstallDoesNotResetUnloadedUnit(t *testing.T) {
	m, f := rolloutManager(t)
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	completedInstallServer(t, m, artifact)
	if err := m.installRollout(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.calls {
		if strings.Contains(call, "reset-failed") {
			t.Fatal("reset unloaded unit", call)
		}
	}
}
func TestAccountCannotBeRoot(t *testing.T) {
	m, f := rolloutManager(t)
	m.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "getent" {
			return []byte("aiagent-gateway:x:0:"), nil
		}
		return f.run(ctx, name, args...)
	}
	if err := m.installRollout(context.Background()); err == nil {
		t.Fatal("root account accepted")
	}
	if ok, _ := exists(m.path(binaryPath)); ok {
		t.Fatal("installed before account validation")
	}
}
