package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestYAMLCLIInitializationAndEnvironmentRemoval(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(config, []byte("admin_token: admin-yaml\ndata_dir: data\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GATEWAY_DATA_DIR", filepath.Join(dir, "wrong-data"))
	t.Setenv("GATEWAY_ADMIN_TOKEN", "ignored")
	t.Setenv("GATEWAY_INIT", "true")
	if code := runForeground([]string{"--config", config, "--init-only"}); code != 0 {
		t.Fatal("YAML initialization failed", code)
	}
	for _, name := range []string{"config.json", "routes.json", "route-logs.json"} {
		if _, err := os.Stat(filepath.Join(dir, "data", name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "wrong-data")); !os.IsNotExist(err) {
		t.Fatal("environment data path used")
	}
	if code := runForeground([]string{"--config", config, "--init-only"}); code != 0 {
		t.Fatal("revalidation failed", code)
	}
	t.Chdir(dir)
	if code := runForeground([]string{"--init-only"}); code != 0 {
		t.Fatal("default config.yaml was not loaded", code)
	}
	if err := os.WriteFile(filepath.Join(dir, "data/config.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := runForeground([]string{"--config", config, "--init-only"}); code != 1 {
		t.Fatal("corrupt route store overwritten", code)
	}
	for _, role := range []string{"worker", "state-server", "entry", "rollout-controller"} {
		err := serveRole(role, []string{"--config", filepath.Join(dir, "absent.yaml")})
		if err == nil || !strings.Contains(err.Error(), "open YAML config") {
			t.Fatal("role ignored config path", role, err)
		}
	}
}

func TestInitFlagRemoved(t *testing.T) {
	if code := runForeground([]string{"--init"}); code != 2 {
		t.Fatal("deprecated init flag accepted", code)
	}
}
