package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestYAMLCLIInitializationAndEnvironmentRemoval(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(config, []byte("admin_token: admin-yaml\ndata_dir: data\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GATEWAY_DATA_DIR", filepath.Join(dir, "wrong-data"))
	t.Setenv("GATEWAY_ADMIN_TOKEN", "ignored")
	t.Setenv("GATEWAY_INIT", "true")
	if code := runForeground([]string{"--config", config}); code != 1 {
		t.Fatal("environment initialized missing route store", code)
	}
	if code := runForeground([]string{"--config", config, "--init-only"}); code != 0 {
		t.Fatal("YAML initialization failed", code)
	}
	if _, err := os.Stat(filepath.Join(dir, "data/config.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wrong-data")); !os.IsNotExist(err) {
		t.Fatal("environment data path used")
	}
	if code := runForeground([]string{"--config", config, "--init-only"}); code != 0 {
		t.Fatal("revalidation failed", code)
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
