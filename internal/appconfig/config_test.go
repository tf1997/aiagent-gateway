package appconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimal = "admin_token: admin-secret\n"

func TestLoadYAMLDefaultsPathsAndEnvironmentIgnored(t *testing.T) {
	t.Setenv("GATEWAY_ADMIN_TOKEN", "environment-must-not-win")
	t.Setenv("GATEWAY_LISTEN", "invalid")
	t.Setenv("UPSTREAM_KEY", "environment-upstream")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(minimal+"state_socket: run/state.sock\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminToken != "admin-secret" || c.Port != "8080" || c.MaxInflight != 4096 {
		t.Fatal("YAML defaults were overridden")
	}
	if c.DataDir != filepath.Join(dir, "data") || c.StateSocket != filepath.Join(dir, "run/state.sock") {
		t.Fatal("relative paths or caller routes incorrect")
	}
	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("fell back to environment on missing file")
	}
}

func TestListenerPorts(t *testing.T) {
	for _, tt := range []struct{ name, config, address string }{
		{"defaults", "", ":8080"},
		{"numeric_port", "port: 18080\n", ":18080"},
		{"quoted_port", "port: \"18080\"\n", ":18080"},
		{"minimum_port", "port: 1\n", ":1"},
		{"maximum_port", "port: 65535\n", ":65535"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Parse([]byte(minimal + tt.config))
			if err != nil {
				t.Fatal(err)
			}
			if got := c.ListenAddress(); got != tt.address {
				t.Errorf("HTTP listener = %q, want %q (all interfaces)", got, tt.address)
			}
		})
	}
	for _, value := range []string{"0", "-1", "65536", `""`, "true", "8080.5", `"+8080"`, `" 8080 "`, `":8080"`, `"127.0.0.1:8080"`, `"[::1]:8080"`} {
		if _, err := Parse([]byte(minimal + "port: " + value + "\n")); err == nil {
			t.Fatalf("accepted invalid port: %s", value)
		}
	}
}

func TestStrictYAMLValidationAndSecretRedaction(t *testing.T) {
	for _, input := range []string{
		"", "null", "[]", minimal + "unknown_field: true\n", minimal + "admin_port: 9090\n", minimal + "max_inflight: 0\n",
		minimal + "max_sessions: -1\n", minimal + "max_inflight: null\n", minimal + "port: invalid\n", minimal + "public_url: not-a-url\n",
		minimal + "admin_token: duplicate-secret\n", minimal + "---\nadmin_token: second-document\n",
		minimal + "callers: []\n", minimal + "credentials: {upstream: ''}\n",
		minimal + "credentials: {upstream: secret-value}\n", minimal + "credentials: {}\n",
		minimal + "data_dir: ''\n", minimal + "callers: [{id: app, token: secret-value}]\n",
		minimal + "max_inflight: super-sensitive-secret\n",
	} {
		_, err := Parse([]byte(input))
		if err == nil {
			t.Fatalf("accepted invalid YAML: %q", input)
		}
		if strings.Contains(err.Error(), "super-sensitive-secret") || strings.Contains(err.Error(), "secret-value") {
			t.Fatal("parser exposed secret", err)
		}
	}
}

func TestExampleConfig(t *testing.T) {
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Fatal(err)
	}
}
