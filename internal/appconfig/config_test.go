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
	path := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(path, []byte(minimal+"credentials:\n  upstream: 'literal-$UPSTREAM_KEY'\nstate_socket: run/state.sock\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminToken != "admin-secret" || c.Listen != ":8080" || c.MaxInflight != 4096 || c.Credentials["upstream"] != "literal-$UPSTREAM_KEY" {
		t.Fatal("YAML defaults/credentials were overridden")
	}
	if c.DataDir != filepath.Join(dir, "data") || c.StateSocket != filepath.Join(dir, "run/state.sock") {
		t.Fatal("relative paths or caller routes incorrect")
	}
	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("fell back to environment on missing file")
	}
}

func TestStrictYAMLValidationAndSecretRedaction(t *testing.T) {
	for _, input := range []string{
		"", "null", "[]", minimal + "unknown_field: true\n", minimal + "max_inflight: 0\n",
		minimal + "max_sessions: -1\n", minimal + "max_inflight: null\n", minimal + "listen: invalid\n", minimal + "public_url: not-a-url\n",
		minimal + "admin_token: duplicate-secret\n", minimal + "---\nadmin_token: second-document\n",
		minimal + "callers: []\n", minimal + "credentials: {upstream: ''}\n",
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

func TestCredentialMapAppliedWithoutEnvironmentFallback(t *testing.T) {
	t.Setenv("ONLY_IN_ENV", "env-secret")
	c, err := Parse([]byte(minimal + "credentials: {upstream: yaml-secret}\n"))
	if err != nil {
		t.Fatal(err)
	}
	opts := c.Options()
	if opts.Credentials["upstream"] != "yaml-secret" || opts.Credentials["ONLY_IN_ENV"] != "" {
		t.Fatal("credentials escaped YAML scope")
	}
}

func TestExampleConfig(t *testing.T) {
	if _, err := Load("../../gateway.example.yaml"); err != nil {
		t.Fatal(err)
	}
}
