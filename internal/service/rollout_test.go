package service

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rolloutManager(t *testing.T) (*manager, *fakeSystem) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gateway-install-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	source := filepath.Join(dir, "candidate")
	os.WriteFile(source, []byte("new business binary"), 0755)
	m := &manager{root: filepath.Join(dir, "root"), executable: source, output: &bytes.Buffer{}}
	fake := &fakeSystem{m: m, active: true, enabled: true}
	m.command = fake.run
	return m, fake
}
func TestRolloutInstallPreservesServingVersionAndCredentials(t *testing.T) {
	m, system := rolloutManager(t)
	for path, body := range map[string]string{configPath: "data_dir: /var/lib/aiagent-gateway\nadmin_token: unchanged-secret\n", dataPath: `{"schema_version":2,"routes":[]}`, binaryPath: "old CLI"} {
		if err := atomicWrite(m.path(path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d := Deployment{Version: 2, Infrastructure: "/opt/aiagent-gateway/infrastructure/old/gateway", InitialRelease: "/opt/aiagent-gateway/releases/old/gateway", Policy: p.DefaultPolicy()}
	raw, _ := json.Marshal(d)
	atomicWrite(m.path(DeploymentPath), raw, 0600)
	if err := os.MkdirAll(filepath.Dir(m.path(ControllerSocket)), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", m.path(ControllerSocket))
	if err != nil {
		t.Fatal(err)
	}
	posted := make(chan string, 1)
	report := rollout.Report{Phase: "COMPLETED", Stable: p.Target{Release: "new"}}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer unchanged-secret" {
			t.Error("token changed")
		}
		if r.Method == "POST" {
			var q struct {
				Artifact string
				Policy   p.Policy
			}
			if json.NewDecoder(r.Body).Decode(&q) != nil {
				t.Error("invalid submission")
			}
			posted <- q.Artifact
		}
		json.NewEncoder(w).Encode(report)
	})}
	go srv.Serve(l)
	defer srv.Close()
	if err = m.installRollout(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	artifact := <-posted
	b, err := os.ReadFile(m.path(artifact))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if filepath.Base(filepath.Dir(artifact)) != hex.EncodeToString(sum[:]) {
		t.Fatal("release not immutable")
	}
	if string(read(t, m.path(configPath))) != "data_dir: /var/lib/aiagent-gateway\nadmin_token: unchanged-secret\n" || string(read(t, m.path(binaryPath))) != "old CLI" {
		t.Fatal("upgrade replaced stable infrastructure or credentials")
	}
	for _, call := range system.calls {
		if strings.Contains(call, " stop ") || strings.Contains(call, " restart ") {
			t.Fatal("upgrade interrupted serving process", call)
		}
	}
	if strings.Contains(m.output.(*bytes.Buffer).String(), "unchanged-secret") {
		t.Fatal("token exposed")
	}
}
func TestLegacyMigrationIsExplicit(t *testing.T) {
	m, system := rolloutManager(t)
	atomicWrite(m.path(configPath), []byte("GATEWAY_ADMIN_TOKEN=secret"), 0600)
	atomicWrite(m.path(dataPath), []byte(`{"schema_version":2}`), 0600)
	if err := m.installRollout(context.Background(), false); err == nil || !strings.Contains(err.Error(), "--migrate") {
		t.Fatal("legacy installation silently restarted", err)
	}
	if len(system.calls) != 0 {
		t.Fatal("legacy service was touched", system.calls)
	}
}
func TestReleaseStagingRejectsHashMutation(t *testing.T) {
	m, _ := rolloutManager(t)
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(m.path(artifact), []byte("changed"), 0755)
	if _, err = m.stageRelease(); err == nil {
		t.Fatal("mutated release accepted")
	}
}

func TestEnvironmentDeploymentRejectedBeforeMutation(t *testing.T) {
	m, system := rolloutManager(t)
	if err := atomicWrite(m.path("/etc/aiagent-gateway/gateway.env"), []byte("GATEWAY_ADMIN_TOKEN=old-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.installRollout(context.Background(), false); err == nil || !strings.Contains(err.Error(), "config.yaml") {
		t.Fatal("environment deployment accepted", err)
	}
	if len(system.calls) != 0 {
		t.Fatal("existing services changed", system.calls)
	}
	if _, err := os.Stat(m.path(configPath)); !os.IsNotExist(err) {
		t.Fatal("old credentials replaced")
	}
	if _, err := os.Stat(m.path(ReleasesDir)); !os.IsNotExist(err) {
		t.Fatal("release staged before migration check")
	}
}

func TestSystemYAMLRejectsUnsupportedPaths(t *testing.T) {
	m, _ := rolloutManager(t)
	raw, err := credentials()
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(m.path(configPath), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateSystemConfig(m.path(configPath)); err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte("data_dir: /var/lib/aiagent-gateway"), []byte("data_dir: ./different"), 1)
	if err := atomicWrite(m.path(configPath), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateSystemConfig(m.path(configPath)); err == nil {
		t.Fatal("installer accepted a path inconsistent with units")
	}
}

func TestOldYAMLInfrastructureRequiresMaintenance(t *testing.T) {
	m, system := rolloutManager(t)
	if err := atomicWrite(m.path("/etc/systemd/system/aiagent-gateway-state.service"), []byte("LoadCredential=gateway.yaml:/etc/aiagent-gateway/gateway.yaml\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := m.installRollout(context.Background(), false); err == nil || !strings.Contains(err.Error(), "route tokens") {
		t.Fatal("accepted incompatible infrastructure", err)
	}
	if len(system.calls) != 0 {
		t.Fatal("existing services changed", system.calls)
	}
	if _, err := os.Stat(m.path(ReleasesDir)); !os.IsNotExist(err) {
		t.Fatal("staged a release before migration check")
	}
}
func TestRolloutUnitsBindImmutableRelease(t *testing.T) {
	raw, err := units.ReadFile("units/aiagent-gateway-worker.service")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "ExecStart=@BINARY@ worker") || strings.Contains(text, "/usr/local/bin/") || !strings.Contains(text, "BindsTo=aiagent-gateway-state.service") {
		t.Fatal("worker process identity is not stable")
	}
	for _, name := range []string{"entry", "state", "rollout"} {
		raw, err = units.ReadFile("units/aiagent-gateway-" + name + ".service")
		if err != nil || !strings.Contains(string(raw), "PartOf=aiagent-gateway.service") {
			t.Fatal("service group incomplete", name)
		}
	}
}
func TestUninstallKeepsRecoveryArtifacts(t *testing.T) {
	m, _ := rolloutManager(t)
	for path, body := range map[string]string{DeploymentPath: `{"version":1}`, configPath: "secret", dataPath: "config", binaryPath: "CLI", unitPath: masterUnit} {
		atomicWrite(m.path(path), []byte(body), 0600)
	}
	if err := m.uninstallRollout(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{DeploymentPath, configPath, dataPath} {
		if _, err := os.Stat(m.path(path)); err != nil {
			t.Fatal("recovery data removed", path)
		}
	}
	if _, err := os.Stat(m.path(binaryPath)); !os.IsNotExist(err) {
		t.Fatal("CLI retained")
	}
}
