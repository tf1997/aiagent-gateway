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

func TestPolicyForConfigStorageFormats(t *testing.T) {
	for _, tc := range []struct {
		name, metadata, routes, want string
	}{
		{"legacy-schema-1", `{"schema_version":1,"routes":[{"protocol":"proxy"},{"protocol":"a2a","agent_card":{"capabilities":{"streaming":true}}}]}`, "", "http,a2a,a2a_sse"},
		{"legacy-schema-2", `{"schema_version":2,"routes":[{"agent_type":"hermes","expose":["proxy","a2a"],"agent_card":{"capabilities":{"streaming":true}}}]}`, "", "http,sse,a2a,a2a_sse"},
		{"split", `{"storage_version":1,"schema_version":2,"config_revision":7}`, `{"storage_version":1,"schema_version":2,"config_revision":7,"routes":[{"agent_type":"hermes","expose":["proxy","a2a"],"agent_card":{"capabilities":{"streaming":true}}}]}`, "http,sse,a2a,a2a_sse"},
		{"split-disabled", `{"storage_version":1,"schema_version":2,"config_revision":7}`, `{"storage_version":1,"schema_version":2,"config_revision":7,"routes":[{"enabled":false,"agent_type":"hermes","expose":["proxy","a2a"],"agent_card":{"capabilities":{"streaming":true}}},{"enabled":true,"expose":["proxy"]}]}`, "http"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			if err := os.WriteFile(path, []byte(tc.metadata), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.routes != "" {
				if err := os.WriteFile(filepath.Join(dir, "routes.json"), []byte(tc.routes), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := PolicyForConfig(p.DefaultPolicy(), path)
			if err != nil || strings.Join(got.Profiles, ",") != tc.want {
				t.Fatalf("profiles = %v, want %s: %v", got.Profiles, tc.want, err)
			}
		})
	}
}

func TestPolicyForConfigRejectsIncompleteSplitStore(t *testing.T) {
	for _, tc := range []struct {
		name, routes string
	}{
		{"missing", ""},
		{"corrupt", "broken"},
		{"revision", `{"storage_version":1,"schema_version":2,"config_revision":8,"routes":[]}`},
		{"storage", `{"storage_version":2,"schema_version":2,"config_revision":7,"routes":[]}`},
		{"schema", `{"storage_version":1,"schema_version":1,"config_revision":7,"routes":[]}`},
		{"missing-routes", `{"storage_version":1,"schema_version":2,"config_revision":7}`},
		{"pending", `{"storage_version":1,"schema_version":2,"config_revision":7,"routes":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			if err := os.WriteFile(path, []byte(`{"storage_version":1,"schema_version":2,"config_revision":7}`), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.routes != "" {
				if err := os.WriteFile(filepath.Join(dir, "routes.json"), []byte(tc.routes), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "pending" {
				if err := os.WriteFile(filepath.Join(dir, ".config.pending.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := PolicyForConfig(p.DefaultPolicy(), path); err == nil {
				t.Fatal("incomplete store silently reduced rollout coverage")
			}
		})
	}
}

func TestRolloutInstallPreservesServingVersionAndCredentials(t *testing.T) {
	m, system := rolloutManager(t)
	for path, body := range map[string]string{configPath: "data_dir: /var/lib/aiagent-gateway\nadmin_token: unchanged-secret\n", dataPath: `{"schema_version":2,"routes":[]}`, binaryPath: "old CLI"} {
		if err := atomicWrite(m.path(path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d := Deployment{InitialRelease: "/opt/aiagent-gateway/releases/old/gateway", Policy: p.DefaultPolicy()}
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
	report := rollout.Report{ID: "old-rollout", Phase: "COMPLETED", Stable: p.Target{Release: "old"}}
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
			report = rollout.Report{ID: "new-rollout", Phase: "COMPLETED", Stable: p.Target{Artifact: q.Artifact, Release: filepath.Base(filepath.Dir(q.Artifact))}}
		}
		json.NewEncoder(w).Encode(report)
	})}
	go srv.Serve(l)
	defer srv.Close()
	if err = m.installRollout(context.Background()); err != nil {
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
