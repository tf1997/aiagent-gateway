package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"aiagent-gateway/internal/state"
)

func completedInstallServer(t *testing.T, m *manager, artifact string) {
	t.Helper()
	serveInstallSocket(t, m.path(ControllerSocket), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := adminToken(m.path(configPath))
		if err != nil || r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("install authentication", err)
		}
		_ = json.NewEncoder(w).Encode(rollout.Report{ID: "initial", Phase: "COMPLETED",
			Stable: p.Target{Artifact: artifact, Release: filepath.Base(filepath.Dir(artifact))}})
	}))
}

func TestFreshInstallUsesHostService(t *testing.T) {
	m, system := rolloutManager(t)
	system.active = false
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	completedInstallServer(t, m, artifact)
	if err := m.installRollout(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	d, err := LoadDeployment(m.path(DeploymentPath))
	if err != nil || d.InitialRelease != artifact {
		t.Fatalf("manifest: %+v %v", d, err)
	}
	unit := string(read(t, m.path(unitPath)))
	for _, expected := range []string{"Type=simple", "User=aiagent-gateway", " host --config=", "KillMode=mixed", "LoadCredential=deployment.json:", "ExecStartPost="} {
		if !strings.Contains(unit, expected) {
			t.Fatal("missing host setting", expected)
		}
	}
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(read(t, m.path(DeploymentPath)), &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 3 || manifest["version"] != nil {
		t.Fatal("unexpected deployment fields", manifest)
	}
	for _, key := range []string{"infrastructure", "initial_release", "policy"} {
		if manifest[key] == nil {
			t.Fatal("missing deployment setting", key)
		}
	}
	if _, err := os.Stat(m.path(dataPath)); err != nil {
		t.Fatal("init unit was not invoked", err)
	}
	if err := os.Remove(m.path(unitPath)); err != nil {
		t.Fatal(err)
	}
	if err := m.restoreRolloutUnits(d); err != nil {
		t.Fatal(err)
	}
	if string(read(t, m.path(unitPath))) != unit {
		t.Fatal("reinstall changed topology")
	}
	if err := m.uninstallRollout(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dataPath, configPath, DeploymentPath, artifact} {
		if _, err := os.Stat(m.path(path)); err != nil {
			t.Fatal("uninstall removed recovery data", path, err)
		}
	}
}

func seedHostUpdate(t *testing.T, m *manager, phase string) map[string][]byte {
	t.Helper()
	d := Deployment{Infrastructure: "/opt/aiagent-gateway/infrastructure/old/gateway",
		InitialRelease: "/opt/aiagent-gateway/releases/old/gateway", Policy: p.DefaultPolicy()}
	raw, _ := json.Marshal(d)
	files := map[string][]byte{
		DeploymentPath: raw, configPath: []byte("admin_token: unchanged-secret\ndata_dir: /var/lib/aiagent-gateway\n"),
		dataPath: []byte(`{"storage_version":1,"schema_version":2,"config_revision":7}`),
		filepath.Join(filepath.Dir(dataPath), "routes.json"):          []byte(`{"routes":[{"id":"preserved"}]}`),
		filepath.Join(filepath.Dir(dataPath), "route-logs.json"):      []byte(`{"idempotency":{"receipt":"preserved"}}`),
		filepath.Join(filepath.Dir(dataPath), ".config.pending.json"): []byte(`{"transaction":"preserved"}`),
		filepath.Join(filepath.Dir(dataPath), "shared/state.log"):     []byte("durable business state"),
		filepath.Join(filepath.Dir(dataPath), "entry/state.log"):      []byte("previous traffic"),
		unitPath: []byte("[Service]\nExecStart=/opt/aiagent-gateway/infrastructure/old/gateway host\n"), binaryPath: []byte("old CLI"),
		unitPath + ".d/override.conf": []byte("[Service]\nLimitNOFILE=32768\n"),
	}
	for path, body := range files {
		if err := atomicWrite(m.path(path), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	controlDir := filepath.Join(filepath.Dir(dataPath), "release")
	j, err := state.Open(m.path(controlDir))
	if err != nil {
		t.Fatal(err)
	}
	report, _ := json.Marshal(rollout.Report{ID: "previous", Phase: phase})
	if err := j.Replace("controller/", map[string]json.RawMessage{"controller/report": report}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	return files
}

func TestHostUpdatePreservesBusinessState(t *testing.T) {
	m, system := rolloutManager(t)
	system.active = false
	files := seedHostUpdate(t, m, "COMPLETED")
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	completedInstallServer(t, m, artifact)
	if err := m.installRollout(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	d, err := LoadDeployment(m.path(DeploymentPath))
	if err != nil || d.InitialRelease != artifact {
		t.Fatalf("host update manifest: %+v %v", d, err)
	}
	for path, original := range files {
		if path == configPath || (strings.HasPrefix(path, filepath.Dir(dataPath)+"/") && !strings.Contains(path, "/entry/")) {
			if !bytes.Equal(original, read(t, m.path(path))) {
				t.Fatal("host update changed business data", path)
			}
		}
	}
	backups, err := filepath.Glob(m.path("/var/lib/aiagent-gateway-backups/*"))
	if err != nil || len(backups) != 1 {
		t.Fatal("missing maintenance backup", backups, err)
	}
	for path, original := range files {
		if !bytes.Equal(original, read(t, filepath.Join(backups[0], strings.TrimPrefix(path, "/")))) {
			t.Fatal("incomplete backup", path)
		}
	}
	for _, path := range []string{filepath.Join(filepath.Dir(dataPath), "entry"), filepath.Join(filepath.Dir(dataPath), "release")} {
		if _, err := os.Stat(m.path(path)); !os.IsNotExist(err) {
			t.Fatal("stale process state or override retained", path, err)
		}
	}
	if !bytes.Equal(files[unitPath+".d/override.conf"], read(t, m.path(unitPath+".d/override.conf"))) {
		t.Fatal("host update changed custom unit overrides")
	}
}

func TestHostUpdateRejectsUnsafeState(t *testing.T) {
	for _, mode := range []string{"active", "pending", "missing-config", "systemctl-error", "deactivating"} {
		t.Run(mode, func(t *testing.T) {
			m, system := rolloutManager(t)
			system.active = mode == "active"
			phase := "COMPLETED"
			if mode == "pending" {
				phase = "VERIFYING"
			}
			files := seedHostUpdate(t, m, phase)
			if mode == "missing-config" {
				if err := os.Remove(m.path(dataPath)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "systemctl-error" || mode == "deactivating" {
				m.command = func(context.Context, string, ...string) ([]byte, error) {
					if mode == "systemctl-error" {
						return nil, fmt.Errorf("bus unavailable")
					}
					return []byte("deactivating\n"), nil
				}
			}
			if err := m.installRollout(context.Background(), true); err == nil {
				t.Fatal("accepted unsafe host update")
			}
			for _, path := range []string{DeploymentPath, configPath, unitPath, binaryPath} {
				if !bytes.Equal(files[path], read(t, m.path(path))) {
					t.Fatal("rejected host update changed installation", path)
				}
			}
		})
	}
}
