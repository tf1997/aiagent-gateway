package service

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
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

func TestFreshInstallUsesCompleteProcessService(t *testing.T) {
	m, system := rolloutManager(t)
	system.active = false
	artifact, err := m.stageRelease()
	if err != nil {
		t.Fatal(err)
	}
	completedInstallServer(t, m, artifact)
	if err := m.installRollout(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, err := LoadDeployment(m.path(DeploymentPath))
	if err != nil || d.InitialRelease != artifact {
		t.Fatalf("manifest: %+v %v", d, err)
	}
	unit := string(read(t, m.path(unitPath)))
	for _, expected := range []string{"Type=notify", "NotifyAccess=main", "User=aiagent-gateway", " launch --config=", "KillMode=mixed", "LoadCredential=deployment.json:"} {
		if !strings.Contains(unit, expected) {
			t.Fatal("missing complete process setting", expected)
		}
	}
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(read(t, m.path(DeploymentPath)), &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 2 || manifest["version"] != nil {
		t.Fatal("unexpected deployment fields", manifest)
	}
	for _, key := range []string{"initial_release", "policy"} {
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
