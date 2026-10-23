package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
)

func serveInstallSocket(t *testing.T, path string, handler http.Handler) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
}

func TestInstallSameRelease(t *testing.T) {
	for _, mode := range []string{"active", "renamed_binary", "stopped", "rolled_back", "failed", "unhealthy", "wrong_identity", "wrong_incarnation", "wrong_protocol", "wrong_traffic", "recovering", "recovery_required", "in_progress", "other_in_progress", "stable_during_upgrade", "wait_replaced", "wait_wrong_release", "wait_failed", "wait_rolled_back"} {
		t.Run(mode, func(t *testing.T) {
			m, system := rolloutManager(t)
			artifact, err := m.stageRelease()
			if err != nil {
				t.Fatal(err)
			}
			release := filepath.Base(filepath.Dir(artifact))
			if mode == "renamed_binary" {
				renamed := filepath.Join(filepath.Dir(m.executable), "downloaded-again")
				if err := os.Rename(m.executable, renamed); err != nil {
					t.Fatal(err)
				}
				m.executable = renamed
			}
			d := Deployment{InitialRelease: "/opt/aiagent-gateway/releases/old/gateway", Policy: p.DefaultPolicy()}
			raw, _ := json.Marshal(d)
			for path, body := range map[string][]byte{
				configPath: []byte("data_dir: /var/lib/aiagent-gateway\nadmin_token: unchanged-secret\n"),
				dataPath:   []byte(`{"schema_version":2,"routes":[]}`), binaryPath: []byte("old CLI"), DeploymentPath: raw,
			} {
				if err := atomicWrite(m.path(path), body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.restoreRolloutUnits(d); err != nil {
				t.Fatal(err)
			}
			system.calls = nil
			if mode == "stopped" {
				system.active = false
			}
			identity := p.Identity{Protocol: 1, StateProtocol: 1, Ready: true, Release: release, Instance: "b", Incarnation: "boot-b"}
			if mode == "unhealthy" {
				identity.Ready = false
			}
			if mode == "wrong_identity" {
				identity.Release = "other"
			}
			if mode == "wrong_incarnation" {
				identity.Incarnation = "other-boot"
			}
			if mode == "wrong_protocol" {
				identity.StateProtocol = 2
			}
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer unchanged-secret" {
					t.Error("missing worker authentication")
				}
				json.NewEncoder(w).Encode(identity)
			}))
			defer worker.Close()
			stable := p.Target{Artifact: artifact, Release: release, Instance: "b", Incarnation: "boot-b", AdminURL: worker.URL, URL: "http://127.0.0.1:18081"}
			report := rollout.Report{ID: "original-rollout", Phase: "COMPLETED", Stable: stable, Artifact: artifact}
			switch mode {
			case "rolled_back":
				report.Phase, report.Artifact = "ROLLED_BACK", "failed-candidate"
			case "failed":
				report.Phase, report.Artifact = "FAILED", "failed-candidate"
			case "recovering":
				report.Phase, report.Artifact = "RECOVERING", "old-failed-candidate"
			case "recovery_required":
				report.Phase, report.Reason = "RECOVERY_REQUIRED", "candidate_drain_timeout"
			case "stable_during_upgrade":
				report.Phase, report.Artifact = "VERIFYING", "another-release"
			case "in_progress", "wait_replaced", "wait_wrong_release", "other_in_progress", "wait_failed", "wait_rolled_back":
				report.Phase = "VERIFYING"
				report.Stable = p.Target{Artifact: "old", Release: "old"}
				if mode == "other_in_progress" {
					report.Artifact = "other"
				}
			}
			traffic := p.Traffic{Revision: 5, Stable: stable}
			if mode == "wrong_traffic" {
				traffic.Stable.Release = "other"
			}
			serveInstallSocket(t, m.path(EntrySocket), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/control/v1/traffic" || r.Header.Get("Authorization") != "Bearer unchanged-secret" {
					t.Error("unexpected entry request", r.Method, r.URL.Path)
				}
				json.NewEncoder(w).Encode(traffic)
			}))
			var posted, reads atomic.Int64
			serveInstallSocket(t, m.path(ControllerSocket), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posted.Add(1)
					t.Error("duplicate install submitted a rollout")
					w.WriteHeader(409)
					return
				}
				if r.URL.Path == "/control/v1/identity" {
					json.NewEncoder(w).Encode(identity)
					return
				}
				if r.URL.Path == "/control/v1/traffic" {
					json.NewEncoder(w).Encode(traffic)
					return
				}
				if reads.Add(1) > 1 && (mode == "in_progress" || mode == "recovering" || strings.HasPrefix(mode, "wait_")) {
					report.Phase, report.Stable = "COMPLETED", stable
					if mode == "wait_replaced" {
						report.ID = "other-rollout"
					}
					if mode == "wait_wrong_release" {
						report.Stable.Release = "other"
					}
					if mode == "wait_failed" {
						report.Phase = "FAILED"
					}
					if mode == "wait_rolled_back" {
						report.Phase = "ROLLED_BACK"
					}
				}
				json.NewEncoder(w).Encode(report)
			}))
			before := map[string][]byte{}
			for _, path := range []string{configPath, dataPath, binaryPath, DeploymentPath, unitPath} {
				before[path] = read(t, m.path(path))
			}
			err = m.installRollout(context.Background())
			wantSuccess := mode == "active" || mode == "renamed_binary" || mode == "stopped" || mode == "rolled_back" || mode == "failed" || mode == "in_progress" || mode == "recovering"
			if (err == nil) != wantSuccess {
				t.Fatalf("success=%t, error=%v", wantSuccess, err)
			}
			if posted.Load() != 0 {
				t.Fatal("repeat created new rollout", posted.Load())
			}
			for path, original := range before {
				if !bytes.Equal(original, read(t, m.path(path))) {
					t.Fatal("reinstall changed existing file", path)
				}
			}
			calls := []string{"systemctl is-active --quiet " + Name + ".service"}
			if mode == "stopped" {
				calls = append(calls, "systemctl enable --now "+Name+".service")
			}
			if !reflect.DeepEqual(system.calls, calls) {
				t.Fatal("reinstall changed services", system.calls)
			}
			if wantSuccess && !strings.Contains(m.output.(*bytes.Buffer).String(), release) {
				t.Fatal("output did not identify release", m.output)
			}
		})
	}
}
