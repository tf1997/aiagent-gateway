package service

import (
	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const DeploymentPath = "/etc/aiagent-gateway/deployment.json"
const RuntimeDir = "/run/aiagent-gateway"
const StateSocket = RuntimeDir + "/state.sock"
const EntrySocket = RuntimeDir + "/entry.sock"
const ControllerSocket = "/run/aiagent-gateway-control/controller.sock"
const ReleasesDir = "/opt/aiagent-gateway/releases"
const RolloutData = "/var/lib/aiagent-gateway-rollout"

type Deployment struct {
	Version        int      `json:"version"`
	Infrastructure string   `json:"infrastructure"`
	InitialRelease string   `json:"initial_release"`
	Policy         p.Policy `json:"policy"`
}

func LoadDeployment(path string) (Deployment, error) {
	var d Deployment
	b, e := os.ReadFile(path)
	if e == nil {
		e = json.Unmarshal(b, &d)
	}
	if e == nil && ((d.Version != 1 && d.Version != 2) || d.Infrastructure == "" || d.InitialRelease == "") {
		e = fmt.Errorf("invalid deployment manifest")
	}
	if e == nil {
		e = d.Policy.Validate()
	}
	return d, e
}
func adminToken(path string) (string, error) {
	c, err := appconfig.Load(path)
	if err != nil {
		return "", err
	}
	return c.AdminToken, nil
}

// The packaged units and driver have fixed state paths. Reject incompatible
// overrides before stopping services instead of initializing a different store.
func validateSystemConfig(path string) error {
	c, err := appconfig.Load(path)
	if err != nil {
		return err
	}
	if c.DataDir != filepath.Dir(dataPath) || c.StateSocket != StateSocket || c.EntrySocket != EntrySocket || c.ControlSocket != ControllerSocket || c.Deployment != DeploymentPath || c.RolloutData != RolloutData {
		return fmt.Errorf("systemd installation requires the default absolute data, socket, deployment and rollout paths; see docs/CONFIGURATION.md")
	}
	return nil
}
func (m *manager) stageRelease() (string, error) {
	b, err := os.ReadFile(m.executable)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	id := hex.EncodeToString(sum[:])
	dest := m.path(filepath.Join(ReleasesDir, id, "gateway"))
	if old, e := os.ReadFile(dest); e == nil {
		if sha256.Sum256(old) != sum {
			return "", fmt.Errorf("release hash conflict")
		}
	} else {
		if err = atomicWrite(dest, b, 0755); err != nil {
			return "", err
		}
	}
	return filepath.Join(ReleasesDir, id, "gateway"), nil
}
func (m *manager) installRollout(ctx context.Context, migrate bool) error {
	if _, err := os.Stat(m.path("/etc/aiagent-gateway/gateway.env")); err == nil {
		if _, err := os.Stat(m.path(configPath)); err != nil {
			return fmt.Errorf("environment configuration is no longer supported; migrate gateway.env to gateway.yaml before installation (see docs/CONFIGURATION.md)")
		}
	}
	if raw, err := os.ReadFile(m.path("/etc/systemd/system/aiagent-gateway-state.service")); err == nil && strings.Contains(string(raw), "EnvironmentFile=") {
		return fmt.Errorf("environment-based infrastructure requires a maintenance migration to YAML; ordinary worker rollout cannot upgrade it (see docs/CONFIGURATION.md)")
	}
	artifact, err := m.stageRelease()
	if err != nil {
		return err
	}
	if _, err = os.Stat(m.path(DeploymentPath)); err == nil {
		if err := validateSystemConfig(m.path(configPath)); err != nil {
			return err
		}
		token, err := adminToken(m.path(configPath))
		if err != nil {
			return err
		}
		client := rollout.NewClient(m.path(ControllerSocket), token)
		defer client.HTTP.CloseIdleConnections()
		d, err := LoadDeployment(m.path(DeploymentPath))
		if err != nil {
			return err
		}
		if d.Version != 2 {
			return fmt.Errorf("deployment requires a maintenance migration to YAML-aware infrastructure; see docs/CONFIGURATION.md")
		}
		if _, err := os.Stat(m.path(dataPath)); err != nil {
			return fmt.Errorf("existing configuration missing; restore it before upgrading")
		}
		if err = m.restoreRolloutUnits(d); err != nil {
			return err
		}
		if _, active := m.command(ctx, "systemctl", "is-active", "--quiet", Name+".service"); active != nil {
			if err = m.run(ctx, "systemctl", "enable", "--now", Name+".service"); err != nil {
				return err
			}
			if err = waitStable(ctx, client, token); err != nil {
				return err
			}
		}

		if d.Policy.AutoProfiles {
			d.Policy, err = policyForConfig(d.Policy, m.path(dataPath))
			if err != nil {
				return err
			}
		}
		if err = client.Call(ctx, "POST", "/control/v1/rollout", struct {
			Artifact string
			Policy   p.Policy
		}{artifact, d.Policy}, nil); err != nil {
			return err
		}
		return waitRollout(context.Background(), client, m.output)
	} else if !os.IsNotExist(err) {
		return err
	}
	configExists, err := exists(m.path(configPath))
	if err != nil {
		return err
	}
	dataExists, err := exists(m.path(dataPath))
	if err != nil {
		return err
	}
	if configExists && dataExists && !migrate {
		return fmt.Errorf("legacy installation requires a maintenance migration: install --migrate; existing in-memory resources cannot be transferred")
	}
	if configExists {
		if err := validateSystemConfig(m.path(configPath)); err != nil {
			return err
		}
	}
	if !dataExists {
		for _, path := range []string{binaryPath, unitPath} {
			if found, err := exists(m.path(path)); err != nil {
				return err
			} else if found {
				return fmt.Errorf("existing configuration is missing; restore it before migration")
			}
		}
	}
	if !configExists && dataExists {
		return fmt.Errorf("existing credentials are missing; restore them before installation")
	}
	if err = m.account(ctx); err != nil {
		return err
	}
	if !configExists {
		raw, err := credentials()
		if err != nil {
			return err
		}
		if err = os.MkdirAll(m.path("/etc/aiagent-gateway"), 0700); err != nil {
			return err
		}
		if err = atomicWrite(m.path(configPath), raw, 0600); err != nil {
			return err
		}
	}
	if configExists && dataExists {
		if err = m.run(ctx, "systemctl", "stop", Name+".service"); err != nil {
			return err
		}
	}
	binary, err := os.ReadFile(m.executable)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(binary)
	infra := filepath.Join("/opt/aiagent-gateway/infrastructure", hex.EncodeToString(sum[:]), "gateway")
	if err = atomicWrite(m.path(infra), binary, 0755); err != nil {
		return err
	}
	if err = atomicWrite(m.path(binaryPath), binary, 0755); err != nil {
		return err
	}
	for _, name := range []string{"aiagent-gateway.service", "aiagent-gateway-init.service", "aiagent-gateway-entry.service", "aiagent-gateway-state.service", "aiagent-gateway-rollout.service"} {
		var raw []byte
		if name == "aiagent-gateway.service" {
			raw = []byte(masterUnit)
		} else if name == "aiagent-gateway-init.service" {
			raw, err = units.ReadFile("units/" + name)
		} else {
			raw, err = units.ReadFile("units/" + name)
		}
		if err != nil {
			return err
		}
		raw = []byte(strings.ReplaceAll(string(raw), "@BINARY@", infra))
		if err = atomicWrite(m.path("/etc/systemd/system/"+name), raw, 0644); err != nil {
			return err
		}
	}
	d := Deployment{Version: 2, Infrastructure: infra, InitialRelease: artifact, Policy: p.DefaultPolicy()}
	raw, _ := json.MarshalIndent(d, "", "  ")
	if err = atomicWrite(m.path(DeploymentPath), raw, 0600); err != nil {
		return err
	}
	if err = m.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if !dataExists {
		if err = m.run(ctx, "systemctl", "start", Name+"-init.service"); err != nil {
			return err
		}
	}
	if err = m.run(ctx, "systemctl", "enable", "--now", Name+".service"); err != nil {
		return err
	}
	token, err := adminToken(m.path(configPath))
	if err != nil {
		return err
	}
	client := rollout.NewClient(m.path(ControllerSocket), token)
	defer client.HTTP.CloseIdleConnections()
	return waitRollout(context.Background(), client, m.output)
}
func waitRollout(ctx context.Context, c *rollout.Client, out io.Writer) error {
	last := ""
	unavailable := time.Now().Add(time.Minute)
	for {
		var r rollout.Report
		err := c.Call(ctx, "GET", "/control/v1/rollout", nil, &r)
		if err != nil {
			if time.Now().After(unavailable) {
				return fmt.Errorf("cannot observe background rollout; use rollout status: %w", err)
			}
		} else {
			unavailable = time.Now().Add(time.Minute)
			if r.Phase != last {
				fmt.Fprintf(out, "%s %s\n", r.ID, r.Phase)
				last = r.Phase
			}
			switch r.Phase {
			case "COMPLETED":
				fmt.Fprintf(out, "active release: %s\n", r.Stable.Release)
				return nil
			case "ROLLED_BACK", "FAILED", "RECOVERY_REQUIRED":
				return fmt.Errorf("%s: %s (active release %s)", r.Phase, r.Reason, r.Stable.Release)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

const masterUnit = `[Unit]
Description=AI Agent Gateway services
Wants=network-online.target aiagent-gateway-state.service aiagent-gateway-entry.service aiagent-gateway-rollout.service
After=network-online.target
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/bin/true
[Install]
WantedBy=multi-user.target
`

// SystemdDriver manages immutable release paths; it never invokes a shell.
type SystemdDriver struct {
	Token   string
	command runner
}

func NewSystemdDriver(token string) *SystemdDriver {
	return &SystemdDriver{Token: token, command: command}
}
func (d *SystemdDriver) unit(slot string) string {
	return "aiagent-gateway-worker-" + slot + ".service"
}
func (d *SystemdDriver) Start(ctx context.Context, artifact, slot string) (p.Target, error) {
	if slot != "a" && slot != "b" {
		return p.Target{}, fmt.Errorf("invalid slot")
	}
	clean := filepath.Clean(artifact)
	rel, err := filepath.Rel(ReleasesDir, clean)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.Base(clean) != "gateway" || filepath.Dir(filepath.Dir(clean)) != ReleasesDir {
		return p.Target{}, fmt.Errorf("artifact outside release directory")
	}
	b, err := os.ReadFile(clean)
	if err != nil {
		return p.Target{}, err
	}
	hash := sha256.Sum256(b)
	release := hex.EncodeToString(hash[:])
	if filepath.Base(filepath.Dir(clean)) != release {
		return p.Target{}, fmt.Errorf("artifact digest mismatch")
	}
	port, admin := 18080, 19090
	if slot == "b" {
		port++
		admin++
	}
	target := p.Target{Artifact: clean, Release: release, Instance: slot, URL: fmt.Sprintf("http://127.0.0.1:%d", port), AdminURL: fmt.Sprintf("http://127.0.0.1:%d", admin)}
	if id, e := d.Identity(ctx, target); e == nil {
		if id.Release != release || id.Instance != slot {
			return target, fmt.Errorf("slot is occupied by another release")
		}
		target.Incarnation = id.Incarnation
		if id.Ready {
			return target, nil
		}
	}
	if _, e := d.command(ctx, "systemctl", "is-active", "--quiet", d.unit(slot)); e == nil {
		return target, fmt.Errorf("slot is active but not ready; refusing to replace it")
	}
	unit, err := units.ReadFile("units/aiagent-gateway-worker.service")
	if err != nil {
		return target, err
	}
	text := string(unit)
	for key, value := range map[string]string{"@BINARY@": clean, "@SLOT@": slot, "@RELEASE@": release, "@DATA_PORT@": fmt.Sprint(port), "@ADMIN_PORT@": fmt.Sprint(admin)} {
		text = strings.ReplaceAll(text, key, value)
	}
	if err = atomicWrite("/etc/systemd/system/"+d.unit(slot), []byte(text), 0644); err != nil {
		return target, err
	}
	if _, err = d.command(ctx, "systemctl", "daemon-reload"); err != nil {
		return target, err
	}
	if _, err = d.command(ctx, "systemctl", "start", d.unit(slot)); err != nil {
		return target, err
	}
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	for time.Now().Before(deadline) {
		id, e := d.Identity(ctx, target)
		if e == nil && id.Release == release && id.Instance == slot && id.Ready && id.Protocol == 1 && id.StateProtocol == 1 {
			target.Incarnation = id.Incarnation
			return target, nil
		}
		select {
		case <-ctx.Done():
			cleanup, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			_, _ = d.command(cleanup, "systemctl", "stop", d.unit(slot))
			cancel()
			return target, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	_, _ = d.command(ctx, "systemctl", "stop", d.unit(slot))
	return target, fmt.Errorf("candidate did not become ready")
}
func (d *SystemdDriver) Identity(ctx context.Context, t p.Target) (p.Identity, error) {
	var id p.Identity
	req, err := http.NewRequestWithContext(ctx, "GET", t.AdminURL+"/control/v1/identity", nil)
	if err != nil {
		return id, err
	}
	req.Header.Set("Authorization", "Bearer "+d.Token)
	client := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return id, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return id, fmt.Errorf("worker control HTTP %d", res.StatusCode)
	}
	err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&id)
	return id, err
}
func (d *SystemdDriver) Stop(ctx context.Context, t p.Target) error {
	if _, active := d.command(ctx, "systemctl", "is-active", "--quiet", d.unit(t.Instance)); active == nil {
		id, err := d.Identity(ctx, t)
		if err != nil {
			return fmt.Errorf("active worker control unavailable; cannot confirm drain")
		}
		if id.Inflight != 0 {
			return fmt.Errorf("worker still has active execution")
		}
		if id.Incarnation != t.Incarnation {
			return fmt.Errorf("worker incarnation changed")
		}
	}

	if _, err := d.command(ctx, "systemctl", "stop", d.unit(t.Instance)); err != nil {
		return err
	}
	_, err := gateway.SharedControl(StateSocket, d.Token, t.Incarnation, "retire", false)
	return err
}
func (d *SystemdDriver) Freeze(ctx context.Context, value bool) error {
	_, err := gateway.SharedControl(StateSocket, d.Token, "controller", "freeze", value)
	return err
}
func RolloutCommand(action string, out io.Writer) error {
	token, err := adminToken(configPath)
	if err != nil {
		return err
	}
	c := rollout.NewClient(ControllerSocket, token)
	defer c.HTTP.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if action == "status" {
		var r rollout.Report
		if err = c.Call(ctx, "GET", "/control/v1/rollout", nil, &r); err != nil {
			return err
		}
		e := json.NewEncoder(out)
		e.SetIndent("", "  ")
		return e.Encode(r)
	}
	if action != "resume" && action != "rollback" {
		return fmt.Errorf("unknown rollout action")
	}
	if err = c.Call(ctx, "POST", "/control/v1/rollout/"+action, nil, nil); err != nil {
		return err
	}
	return waitRollout(context.Background(), c, out)
}

func (d *SystemdDriver) Retire(ctx context.Context, t p.Target) error {
	_, err := gateway.SharedControl(StateSocket, d.Token, t.Incarnation, "retire", false)
	return err
}

func (m *manager) restoreRolloutUnits(d Deployment) error {
	changed := false
	for _, name := range []string{"aiagent-gateway.service", "aiagent-gateway-init.service", "aiagent-gateway-entry.service", "aiagent-gateway-state.service", "aiagent-gateway-rollout.service"} {
		path := m.path("/etc/systemd/system/" + name)
		if _, err := os.Stat(path); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		var raw []byte
		var err error
		if name == "aiagent-gateway.service" {
			raw = []byte(masterUnit)
		} else {
			raw, err = units.ReadFile("units/" + name)
			if err != nil {
				return err
			}
		}
		raw = []byte(strings.ReplaceAll(string(raw), "@BINARY@", d.Infrastructure))
		if err = atomicWrite(path, raw, 0644); err != nil {
			return err
		}
		changed = true
	}
	if _, err := os.Stat(m.path(binaryPath)); os.IsNotExist(err) {
		raw, err := os.ReadFile(m.path(d.Infrastructure))
		if err != nil {
			return err
		}
		if err = atomicWrite(m.path(binaryPath), raw, 0755); err != nil {
			return err
		}
	}
	if changed {
		return m.run(context.Background(), "systemctl", "daemon-reload")
	}
	return nil
}
func (m *manager) uninstallRollout(ctx context.Context) error {
	if err := m.run(ctx, "systemctl", "disable", "--now", Name+".service"); err != nil {
		return err
	}
	for _, name := range []string{"aiagent-gateway.service", "aiagent-gateway-init.service", "aiagent-gateway-entry.service", "aiagent-gateway-state.service", "aiagent-gateway-rollout.service", "aiagent-gateway-worker-a.service", "aiagent-gateway-worker-b.service"} {
		if err := os.Remove(m.path("/etc/systemd/system/" + name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.Remove(m.path(binaryPath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := m.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	fmt.Fprintln(m.output, "Services removed; credentials, shared state, release archives and deployment manifest retained for reinstall.")
	return nil
}
func waitStable(ctx context.Context, c *rollout.Client, token string) error {
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		var report rollout.Report
		if err := c.Call(ctx, "GET", "/control/v1/rollout", nil, &report); err == nil {
			if report.Phase == "RECOVERY_REQUIRED" {
				return fmt.Errorf("rollout recovery required: %s", report.Reason)
			}
			if report.Stable.Incarnation != "" {
				id, e := NewSystemdDriver(token).Identity(ctx, report.Stable)
				if e == nil && id.Ready && id.Incarnation == report.Stable.Incarnation {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("stable deployment did not become ready")
}

func policyForConfig(policy p.Policy, path string) (p.Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return policy, err
	}
	var config struct {
		Routes []struct {
			Enabled   *bool    `json:"enabled"`
			AgentType string   `json:"agent_type"`
			Expose    []string `json:"expose"`
			Protocol  string   `json:"protocol"`
			Card      struct {
				Capabilities struct {
					Streaming bool `json:"streaming"`
				} `json:"capabilities"`
			} `json:"agent_card"`
		} `json:"routes"`
	}
	if err = json.Unmarshal(raw, &config); err != nil {
		return policy, err
	}
	profiles := map[string]bool{}
	for _, r := range config.Routes {
		if r.Enabled != nil && !*r.Enabled {
			continue
		}
		entries := r.Expose
		if len(entries) == 0 && r.Protocol != "" {
			entries = []string{r.Protocol}
		}
		for _, e := range entries {
			if e == "proxy" {
				profiles["http"] = true
				if r.AgentType == "hermes" {
					profiles["sse"] = true
				}
			}
			if e == "a2a" {
				profiles["a2a"] = true
				if r.Card.Capabilities.Streaming {
					profiles["a2a_sse"] = true
				}
			}
		}
	}
	if len(profiles) > 0 {
		policy.Profiles = nil
		for _, key := range []string{"http", "sse", "a2a", "a2a_sse"} {
			if profiles[key] {
				policy.Profiles = append(policy.Profiles, key)
			}
		}
	}
	return policy, policy.Validate()
}
