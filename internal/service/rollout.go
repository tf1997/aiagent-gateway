package service

import (
	"aiagent-gateway/internal/appconfig"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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

type Deployment struct {
	InitialRelease string   `json:"initial_release"`
	Policy         p.Policy `json:"policy"`
}

func LoadDeployment(path string) (Deployment, error) {
	var d Deployment
	b, e := os.ReadFile(path)
	if e == nil {
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		e = decoder.Decode(&d)
		if e == nil && decoder.Decode(new(any)) != io.EOF {
			e = fmt.Errorf("invalid deployment manifest")
		}
	}
	if e == nil && d.InitialRelease == "" {
		e = fmt.Errorf("invalid deployment manifest")
	}
	if e == nil {
		d.Policy.Handoff = d.Policy.Handoff.Effective()
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
	if c.DataDir != filepath.Dir(dataPath) || c.StateSocket != StateSocket || c.EntrySocket != EntrySocket || c.ControlSocket != ControllerSocket || c.Deployment != DeploymentPath {
		return fmt.Errorf("systemd installation requires the default absolute data, socket and deployment paths; see docs/CONFIGURATION.md")
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
func (m *manager) installRollout(ctx context.Context) error {
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
		// Repeated installs inspect the live report and reuse the current release
		// or task instead of restarting a serving gateway.
		var current rollout.Report
		if err = client.Call(ctx, "GET", "/control/v1/rollout", nil, &current); err != nil {
			return err
		}
		switch current.Phase {
		case "RECOVERY_REQUIRED":
			return fmt.Errorf("rollout %s requires recovery: %s; use rollout status and resume or rollback", current.ID, current.Reason)
		case "COMPLETED", "ROLLED_BACK", "FAILED":
			if sameRelease(current.Stable, artifact) {
				if err := m.verifyInstalledRelease(ctx, token, current.Stable); err != nil {
					return fmt.Errorf("release is already installed but not ready: %w; inspect rollout status", err)
				}
				fmt.Fprintf(m.output, "release already active: %s; installation unchanged\n", current.Stable.Release)
				return nil
			}
		case "":
			return fmt.Errorf("rollout controller is initializing; retry install after startup")
		default:
			if current.Artifact != artifact && !(current.Phase == "RECOVERING" && sameRelease(current.Stable, artifact)) {
				return fmt.Errorf("rollout %s is %s for another release; wait for completion or use rollout status", current.ID, current.Phase)
			}
			fmt.Fprintf(m.output, "release %s already has rollout %s (%s); waiting for that rollout\n", filepath.Base(filepath.Dir(artifact)), current.ID, current.Phase)
			return waitRolloutFor(context.Background(), client, m.output, current.ID, artifact)
		}

		if d.Policy.AutoProfiles {
			d.Policy, err = PolicyForConfig(d.Policy, m.path(dataPath))
			if err != nil {
				return err
			}
		}
		var started rollout.Report
		if err = client.Call(ctx, "POST", "/control/v1/rollout", struct {
			Artifact string
			Policy   p.Policy
		}{artifact, d.Policy}, &started); err != nil {
			return err
		}
		return waitRolloutFor(context.Background(), client, m.output, started.ID, artifact)
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, path := range []string{binaryPath, unitPath} {
		if found, err := exists(m.path(path)); err != nil {
			return err
		} else if found {
			return fmt.Errorf("deployment manifest missing; restore it before installing")
		}
	}
	configExists, err := exists(m.path(configPath))
	if err != nil {
		return err
	}
	dataExists, err := exists(m.path(dataPath))
	if err != nil {
		return err
	}
	if configExists {
		if err := validateSystemConfig(m.path(configPath)); err != nil {
			return err
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
	binary, err := os.ReadFile(m.executable)
	if err != nil {
		return err
	}
	if err = atomicWrite(m.path(binaryPath), binary, 0755); err != nil {
		return err
	}
	if err = m.writeProcessUnits(artifact); err != nil {
		return err
	}
	d := Deployment{InitialRelease: artifact, Policy: p.DefaultPolicy()}
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
	return waitRolloutFor(context.Background(), client, m.output, "", artifact)
}
func waitRollout(ctx context.Context, c *rollout.Client, out io.Writer) error {
	return waitRolloutFor(ctx, c, out, "", "")
}

// Pin the requested rollout and artifact: completion of another operation must
// not be reported as successful installation of this binary.
func waitRolloutFor(ctx context.Context, c *rollout.Client, out io.Writer, id, artifact string) error {
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
			if id != "" && r.ID != id {
				return fmt.Errorf("rollout changed from %s to %s; inspect rollout status", id, r.ID)
			}
			if r.Phase != last {
				fmt.Fprintf(out, "%s %s\n", r.ID, r.Phase)
				last = r.Phase
			}
			switch r.Phase {
			case "COMPLETED":
				if artifact != "" && !sameRelease(r.Stable, artifact) {
					return fmt.Errorf("rollout completed with a different active release: %s", r.Stable.Release)
				}
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

func sameRelease(target p.Target, artifact string) bool {
	return target.Artifact == artifact && target.Release == filepath.Base(filepath.Dir(artifact))
}

func sameWorker(id p.Identity, target p.Target) bool {
	return id.Ready && target.Incarnation != "" && id.Incarnation == target.Incarnation && id.Instance == target.Instance && id.Release == target.Release && id.Protocol == p.Version && id.StateProtocol == 1
}

func (m *manager) verifyInstalledRelease(ctx context.Context, token string, target p.Target) error {
	client := rollout.NewClient(m.path(ControllerSocket), token)
	defer client.HTTP.CloseIdleConnections()
	var id p.Identity
	err := client.Call(ctx, "GET", "/control/v1/identity", nil, &id)
	if err != nil {
		return err
	}
	if !sameWorker(id, target) {
		return fmt.Errorf("stable gateway identity or readiness mismatch")
	}
	traffic, err := client.Traffic(ctx)
	if err != nil {
		return err
	}
	if traffic.Stable != target || traffic.Candidate != nil || traffic.Percent != 0 || traffic.Frozen || traffic.Rollout != "" {
		return fmt.Errorf("entry is not serving the recorded stable gateway")
	}
	return nil
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
		var r json.RawMessage
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

func (m *manager) restoreRolloutUnits(d Deployment) error {
	changed := false
	names := []string{"aiagent-gateway.service", "aiagent-gateway-init.service"}
	for _, name := range names {
		path := m.path("/etc/systemd/system/" + name)
		if _, err := os.Stat(path); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		raw, err := units.ReadFile("units/" + name)
		if err != nil {
			return err
		}
		raw = []byte(strings.ReplaceAll(string(raw), "@BINARY@", d.InitialRelease))
		if err = atomicWrite(path, raw, 0644); err != nil {
			return err
		}
		changed = true
	}
	if _, err := os.Stat(m.path(binaryPath)); os.IsNotExist(err) {
		raw, err := os.ReadFile(m.path(d.InitialRelease))
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
	for _, name := range []string{"aiagent-gateway.service", "aiagent-gateway-init.service"} {
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
				var id p.Identity
				e := c.Call(ctx, "GET", "/control/v1/identity", nil, &id)
				if e == nil && sameWorker(id, report.Stable) {
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

func PolicyForConfig(policy p.Policy, path string) (p.Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return policy, err
	}
	type storeVersion struct {
		Storage  int    `json:"storage_version"`
		Schema   int    `json:"schema_version"`
		Revision uint64 `json:"config_revision"`
	}
	var version storeVersion
	if err = json.Unmarshal(raw, &version); err != nil {
		return policy, err
	}
	if version.Storage != 0 {
		if version.Storage != 1 || version.Schema != 2 || version.Revision == 0 {
			return policy, fmt.Errorf("unsupported configuration storage version")
		}
		raw, err = os.ReadFile(filepath.Join(filepath.Dir(path), "routes.json"))
		if err != nil {
			return policy, err
		}
	}
	var config struct {
		storeVersion
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
	if version.Storage != 0 && (config.storeVersion != version || config.Routes == nil) {
		return policy, fmt.Errorf("incomplete configuration storage; retry after the state service has recovered")
	}
	// The state service owns recovery. Never infer fewer required profiles from
	// a commit that has not finished publishing all of its files.
	if _, err = os.Lstat(filepath.Join(filepath.Dir(path), ".config.pending.json")); err == nil {
		return policy, fmt.Errorf("pending configuration requires state service recovery before upgrading")
	} else if !os.IsNotExist(err) {
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
