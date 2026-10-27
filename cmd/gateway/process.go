package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/handoff"
	"aiagent-gateway/internal/process"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/service"
)

// process runs the complete gateway. Installed services enter through launch
// so the durable handoff decision selects the executable before writers open.
func runProcess(args []string) int {
	fs := flag.NewFlagSet("process", flag.ContinueOnError)
	config := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
	candidateFD := fs.Int("candidate-fd", 0, "inherited complete-process handshake descriptor")
	deployment := fs.String("deployment", "", "complete gateway deployment manifest")
	releases := fs.String("releases-dir", service.ReleasesDir, "immutable release directory")
	unit := fs.String("unit", "", "systemd service unit")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 || (*candidateFD != 0 && *candidateFD != process.CandidateChannelFD) {
		return 2
	}
	path, err := os.Executable()
	if err != nil {
		slog.Error("process executable unavailable", "error", err)
		return 1
	}
	f, err := os.Open(path)
	if err != nil {
		slog.Error("process executable unavailable", "error", err)
		return 1
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	f.Close()
	if err != nil {
		slog.Error("process digest failed", "error", err)
		return 1
	}
	identity := p.Identity{PID: os.Getpid(), Release: hex.EncodeToString(h.Sum(nil)), Instance: "a", Incarnation: p.ID("process_")}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *candidateFD != 0 {
		if err = process.ServeCandidate(ctx, identity.Release); err != nil {
			slog.Error("candidate process failed", "error", err)
			return 1
		}
		return 0
	}
	cfg, err := appconfig.Load(*config)
	if err != nil {
		slog.Error("process config failed", "error", err)
		return 1
	}
	target := p.Target{Artifact: path, Release: identity.Release, Instance: identity.Instance, Incarnation: identity.Incarnation,
		URL: "http://gateway", Socket: filepath.Join(filepath.Dir(cfg.StateSocket), "p", identity.Incarnation, "business.sock")}
	if *deployment != "" {
		d, loadErr := service.LoadDeployment(*deployment)
		if loadErr != nil {
			slog.Error("deployment failed", "error", loadErr)
			return 1
		}
		uid := uint32(os.Geteuid())
		if *releases == service.ReleasesDir {
			uid = 0
		}
		if err = service.ValidateArtifact(*releases, path, uid); err != nil {
			slog.Error("untrusted process artifact", "error", err)
			return 1
		}
		err = process.ServeService(ctx, cfg, target, identity, process.ServiceOptions{ConfigPath: *config, ReleasesDir: *releases, Unit: *unit, OwnerUID: uid, Policy: d.Policy, Output: os.Stderr})
	} else {
		err = process.ServeGateway(ctx, cfg, target, identity)
	}
	if err != nil {
		if *deployment != "" && errors.Is(err, handoff.ErrReleaseChanged) {
			return runLaunch([]string{"--config", *config, "--deployment", *deployment, "--releases-dir", *releases, "--unit", *unit})
		}
		slog.Error("complete process failed", "error", err)
		return 1
	}
	return 0
}
