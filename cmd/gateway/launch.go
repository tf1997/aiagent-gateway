package main

import (
	"flag"
	"log/slog"
	"os"
	"syscall"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/service"
)

// launch is an exec envelope, never a serving supervisor or state writer.
func runLaunch(args []string) int {
	fs := flag.NewFlagSet("launch", flag.ContinueOnError)
	config := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
	deployment := fs.String("deployment", service.DeploymentPath, "deployment manifest")
	releases := fs.String("releases-dir", service.ReleasesDir, "immutable release directory")
	unit := fs.String("unit", service.Name+".service", "systemd service unit; empty for local execution")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		return 2
	}
	cfg, err := appconfig.Load(*config)
	if err != nil {
		slog.Error("launch config failed", "error", err)
		return 1
	}
	d, err := service.LoadDeployment(*deployment)
	if err != nil {
		slog.Error("launch deployment failed", "error", err)
		return 1
	}
	uid := uint32(os.Geteuid())
	if *releases == service.ReleasesDir {
		uid = 0
	}
	artifact, err := service.SelectArtifact(cfg.DataDir, d.InitialRelease, *releases, uid)
	if err != nil {
		slog.Error("launch selection failed", "error", err)
		return 1
	}
	err = syscall.Exec(artifact, []string{artifact, "process", "--config", *config, "--deployment", *deployment, "--releases-dir", *releases, "--unit", *unit}, os.Environ())
	slog.Error("launch exec failed", "error", err)
	return 1
}
