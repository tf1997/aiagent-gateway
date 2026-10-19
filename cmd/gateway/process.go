package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/process"
	p "aiagent-gateway/internal/releaseprotocol"
)

// process is the complete runtime development entry. It is deliberately not
// selected by install until live state handoff is implemented and validated.
func runProcess(args []string) int {
	fs := flag.NewFlagSet("process", flag.ContinueOnError)
	config := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
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
		slog.Error("process config failed", "error", err)
		return 1
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
	target := p.Target{Artifact: path, Release: identity.Release, Instance: identity.Instance, Incarnation: identity.Incarnation,
		URL: "http://gateway", Socket: filepath.Join(filepath.Dir(cfg.StateSocket), "p", identity.Incarnation, "business.sock")}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err = process.ServeGateway(ctx, cfg, target, identity); err != nil {
		slog.Error("complete process failed", "error", err)
		return 1
	}
	return 0
}
