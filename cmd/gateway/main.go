package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/service"
)

func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "launch":
			return runLaunch(os.Args[2:])
		case "process":
			return runProcess(os.Args[2:])
		case "rollout":
			if len(os.Args) < 3 || len(os.Args) > 4 || (len(os.Args) == 4 && os.Args[3] != "--json") {
				fmt.Fprintln(os.Stderr, "Usage: aiagent-gateway rollout status|resume|rollback [--json]")
				return 2
			}
			if err := service.RolloutCommand(os.Args[2], os.Stdout); err != nil {
				slog.Error("rollout command failed", "error", err)
				return 1
			}
			return 0
		}
	}
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "install", "uninstall", "start", "stop", "restart", "status", "cleanup", "cleanup-startup":
			if len(os.Args) > 2 {
				if len(os.Args) == 3 && (os.Args[2] == "--help" || os.Args[2] == "-h") {
					if os.Args[1] == "install" {
						fmt.Printf("Usage: sudo %s install\n", os.Args[0])
					} else {
						fmt.Printf("Usage: sudo %s %s\n", os.Args[0], os.Args[1])
					}
					return 0
				}
				slog.Error("service command takes no arguments")
				return 2
			}
			if err := service.Manage(os.Args[1], os.Stdout); err != nil {
				slog.Error("service command failed", "error", err)
				return 1
			}
			return 0
		}
	}
	return runForeground(os.Args[1:])
}

func runForeground(args []string) int {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	configPath := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
	initOnly := fs.Bool("init-only", false, "initialize or validate the route store, then exit")
	checkReady := fs.Bool("check-ready", false, "wait for the local HTTP readiness endpoint, then exit")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		slog.Error("unexpected command arguments")
		return 2
	}
	cfg, err := appconfig.Load(*configPath)
	if err != nil {
		slog.Error("configuration failed", "error", err)
		return 1
	}
	if *checkReady {
		if err := waitReady(cfg.ListenAddress(), 15*time.Second); err != nil {
			slog.Error("readiness check failed", "error", err)
			return 1
		}
		return 0
	}
	g, err := gateway.Open(cfg.Options())
	if err != nil {
		slog.Error("startup failed", "error", err)
		return 1
	}
	defer g.Close()
	if *initOnly {
		slog.Info("configuration initialized or validated")
		return 0
	}
	data := &http.Server{Addr: cfg.ListenAddress(), Handler: gateway.PublicHandler(g.DataHandler(), g.AdminHandler()), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	errs := make(chan error, 1)
	go func() { errs <- data.ListenAndServe() }()
	slog.Info("gateway started", "http", data.Addr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	failed := false
	select {
	case <-ctx.Done():
	case err := <-errs:
		failed = true
		slog.Error("listener stopped", "error", err)
	}
	g.BeginShutdown()
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if data.Shutdown(shutdown) != nil {
		_ = data.Close()
	}
	slog.Info("gateway stopped")
	if failed {
		return 1
	}
	return 0
}

func readinessURL(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/readyz"}).String(), nil
}
func waitReady(address string, timeout time.Duration) error {
	target, err := readinessURL(address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return err
		}
		res, err := client.Do(req)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gateway did not become ready: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
