package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/service"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func positiveEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Error("invalid positive integer", "variable", key)
		os.Exit(1)
	}
	return n
}
func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "install", "uninstall", "start", "stop", "restart", "status":
			if len(os.Args) > 2 {
				if len(os.Args) == 3 && (os.Args[2] == "--help" || os.Args[2] == "-h") {
					fmt.Printf("Usage: sudo %s %s\n", os.Args[0], os.Args[1])
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
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [install|uninstall|start|stop|restart|status]\nWithout a command, run the gateway in the foreground.\n", os.Args[0])
		flag.PrintDefaults()
	}

	initOnly := flag.Bool("init-only", false, "initialize or validate the configuration file, then exit")
	checkReady := flag.Bool("check-ready", false, "wait for the local HTTP readiness endpoint, then exit")
	flag.Parse()
	if flag.NArg() != 0 {
		slog.Error("unknown command", "command", flag.Arg(0))
		return 2
	}
	if *checkReady {
		if err := waitReady(env("GATEWAY_LISTEN", ":8080"), 15*time.Second); err != nil {
			slog.Error("readiness check failed", "error", err)
			return 1
		}
		return 0
	}
	var callers []gateway.Caller
	if err := json.Unmarshal([]byte(os.Getenv("GATEWAY_CALLERS")), &callers); err != nil {
		slog.Error("GATEWAY_CALLERS must be a JSON array")
		return 1
	}
	g, err := gateway.Open(gateway.Options{DataDir: env("GATEWAY_DATA_DIR", "./data"), AdminToken: os.Getenv("GATEWAY_ADMIN_TOKEN"), Callers: callers, Initialize: *initOnly || os.Getenv("GATEWAY_INIT") == "true", PublicURL: os.Getenv("GATEWAY_PUBLIC_URL"), MaxInflight: positiveEnv("GATEWAY_MAX_INFLIGHT", 4096), MaxPerCaller: positiveEnv("GATEWAY_MAX_PER_CALLER", 1024), MaxPerEndpoint: positiveEnv("GATEWAY_MAX_PER_ENDPOINT", 256), MaxSessions: positiveEnv("GATEWAY_MAX_SESSIONS", 100000), MaxTasks: positiveEnv("GATEWAY_MAX_TASKS", 100000), MaxResources: positiveEnv("GATEWAY_MAX_RESOURCES", 200000), MaxResourceAliases: positiveEnv("GATEWAY_MAX_RESOURCE_ALIASES", 200000), MaxDataIdempotency: positiveEnv("GATEWAY_MAX_DATA_IDEMPOTENCY", 100000)})
	if err != nil {
		slog.Error("startup failed", "error", err)
		return 1
	}
	defer g.Close()
	if *initOnly {
		slog.Info("configuration initialized or validated")
		return 0
	}
	data := &http.Server{Addr: env("GATEWAY_LISTEN", ":8080"), Handler: g.DataHandler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	admin := &http.Server{Addr: env("GATEWAY_ADMIN_LISTEN", "127.0.0.1:9090"), Handler: g.AdminHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10}
	errs := make(chan error, 2)
	go func() { errs <- data.ListenAndServe() }()
	go func() { errs <- admin.ListenAndServe() }()
	slog.Info("gateway started", "data", data.Addr, "admin", admin.Addr)
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
	_ = admin.Shutdown(shutdown)
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
