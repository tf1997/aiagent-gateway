package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"aiagent-gateway/internal/gateway"
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
func main() {
	var callers []gateway.Caller
	if err := json.Unmarshal([]byte(os.Getenv("GATEWAY_CALLERS")), &callers); err != nil {
		slog.Error("GATEWAY_CALLERS must be a JSON array")
		os.Exit(1)
	}
	g, err := gateway.Open(gateway.Options{DataDir: env("GATEWAY_DATA_DIR", "./data"), AdminToken: os.Getenv("GATEWAY_ADMIN_TOKEN"), Callers: callers, Initialize: os.Getenv("GATEWAY_INIT") == "true", PublicURL: os.Getenv("GATEWAY_PUBLIC_URL"), MaxInflight: positiveEnv("GATEWAY_MAX_INFLIGHT", 4096), MaxPerCaller: positiveEnv("GATEWAY_MAX_PER_CALLER", 1024), MaxPerEndpoint: positiveEnv("GATEWAY_MAX_PER_ENDPOINT", 256), MaxSessions: positiveEnv("GATEWAY_MAX_SESSIONS", 100000), MaxTasks: positiveEnv("GATEWAY_MAX_TASKS", 100000)})
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	defer g.Close()
	data := &http.Server{Addr: env("GATEWAY_LISTEN", ":8080"), Handler: g.DataHandler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	admin := &http.Server{Addr: env("GATEWAY_ADMIN_LISTEN", "127.0.0.1:9090"), Handler: g.AdminHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10}
	errs := make(chan error, 2)
	go func() { errs <- data.ListenAndServe() }()
	go func() { errs <- admin.ListenAndServe() }()
	slog.Info("gateway started", "data", data.Addr, "admin", admin.Addr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case err := <-errs:
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
}
