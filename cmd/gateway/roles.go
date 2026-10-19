package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/host"
	p "aiagent-gateway/internal/releaseprotocol"
)

func runRole(role string, args []string) int {
	if err := serveRole(role, args); err != nil {
		slog.Error("role failed", "role", role, "error", err)
		return 1
	}
	return 0
}

func serveRole(role string, args []string) error {
	if role != "host" && role != "worker" {
		return fmt.Errorf("unknown role %q", role)
	}
	fs := flag.NewFlagSet(role, flag.ContinueOnError)
	configPath := fs.String("config", appconfig.DefaultPath, "YAML configuration file")
	deploymentPath := fs.String("deployment", "", "host deployment manifest")
	slot := fs.String("slot", "", "worker slot")
	release := fs.String("release", "", "release digest")
	incarnation := fs.String("incarnation", "", "host-assigned worker identity")
	parentFD := fs.Int("parent-fd", -1, "host lifetime pipe")
	listen := fs.String("listen", "", "worker data listener")
	admin := fs.String("admin", "", "worker control listener")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	cfg, err := appconfig.Load(*configPath)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if role == "host" {
		if *deploymentPath == "" {
			*deploymentPath = cfg.Deployment
		}
		return host.Serve(ctx, cfg, *configPath, *deploymentPath)
	}
	if (*slot != "a" && *slot != "b") || *release == "" || *incarnation == "" || *parentFD != 3 || *listen == "" || *admin == "" {
		return fmt.Errorf("worker requires host-assigned slot, release, incarnation, listeners and parent pipe")
	}
	parent := os.NewFile(uintptr(*parentFD), "host-lifetime")
	defer parent.Close()
	go func() {
		var b [1]byte
		_, _ = parent.Read(b[:])
		// A dead host cannot proxy streams or serve state; never leave orphan workers.
		os.Exit(1)
	}()
	opts := cfg.Options()
	opts.StateSocket = cfg.StateSocket
	opts.WorkerID = *incarnation
	g, err := gateway.Open(opts)
	if err != nil {
		return err
	}
	defer g.Close()
	identity := p.Identity{PID: os.Getpid(), Protocol: p.Version, StateProtocol: 1, Release: *release, Instance: *slot, Incarnation: opts.WorkerID}
	type endpoint struct {
		server   *http.Server
		listener net.Listener
	}
	var servers []endpoint
	defer func() {
		for _, s := range servers {
			s.server.Close()
			s.listener.Close()
		}
	}()
	for _, ep := range []struct {
		address string
		handler http.Handler
	}{
		{*listen, g.WorkerHandler(identity)}, {*admin, g.WorkerControl(identity)},
	} {
		l, err := net.Listen("tcp", ep.address)
		if err != nil {
			return err
		}
		servers = append(servers, endpoint{&http.Server{Handler: ep.handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}, l})
	}
	errs := make(chan error, len(servers)+1)
	for _, s := range servers {
		go func(s endpoint) { errs <- s.server.Serve(s.listener) }(s)
	}
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if !g.WorkerIdentity(identity).Ready {
					errs <- fmt.Errorf("worker lost shared state generation")
					return
				}
			}
		}
	}()
	slog.Info("gateway worker started", "slot", *slot)
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	for _, s := range servers {
		if e := s.server.Shutdown(shutdown); e != nil {
			s.server.Close()
		}
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
