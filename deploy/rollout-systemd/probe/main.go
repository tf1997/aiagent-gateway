// The probe exercises P0 under real systemd. It deliberately serves identity
// responses, not gateway business traffic, and is never installed by install.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"aiagent-gateway/internal/handoff"
	"aiagent-gateway/internal/process"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/service"
)

var version = "development"

type settings struct {
	dir, releases, initial, unit, incarnation, config string
	parentFD                                          int
}

type message struct {
	Action   string
	PID      int
	Identity handoff.Identity
	Snapshot handoff.Snapshot
	Handoff  string
}

type node struct {
	stopping   atomic.Bool
	credential bool
	mu         sync.Mutex
	cfg        settings
	identity   handoff.Identity
	owner      *handoff.Owner
	sockets    []*process.Socket
	servers    []*http.Server
	busy       bool
	done       chan error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("expected launch or serve")
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	var c settings
	fs.StringVar(&c.dir, "dir", "/var/lib/gateway-probe", "state directory")
	fs.StringVar(&c.releases, "releases", "/opt/gateway-probe/releases", "trusted artifacts")
	fs.StringVar(&c.config, "config", "/etc/gateway-probe.conf", "read-only credential")
	fs.StringVar(&c.initial, "initial", "", "initial digest")
	fs.StringVar(&c.unit, "unit", "gateway-probe.service", "systemd service")
	fs.StringVar(&c.incarnation, "incarnation", "", "candidate identity")
	fs.IntVar(&c.parentFD, "parent-fd", 0, "private parent socket")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	if mode == "launch" {
		d, err := handoff.ReadDecision(c.dir)
		selected := c.initial
		if err == nil {
			selected = d.ColdStart
		} else if !os.IsNotExist(err) && !errors.Is(err, handoff.ErrNoBaseline) {
			return err
		}
		artifact := filepath.Join(c.releases, selected, "gateway")
		if err = service.ValidateArtifact(c.releases, artifact, 0); err != nil {
			return err
		}
		return syscall.Exec(artifact, append([]string{artifact, "serve"}, arguments(c)...), os.Environ())
	}
	if mode != "serve" {
		return fmt.Errorf("unknown mode")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if c.incarnation == "" {
		c.incarnation = p.ID("process_")
	}
	n := &node{cfg: c, identity: handoff.Identity{Incarnation: c.incarnation, Release: hex.EncodeToString(sum[:])}, done: make(chan error, 1), busy: c.parentFD != 0}
	defer n.close()
	var config []byte
	if c.parentFD != 0 {
		f := os.NewFile(6, "configuration")
		config, err = io.ReadAll(f)
		f.Close()
	} else {
		config, err = os.ReadFile(c.config)
	}
	if err != nil || string(config) != "probe-credential\n" {
		return fmt.Errorf("credential was not inherited: %v", err)
	}
	n.credential = true
	if c.parentFD != 0 {
		if c.parentFD != 7 {
			return fmt.Errorf("unexpected parent FD")
		}
		for fd := 3; fd <= 5; fd++ {
			f := os.NewFile(uintptr(fd), "inherited-listener")
			s, e := process.Inherit(f)
			f.Close()
			if e != nil {
				return e
			}
			n.sockets = append(n.sockets, s)
		}
		f := os.NewFile(7, "handoff-channel")
		conn, e := net.FileConn(f)
		f.Close()
		if e != nil {
			return e
		}
		defer conn.Close()
		if err = n.candidate(conn); err != nil {
			return err
		}
		conn.Close()
	} else {
		n.owner, err = handoff.Acquire(c.dir, n.identity)
		if err != nil {
			return err
		}
		if n.owner.Decision().Phase == "" {
			err = n.owner.Bootstrap()
		} else {
			_, err = n.owner.Recover()
		}
		if err != nil {
			return err
		}
		if err = os.MkdirAll("/run/gateway-probe", 0700); err != nil {
			return err
		}
		// Cold start follows systemd cgroup cleanup. Never unlink during handoff.
		if err = os.Remove("/run/gateway-probe/control.sock"); err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, addr := range []struct{ network, address string }{{"tcp", "127.0.0.1:18080"}, {"tcp", "127.0.0.1:19090"}, {"unix", "/run/gateway-probe/control.sock"}} {
			s, e := process.Listen(addr.network, addr.address)
			if e != nil {
				return e
			}
			n.sockets = append(n.sockets, s)
		}
		if err = n.activate(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = (process.Notifier{Socket: os.Getenv("NOTIFY_SOCKET")}).Ready(ctx)
		cancel()
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	select {
	case err = <-n.done:
		return err
	case <-ctx.Done():
		n.stopping.Store(true)
		return n.drain()
	}
}

func arguments(c settings) []string {
	return []string{"--dir=" + c.dir, "--releases=" + c.releases, "--initial=" + c.initial, "--unit=" + c.unit, "--config=" + c.config}
}

func (n *node) activate() error {
	for _, s := range n.sockets {
		l, err := s.Activate()
		if err != nil {
			return err
		}
		h := &http.Server{Handler: http.HandlerFunc(n.handle), ReadHeaderTimeout: 5 * time.Second}
		n.servers = append(n.servers, h)
		go func() { _ = h.Serve(l) }()
	}
	return nil
}

func (n *node) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/upgrade":
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		n.mu.Lock()
		if n.busy {
			n.mu.Unlock()
			http.Error(w, "handoff active", 409)
			return
		}
		n.busy = true
		n.mu.Unlock()
		artifact := filepath.Join(n.cfg.releases, r.URL.Query().Get("release"), "gateway")
		if err := service.ValidateArtifact(n.cfg.releases, artifact, 0); err != nil {
			n.mu.Lock()
			n.busy = false
			n.mu.Unlock()
			http.Error(w, err.Error(), 400)
			return
		}
		w.WriteHeader(202)
		go func() { n.done <- n.upgrade(artifact, r.URL.Query().Get("failpoint")) }()
	case "/hold":
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", version)
		http.NewResponseController(w).Flush()
		select {
		case <-time.After(2 * time.Second):
			fmt.Fprint(w, "data: done\n\n")
		case <-r.Context().Done():
		}
	default:
		n.mu.Lock()
		busy := n.busy
		owner := n.owner
		n.mu.Unlock()
		var d handoff.Decision
		if owner != nil {
			d = owner.Decision()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"pid": os.Getpid(), "version": version, "identity": n.identity, "busy": busy, "credential": n.credential, "decision": d})
	}
}

func (n *node) drain() error {
	for _, s := range n.sockets {
		if err := s.Pause(); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, h := range n.servers {
		if err := h.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) close() {
	for _, h := range n.servers {
		h.Close()
	}
	for _, s := range n.sockets {
		s.Close()
	}
	if n.owner != nil {
		n.owner.Close()
	}
}

func (n *node) upgrade(artifact, failpoint string) error {
	var files []*os.File
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for _, s := range n.sockets {
		f, err := s.File()
		if err != nil {
			return err
		}
		files = append(files, f)
	}
	// Each launch receives its own read-only credential FD.
	config, err := os.Open(n.cfg.config)
	if err != nil {
		return err
	}
	files = append(files, config)
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	parent := os.NewFile(uintptr(fds[0]), "parent")
	child := os.NewFile(uintptr(fds[1]), "child")
	files = append(files, child)
	conn, err := net.FileConn(parent)
	parent.Close()
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(25 * time.Second))
	incarnation := p.ID("process_")
	args := append([]string{"serve"}, arguments(n.cfg)...)
	args = append(args, "--incarnation="+incarnation, "--parent-fd=7")
	cmd := exec.Command(artifact, args...)
	cmd.ExtraFiles = files
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err = cmd.Start(); err != nil {
		return err
	}
	for _, f := range files {
		f.Close()
	}
	files = nil
	go cmd.Wait()
	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)
	var hello message
	if err = dec.Decode(&hello); err != nil {
		return err
	}
	target := handoff.Identity{Incarnation: incarnation, Release: filepath.Base(filepath.Dir(artifact))}
	if hello.Action != "ready" || hello.PID != cmd.Process.Pid || hello.Identity != target {
		return fmt.Errorf("candidate identity mismatch")
	}
	id := p.ID("handoff_")
	if err = n.owner.Prepare(id, p.ID("rollout_"), target); err != nil {
		return err
	}
	request := func(m message, want string) error {
		if e := enc.Encode(m); e != nil {
			return e
		}
		var reply message
		if e := dec.Decode(&reply); e != nil {
			return e
		}
		if reply.Action != want || reply.Identity != target {
			return fmt.Errorf("unexpected candidate response: %+v", reply)
		}
		return nil
	}
	if err = request(message{Action: "activate"}, "active"); err != nil {
		return err
	}
	if err = n.drain(); err != nil {
		return err
	}
	if n.stopping.Load() {
		return context.Canceled
	}
	// P0 tests the ownership envelope with a fixed payload. Business state
	// snapshotting and state-call barriers are delivered separately in P2.
	sum := sha256.Sum256([]byte("probe-state"))
	snapshot := handoff.Snapshot{Digest: hex.EncodeToString(sum[:]), Revision: 1, Bytes: 11}
	if err = n.owner.Seal(id, snapshot); err != nil {
		return err
	}
	if err = n.owner.Close(); err != nil {
		return err
	}
	if err = request(message{Action: "commit", Handoff: id, Snapshot: snapshot}, "committed"); err != nil {
		return err
	}
	if failpoint == "after-commit" {
		return fmt.Errorf("injected crash after COMMITTED before MainPID")
	}
	if err = (process.Notifier{Socket: os.Getenv("NOTIFY_SOCKET")}).MainPID(cmd.Process.Pid); err != nil {
		return err
	}
	if err = request(message{Action: "take-main", PID: os.Getpid(), Handoff: id}, "main"); err != nil {
		return err
	}
	return nil // normal exit; the child is the confirmed service MainPID
}

func (n *node) candidate(conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)
	reply := func(action string) error {
		return enc.Encode(message{Action: action, PID: os.Getpid(), Identity: n.identity})
	}
	if err := reply("ready"); err != nil {
		return err
	}
	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			return err
		} // pre-promotion parent EOF is fatal
		switch m.Action {
		case "activate":
			if err := n.activate(); err != nil {
				return err
			}
			if err := reply("active"); err != nil {
				return err
			}
		case "commit":
			o, err := handoff.Acquire(n.cfg.dir, n.identity)
			if err != nil {
				return err
			}
			n.mu.Lock()
			n.owner = o
			n.mu.Unlock()
			if err = o.Commit(m.Handoff, m.Snapshot); err != nil {
				return err
			}
			if err = reply("committed"); err != nil {
				return err
			}
		case "take-main":
			if n.owner == nil || n.owner.Decision().Phase != handoff.Committed {
				return fmt.Errorf("ownership not committed")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := process.WaitMainPID(ctx, n.cfg.unit, os.Getpid())
			if err == nil {
				err = (process.Notifier{Socket: os.Getenv("NOTIFY_SOCKET")}).Ready(ctx)
			}
			cancel()
			if err != nil {
				return err
			}
			if err = reply("main"); err != nil {
				return err
			}
			// The promoted process can outlive its parent. Completion still
			// requires observing the old process leave the service.
			go func() {
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if errors.Is(syscall.Kill(m.PID, 0), syscall.ESRCH) {
						if e := n.owner.Complete(m.Handoff); e != nil {
							n.done <- e
							return
						}
						n.mu.Lock()
						n.busy = false
						n.mu.Unlock()
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				n.done <- fmt.Errorf("old PID %s did not exit", strconv.Itoa(m.PID))
			}()
			return nil
		default:
			return fmt.Errorf("unexpected handoff command")
		}
	}
}
