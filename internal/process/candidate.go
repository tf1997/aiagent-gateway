package process

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
)

const maxCandidateSnapshot = 256 << 20

// Candidate holds only an executor and remote state access until COMMITTED.
// The executor is retained during adoption so existing streams keep leases.
type Candidate struct {
	mu             sync.Mutex
	cfg            appconfig.Config
	boot           candidateBoot
	identity       p.Identity
	client         *shared.Client
	business       *gateway.Gateway
	owner          *Gateway
	decision       handoff.Decision
	receiver       *shared.SnapshotReceiver
	verified       bool
	sockets        []*Socket
	public         []*http.Server
	preview, delta bool
	viewRaw        []byte
	viewDecision   handoff.Decision
}

func openCandidate(cfg appconfig.Config, boot candidateBoot, release string) (*Candidate, error) {
	t := boot.Target
	if boot.Policy.Validate() != nil || boot.Policy != boot.Policy.Effective() || boot.MaxSnapshotBytes > boot.Policy.SnapshotBytes || boot.MaxDeltaBytes > boot.Policy.DeltaBytes {
		return nil, fmt.Errorf("INVALID_CANDIDATE_POLICY")
	}
	if boot.Protocol != peerProtocol || boot.SnapshotFormat != gatewaySnapshotFormat || boot.DeltaFormat != shared.DeltaFormat || boot.MaxDeltaBytes <= 0 || boot.MaxDeltaBytes > maxCandidateSnapshot || boot.ParentPID != os.Getppid() || boot.ParentPID < 2 ||
		t.Release != release || t.Incarnation == "" || t.Incarnation == boot.Source.Incarnation || (t.Instance != "a" && t.Instance != "b") ||
		boot.Token == "" || boot.StateEpoch == "" || boot.MaxSnapshotBytes <= 0 || boot.MaxSnapshotBytes > maxCandidateSnapshot ||
		!filepath.IsAbs(boot.StateSocket) || !filepath.IsAbs(t.Socket) || filepath.Dir(t.Socket) != filepath.Dir(boot.StateSocket) || t.URL != "http://gateway" {
		return nil, fmt.Errorf("INVALID_CANDIDATE_BOOTSTRAP")
	}
	c := &Candidate{cfg: cfg, boot: boot, identity: p.Identity{PID: os.Getpid(), Release: release, Incarnation: t.Incarnation, Instance: t.Instance}}
	c.client = shared.NewClient(boot.StateSocket, boot.Token, t.Incarnation)
	var config shared.Response
	if err := c.client.Call("config", shared.Request{}, &config); err != nil {
		c.client.Close()
		return nil, err
	}
	if config.Epoch != boot.StateEpoch || config.Config == nil || config.Config.Revision != boot.ConfigRevision {
		c.client.Close()
		return nil, fmt.Errorf("CANDIDATE_STATE_IDENTITY_MISMATCH")
	}
	opts := cfg.Options()
	opts.StateClient, opts.WorkerID, opts.DeferBackground = c.client, t.Incarnation, true
	var err error
	c.business, err = gateway.Open(opts)
	if err != nil {
		c.client.Close()
		return nil, err
	}
	return c, nil
}

func (c *Candidate) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, server := range c.public {
		server.Close()
	}
	if c.owner != nil {
		return c.owner.Close()
	}
	return c.business.Close()
}

func (c *Candidate) privateHandler() http.Handler {
	worker := c.business.WorkerHandler(c.identity)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get(p.AuthHeader)), []byte(c.boot.Token)) != 1 ||
			r.Header.Get(p.WorkerHeader) != c.identity.Incarnation || r.Header.Get(p.RequestHeader) == "" {
			http.Error(w, "UNAUTHORIZED_EXECUTION_PEER", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/_process/recover" {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var m peerMessage
			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&m) != nil || decoder.Decode(new(any)) != io.EOF || (m.Action != "identity" && m.Action != "activate") {
				http.Error(w, "INVALID_RECOVERY_COMMAND", http.StatusBadRequest)
				return
			}
			reply, err := c.command(r.Context(), m)
			if err != nil {
				reply.Error = err.Error()
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		r.Header.Set(p.AuthHeader, c.cfg.AdminToken)
		worker.ServeHTTP(w, r)
	})
}

func (c *Candidate) command(ctx context.Context, m peerMessage) (peerMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply := peerMessage{Sequence: m.Sequence, Action: m.Action}
	switch m.Action {
	case "identity":
	case "activate":
		if err := c.activate(ctx); err != nil {
			return reply, err
		}
	case "snapshot":
		if c.owner != nil || m.Decision.Validate() != nil || (!m.Preview && m.Decision.Phase != handoff.Sealed) || (m.Preview && m.Decision.Phase != handoff.Prepared) ||
			m.Decision.Source != (handoff.Identity{Incarnation: c.boot.Source.Incarnation, Release: c.boot.Source.Release}) ||
			m.Decision.Target != (handoff.Identity{Incarnation: c.identity.Incarnation, Release: c.identity.Release}) ||
			m.Decision.Epoch != c.boot.StateEpoch || (!m.Delta && (m.Manifest.Digest != m.Decision.Snapshot.Digest || int64(m.Manifest.Bytes) != m.Decision.Snapshot.Bytes)) ||
			(m.Delta && (c.viewRaw == nil || !sameHandoff(c.viewDecision, m.Decision))) {
			return reply, handoff.ErrConflict
		}
		budget := c.boot.MaxSnapshotBytes
		if m.Delta {
			budget = c.boot.MaxDeltaBytes
		}
		receiver, err := shared.NewSnapshotReceiver(m.Manifest, budget)
		if err != nil {
			return reply, err
		}
		c.receiver, c.decision, c.verified = receiver, m.Decision, false
		c.preview, c.delta = m.Preview, m.Delta
	case "chunk":
		if c.receiver == nil || m.Chunk == nil || c.verified {
			return reply, fmt.Errorf("SNAPSHOT_NOT_STARTED")
		}
		if err := c.receiver.Add(*m.Chunk); err != nil {
			c.receiver = nil
			return reply, err
		}
	case "validate":
		if (c.verified || (c.preview && c.viewDecision == c.decision && c.receiver == nil)) && m.Decision == c.decision {
			break
		}
		if c.receiver == nil || m.Decision != c.decision {
			return reply, handoff.ErrConflict
		}
		raw, err := c.receiver.Finish()
		c.receiver = nil
		if err != nil {
			return reply, err
		}
		if c.delta {
			var version shared.ImageVersion
			raw, version, err = shared.ApplyDelta(ctx, c.viewRaw, imageVersion(c.viewDecision), raw, c.boot.MaxDeltaBytes, c.boot.MaxSnapshotBytes)
			if err != nil {
				return reply, err
			}
			if version != imageVersion(c.decision) {
				return reply, handoff.ErrConflict
			}
		}
		// A preview uses the same full invariant checks as a sealed image,
		// while retaining PREPARED and never setting commit authorization.
		validation := c.decision
		validation.Phase = handoff.Sealed
		if _, err = ValidateHandoff(raw, validation, c.boot.MaxSnapshotBytes, c.cfg.Options()); err != nil {
			return reply, err
		}
		if err = ctx.Err(); err != nil {
			return reply, err
		}
		c.viewRaw, c.viewDecision = raw, c.decision
		c.verified = !c.preview
	case "commit":
		if !c.verified || c.preview || m.Decision != c.decision {
			return reply, handoff.ErrConflict
		}
		if c.owner == nil {
			n, err := openHandoffGateway(ctx, c.cfg, c.boot.Target, c.identity, c.decision, c.boot.MaxSnapshotBytes, c.client, c.business, false, c.boot.Policy)
			if err != nil {
				return reply, err
			}
			c.owner = n
			c.viewRaw = nil
		}
	case "abort":
		if c.owner != nil || m.Decision.Phase != handoff.Aborted || m.Decision.Source != (handoff.Identity{Incarnation: c.boot.Source.Incarnation, Release: c.boot.Source.Release}) || m.Decision.Target != (handoff.Identity{Incarnation: c.identity.Incarnation, Release: c.identity.Release}) || m.Decision.Epoch != c.boot.StateEpoch {
			return reply, handoff.ErrConflict
		}
		actual, err := handoff.ReadDecision(c.cfg.DataDir)
		if err != nil {
			return reply, err
		}
		if actual != m.Decision {
			return reply, handoff.ErrConflict
		}
		if err = c.client.RestoreRemote(ctx, actual.Epoch); err != nil {
			return reply, err
		}
		if err = c.client.RetryReleases(ctx); err != nil {
			return reply, err
		}
		c.receiver, c.verified, c.decision = nil, false, handoff.Decision{}
		c.viewRaw, c.viewDecision = nil, handoff.Decision{}
	default:
		return reply, fmt.Errorf("UNKNOWN_CANDIDATE_COMMAND")
	}
	reply.Identity = c.business.WorkerIdentity(c.identity)
	if c.viewDecision.ID != "" {
		version := imageVersion(c.viewDecision)
		reply.View = &version
	}
	if c.owner != nil {
		reply.Decision = c.owner.owner.Decision()
		status := c.owner.shared.BarrierStatus()
		reply.State = &status
		reply.Policy = &c.owner.policy
	} else {
		reply.Policy = &c.boot.Policy
	}
	return reply, nil
}

// activate is serialized with commands, including recovery after a lost reply.
// Both listeners must succeed; a partial activation remains retryable.
func (c *Candidate) activate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.owner == nil || !c.owner.Ready() {
		return fmt.Errorf("CANDIDATE_NOT_COMMITTED")
	}
	if len(c.public) != 0 {
		return nil
	}
	var listeners []net.Listener
	for _, socket := range c.sockets[:2] {
		listener, err := socket.Activate()
		if err != nil {
			for i := range listeners {
				_ = c.sockets[i].Pause()
			}
			return err
		}
		listeners = append(listeners, listener)
	}
	for i, handler := range []http.Handler{c.owner.DataHandler(), c.owner.AdminHandler()} {
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
		c.public = append(c.public, server)
		go func() { _ = server.Serve(listeners[i]) }()
	}
	return nil
}

func (c *Candidate) committed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner != nil
}

// ServeCandidate is the process --candidate-fd=7 entry. Exactly three listening
// descriptors, one read-only config and the private channel are inherited.
// Public listeners remain dormant until the complete owner has committed.
func ServeCandidate(ctx context.Context, release string) (err error) {
	var sockets []*Socket
	defer func() {
		for _, s := range sockets {
			s.Close()
		}
	}()
	for fd := 3; fd <= 5; fd++ {
		if err := validateListener(fd); err != nil {
			return err
		}
		f := os.NewFile(uintptr(fd), "candidate-listener")
		s, err := Inherit(f)
		f.Close()
		if err != nil {
			return err
		}
		sockets = append(sockets, s)
	}
	if sockets[0].Addr().Network() != "tcp" || sockets[1].Addr().Network() != "tcp" || sockets[2].Addr().Network() != "unix" {
		return fmt.Errorf("INVALID_CANDIDATE_LISTENER_SET")
	}
	f := os.NewFile(7, "candidate-channel")
	conn, err := net.FileConn(f)
	f.Close()
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	first, err := readPeer(conn)
	if err != nil {
		return err
	}
	if first.Action != "boot" || first.Sequence != 1 || first.Boot == nil {
		return fmt.Errorf("MISSING_CANDIDATE_BOOTSTRAP")
	}
	booted := false
	defer func() {
		if !booted && err != nil {
			_ = writePeer(conn, peerMessage{Sequence: first.Sequence, Action: first.Action, Error: err.Error()})
		}
	}()
	config := os.NewFile(6, "candidate-configuration")
	cfg, err := appconfig.LoadReader(config, first.Boot.ConfigPath)
	config.Close()
	if err != nil {
		return err
	}
	for i, port := range []string{cfg.Port, cfg.AdminPort} {
		_, actual, err := net.SplitHostPort(sockets[i].Addr().String())
		if err != nil || actual != port {
			return fmt.Errorf("CANDIDATE_LISTENER_CONFIG_MISMATCH")
		}
	}
	c, err := openCandidate(cfg, *first.Boot, release)
	if err != nil {
		return err
	}
	defer c.Close()
	c.sockets = sockets
	listener, err := net.Listen("unix", c.boot.Target.Socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(c.boot.Target.Socket, 0600); err != nil {
		return err
	}
	private := &http.Server{Handler: c.privateHandler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second}
	defer private.Close()
	go func() { _ = private.Serve(listener) }()
	if err = writePeer(conn, peerMessage{Sequence: first.Sequence, Action: first.Action, Identity: c.business.WorkerIdentity(c.identity), Policy: &c.boot.Policy}); err != nil {
		return err
	}
	booted = true
	_ = conn.SetDeadline(time.Time{})
	sequence := first.Sequence
	for {
		m, readErr := readPeer(conn)
		if readErr != nil {
			if !c.committed() {
				return readErr // the source disappeared before ownership committed
			}
			// A normal source exit cannot kill the committed process. Listener,
			// MainPID and controller completion are confirmed separately.
			select {
			case <-ctx.Done():
				return nil
			}
		}
		if m.Sequence != sequence+1 || m.Deadline.IsZero() {
			return fmt.Errorf("INVALID_CANDIDATE_COMMAND_SEQUENCE")
		}
		sequence = m.Sequence
		commandCtx, cancel := context.WithDeadline(ctx, m.Deadline)
		reply, commandErr := c.command(commandCtx, m)
		cancel()
		if commandErr != nil {
			reply.Error = commandErr.Error()
		}
		_ = conn.SetWriteDeadline(m.Deadline)
		if writeErr := writePeer(conn, reply); writeErr != nil {
			if !c.committed() {
				return writeErr
			}
			conn.Close()
		}
		err = nil
	}
}
