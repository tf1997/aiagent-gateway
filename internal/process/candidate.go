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
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

const maxCandidateSnapshot = 256 << 20

// Candidate holds only an executor and remote state access until COMMITTED.
// The executor is retained during adoption so existing streams keep leases.
type Candidate struct {
	frontend              *entry.Entry
	allocator             *candidateAllocator
	mu                    sync.Mutex
	cfg                   appconfig.Config
	boot                  candidateBoot
	identity              p.Identity
	client                *shared.Client
	business              *gateway.Gateway
	owner                 *Gateway
	decision              handoff.Decision
	receiver              *shared.SnapshotReceiver
	verified              bool
	sockets               []*Socket
	public                []*http.Server
	publicActive          bool
	preview, delta        bool
	operations            bool
	operationStream       bool
	compactResults        bool
	records               map[string]json.RawMessage
	recordHeader          gatewayImage
	cursor, pendingCursor state.Cursor
	viewRaw               []byte
	viewDecision          handoff.Decision
	prepared              *preparedHandoff
	service               *Service
	lifetime              context.Context
	wake                  func()
	serviceCancel         context.CancelFunc
	serviceDone           chan struct{}
	serviceErr            error
	serviceReady          bool
	callsSealed           bool
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
	c := &Candidate{cfg: cfg, boot: boot, serviceDone: make(chan struct{}), identity: p.Identity{PID: os.Getpid(), Release: release, Incarnation: t.Incarnation, Instance: t.Instance}}
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
	c.allocator = newCandidateAllocator(boot)
	limits := shared.QueueLimits{Requests: boot.Policy.QueueRequests, Bytes: boot.Policy.QueueBytes, Wait: time.Duration(boot.Policy.StateWaitMillis) * time.Millisecond}
	if err = c.client.ConfigureHandoffQueue(limits); err != nil {
		c.business.Close()
		c.client.Close()
		return nil, err
	}
	if err = c.allocator.gate.Configure(limits); err != nil {
		c.business.Close()
		c.client.Close()
		return nil, err
	}
	c.frontend, err = entry.NewFrontend(c.allocator, boot.Target, c.business.ExecuteLocal)
	if err != nil {
		c.business.Close()
		c.client.Close()
		return nil, err
	}
	source := boot.Source
	source.Socket = boot.StateSocket
	if err = c.frontend.SetPeer(source, boot.Token); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Candidate) Close() error {
	c.mu.Lock()
	if c.serviceCancel != nil {
		c.serviceCancel()
		done := c.serviceDone
		c.mu.Unlock()
		<-done
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	if c.service != nil {
		c.service.close()
	}
	if c.frontend != nil {
		c.frontend.Close()
	}
	if c.allocator != nil {
		c.allocator.client.CloseIdleConnections()
	}
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
			if decoder.Decode(&m) != nil || decoder.Decode(new(any)) != io.EOF || (m.Action != "identity" && m.Action != "activate" && m.Action != "takeover") {
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
	case "quiesce":
		if !c.callsSealed {
			if err := c.allocator.gate.Seal(ctx); err != nil {
				return reply, err
			}
			if err := c.client.Quiesce(ctx); err != nil {
				_ = c.allocator.gate.Resume()
				return reply, err
			}
			c.callsSealed = true
		}
	case "resume_calls":
		if err := c.resumeCalls(ctx); err != nil {
			return reply, err
		}
	case "stats":
		if c.owner == nil {
			return reply, fmt.Errorf("CANDIDATE_NOT_COMMITTED")
		}
		stats := c.owner.entry.Stats(time.Time{}, c.identity.Incarnation)
		reply.Stats = &stats
	case "activate":
		if err := c.activate(ctx); err != nil {
			return reply, err
		}
	case "takeover":
		if err := c.takeover(ctx); err != nil {
			return reply, err
		}
	case "accept":
		if err := c.acceptPublic(ctx); err != nil {
			return reply, err
		}
	case "pause":
		_ = c.sockets[publicSocket].Pause()
		for _, server := range c.public {
			server.SetKeepAlivesEnabled(false)
		}
		c.publicActive = false
	case "operation_stream":
		if c.owner != nil || c.receiver != nil || c.viewRaw != nil {
			return reply, fmt.Errorf("OPERATION_NEGOTIATION_TOO_LATE")
		}
		c.operationStream = true
		reply.Operations = true
	case "compact_results":
		if c.owner != nil || c.receiver != nil || c.viewRaw != nil {
			return reply, fmt.Errorf("RESULT_NEGOTIATION_TOO_LATE")
		}
		c.compactResults = true
	case "snapshot":
		if c.owner != nil || m.Decision.Validate() != nil || (!m.Preview && m.Decision.Phase != handoff.Sealed) || (m.Preview && m.Decision.Phase != handoff.Prepared) ||
			m.Decision.Source != (handoff.Identity{Incarnation: c.boot.Source.Incarnation, Release: c.boot.Source.Release}) ||
			m.Decision.Target != (handoff.Identity{Incarnation: c.identity.Incarnation, Release: c.identity.Release}) ||
			m.Decision.Epoch != c.boot.StateEpoch || (!m.Delta && !m.Operations && (m.Manifest.Digest != m.Decision.Snapshot.Digest || int64(m.Manifest.Bytes) != m.Decision.Snapshot.Bytes)) ||
			(m.Delta && (c.viewRaw == nil || !sameHandoff(c.viewDecision, m.Decision))) ||
			(m.Operations && (!c.operationStream || m.Delta || m.Cursor == nil || c.records == nil || !sameHandoff(c.viewDecision, m.Decision) || m.Cursor.Epoch != c.cursor.Epoch || m.Cursor.Handoff != c.cursor.Handoff)) {
			return reply, handoff.ErrConflict
		}
		budget := c.boot.MaxSnapshotBytes
		if m.Delta || m.Operations {
			budget = c.boot.MaxDeltaBytes
		}
		receiver, err := shared.NewSnapshotReceiver(m.Manifest, budget)
		if err != nil {
			return reply, err
		}
		c.receiver, c.decision, c.verified = receiver, m.Decision, false
		c.prepared = nil
		c.preview, c.delta, c.operations = m.Preview, m.Delta, m.Operations
		if m.Cursor != nil {
			c.pendingCursor = *m.Cursor
		}
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
		if c.operations {
			at, applyErr := state.ApplyOperations(c.records, c.cursor, raw, c.boot.MaxDeltaBytes, c.boot.MaxSnapshotBytes)
			if applyErr != nil {
				return reply, applyErr
			}
			if at != c.pendingCursor {
				c.records = nil
				return reply, fmt.Errorf("OPERATION_ACK_MISMATCH")
			}
			c.cursor = at
			if err = ctx.Err(); err != nil {
				return reply, err
			}
			if c.preview {
				break
			}
			raw, err = renderProcessRecords(c.recordHeader, c.records, at, c.boot.MaxSnapshotBytes)
			if err != nil {
				return reply, err
			}
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
		image, view, err := decodeGatewayImage(raw, validation, c.boot.MaxSnapshotBytes, c.cfg.Options())
		if err != nil {
			return reply, err
		}
		if image.Entry.Compact && !c.compactResults {
			return reply, fmt.Errorf("ENTRY_RECEIPTS_NOT_NEGOTIATED")
		}
		if err = ctx.Err(); err != nil {
			return reply, err
		}
		if !c.operations && c.operationStream {
			header, records, indexErr := processRecords(raw)
			if indexErr != nil {
				return reply, indexErr
			}
			c.recordHeader, c.records = header, records
			c.cursor = state.Cursor{Epoch: c.decision.Epoch, Handoff: c.decision.ID, Revision: c.decision.Snapshot.Revision, Digest: c.decision.Snapshot.Digest}
		}
		c.viewRaw, c.viewDecision = raw, c.decision
		c.verified = !c.preview
		if c.verified {
			c.prepared = &preparedHandoff{decision: c.decision, raw: raw, image: image, view: view}
		}
	case "commit":
		if !c.verified || c.preview || m.Decision != c.decision {
			return reply, handoff.ErrConflict
		}
		if c.owner == nil {
			n, err := openPreparedHandoffGateway(ctx, c.cfg, c.boot.Target, c.identity, c.decision, c.boot.MaxSnapshotBytes, c.client, c.business, false, c.boot.Policy, c.prepared)
			if err != nil {
				return reply, err
			}
			c.owner = n
			c.allocator.owner.Store(n)
			_ = c.allocator.gate.Resume()
			c.callsSealed = false
			c.viewRaw = nil
			c.records = nil
			c.prepared = nil
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
		if err = c.resumeCalls(ctx); err != nil {
			return reply, err
		}
		c.receiver, c.verified, c.decision = nil, false, handoff.Decision{}
		c.viewRaw, c.viewDecision = nil, handoff.Decision{}
		c.records, c.cursor = nil, state.Cursor{}
		c.prepared = nil
	default:
		return reply, fmt.Errorf("UNKNOWN_CANDIDATE_COMMAND")
	}
	reply.Identity = c.business.WorkerIdentity(c.identity)
	reply.Lifecycle = c.serviceReady
	// Old parents decode strictly; emit extended fields only after negotiation.
	if c.records != nil && c.operationStream {
		at := c.cursor
		reply.Cursor = &at
	}
	if !c.allocator.healthy() {
		reply.Identity.Ready = false
	}
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

func (c *Candidate) resumeCalls(ctx context.Context) error {
	if err := c.client.ResumeCalls(); err != nil {
		return err
	}
	if err := c.allocator.gate.Resume(); err != nil {
		return err
	}
	c.callsSealed = false
	return c.client.RetryReleases(ctx)
}

// activate is serialized with commands, including recovery after a lost reply.
// The shared HTTP listener starts accepting only once per activation.
func (c *Candidate) activate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.owner == nil || !c.owner.Ready() {
		return fmt.Errorf("CANDIDATE_NOT_COMMITTED")
	}
	return c.acceptPublic(ctx)
}

func (c *Candidate) acceptPublic(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.publicActive {
		return nil
	}
	if len(c.sockets) != inheritedSocketCount {
		return fmt.Errorf("MISSING_PUBLIC_LISTENERS")
	}
	listener, err := c.sockets[publicSocket].Activate()
	if err != nil {
		return err
	}
	data := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := c.allocator.owner.Load(); n != nil {
			n.DataHandler().ServeHTTP(w, r)
			return
		}
		c.frontend.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: gateway.PublicHandler(data, c.publicAdmin()), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	c.public = append(c.public, server)
	go func() { _ = server.Serve(listener) }()
	c.publicActive = true
	return nil
}

func (c *Candidate) takeover(ctx context.Context) error {
	if !c.boot.Service || c.owner == nil || c.lifetime == nil || !c.owner.Ready() {
		return fmt.Errorf("CANDIDATE_NOT_READY_FOR_SERVICE")
	}
	if c.serviceReady {
		return nil
	}
	if c.service == nil {
		var sockets [inheritedSocketCount]*Socket
		copy(sockets[:], c.sockets)
		s, err := newService(c.owner, sockets, ServiceOptions{ConfigPath: c.boot.ConfigPath, ReleasesDir: c.boot.ReleasesDir, Unit: c.boot.Unit, OwnerUID: c.boot.ArtifactUID, Output: os.Stderr}, true)
		if err != nil {
			return err
		}
		c.service = s
		s.queueStatus = func() (shared.BarrierStatus, shared.BarrierStatus) {
			return c.client.QueueStatus(), c.allocator.gate.Status()
		}
		s.opts.Policy = s.controller.report.Policy
		s.servers = append(s.servers, c.public...)
		s.ownerChanged = func(n *Gateway) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.owner = n
			c.business = n.business
			c.allocator.owner.Store(n)
		}
	}
	if err := c.service.acceptControl(); err != nil {
		return err
	}
	if err := c.service.ready(ctx, true); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(c.lifetime)
	c.serviceCancel = cancel
	c.serviceReady = true
	go func() { c.serviceErr = c.service.run(runCtx); close(c.serviceDone); c.wake() }()
	return nil
}

func (c *Candidate) committed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner != nil
}

// ServeCandidate is the process --candidate-fd=6 entry. Exactly two listening
// descriptors, one read-only config and the private channel are inherited.
// Public listeners remain dormant until the complete owner has committed.
func ServeCandidate(ctx context.Context, release string) (err error) {
	var sockets []*Socket
	defer func() {
		for _, s := range sockets {
			s.Close()
		}
	}()
	for fd := 3; fd < candidateConfigFD; fd++ {
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
	if sockets[publicSocket].Addr().Network() != "tcp" || sockets[controlSocket].Addr().Network() != "unix" {
		return fmt.Errorf("INVALID_CANDIDATE_LISTENER_SET")
	}
	f := os.NewFile(CandidateChannelFD, "candidate-channel")
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
	config := os.NewFile(candidateConfigFD, "candidate-configuration")
	cfg, err := appconfig.LoadReader(config, first.Boot.ConfigPath)
	config.Close()
	if err != nil {
		return err
	}
	_, actual, err := net.SplitHostPort(sockets[publicSocket].Addr().String())
	if err != nil || actual != cfg.Port || sockets[controlSocket].Addr().String() != cfg.ControlSocket {
		return fmt.Errorf("CANDIDATE_LISTENER_CONFIG_MISMATCH")
	}
	c, err := openCandidate(cfg, *first.Boot, release)
	if err != nil {
		return err
	}
	defer c.Close()
	receiptCtx, stopReceipts := context.WithCancel(ctx)
	receiptsDone := make(chan struct{})
	go func() {
		defer close(receiptsDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-receiptCtx.Done():
				return
			case <-ticker.C:
				c.allocator.retry(receiptCtx)
			}
		}
	}()
	defer func() { stopReceipts(); <-receiptsDone }()
	c.sockets = sockets
	c.lifetime = ctx
	c.wake = func() { _ = conn.Close() }
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
			if ctx.Err() != nil {
				return nil
			}
			if !c.committed() {
				return readErr // the source disappeared before ownership committed
			}
			// A normal source exit cannot kill the committed process. Listener,
			// MainPID and controller completion are confirmed separately.
			select {
			case <-ctx.Done():
				return nil
			case <-c.serviceDone:
				return c.serviceErr
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
