package process

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/service"
	"aiagent-gateway/internal/state"
)

type CandidateOptions struct {
	ConfigPath, Artifact, ReleasesDir string
	OwnerUID                          uint32
	Listeners                         [3]*Socket
	MaxSnapshotBytes                  int
	MaxDeltaBytes                     int
	Output                            io.Writer
	Service                           bool
	Unit                              string
}

// CandidateProcess refers only to a child created by this process. Persisted
// PIDs cannot authorize killing, adopting or replacing arbitrary processes.
type CandidateProcess struct {
	mu               sync.Mutex
	target           p.Target
	channel          *peerChannel
	cmd              *exec.Cmd
	done             chan struct{}
	waitErr          error
	server           *http.Server
	listener         net.Listener
	dir              string
	token            string
	verified         handoff.Decision
	stateOwner       atomic.Pointer[shared.Server]
	runtimeOwner     atomic.Pointer[Gateway]
	followRaw        []byte
	followCursor     state.Cursor
	operationStream  bool
	followDecision   handoff.Decision
	followErr        error
	followDeadline   time.Time
	maxSnapshotBytes int
	maxDeltaBytes    int
	policy           p.HandoffPolicy
}

func (c *CandidateProcess) Target() p.Target      { return c.target }
func (c *CandidateProcess) PID() int              { return c.cmd.Process.Pid }
func (c *CandidateProcess) Done() <-chan struct{} { return c.done }

func (c *CandidateProcess) Accept(ctx context.Context) error {
	_, err := c.channel.call(ctx, peerMessage{Action: "accept"})
	return err
}

func (c *CandidateProcess) Pause(ctx context.Context) error {
	_, err := c.channel.call(ctx, peerMessage{Action: "pause"})
	return err
}

func (c *CandidateProcess) Takeover(ctx context.Context) error {
	reply, err := c.channel.call(ctx, peerMessage{Action: "takeover"})
	if err != nil {
		reply, err = c.recoverCall(ctx, peerMessage{Action: "takeover"})
	}
	if err != nil {
		return err
	}
	if c.checkIdentity(reply.Identity) != nil || !reply.Identity.Ready || !reply.Lifecycle {
		return fmt.Errorf("CANDIDATE_LIFECYCLE_NOT_READY")
	}
	return nil
}

// StartCandidate enforces the two-process limit while the current owner is
// alive. A failed or stopped child is reaped and its leases retired first.
func (n *Gateway) StartCandidate(ctx context.Context, opts CandidateOptions) (*CandidateProcess, error) {
	return n.startCandidate(ctx, opts, []string{"process", "--candidate-fd=7"})
}

func (n *Gateway) startCandidate(ctx context.Context, opts CandidateOptions, args []string) (_ *CandidateProcess, err error) {
	n.handoffMu.Lock()
	defer n.handoffMu.Unlock()
	if !n.StorageHealthy() || !n.execution.Load() {
		return nil, fmt.Errorf("SOURCE_NOT_READY")
	}
	if n.candidate != nil {
		select {
		case <-n.candidate.done:
			if err := n.entry.RetireIngress(n.candidate.target.Incarnation); err != nil {
				return nil, err
			}
			if err := n.shared.Retire(n.candidate.target.Incarnation); err != nil {
				return nil, err
			}
			n.candidate.closeTransport()
			n.candidate = nil
		default:
			if n.candidate.target.Artifact == opts.Artifact {
				return n.candidate, nil
			}
			return nil, fmt.Errorf("CANDIDATE_ALREADY_ACTIVE")
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = service.ValidateArtifact(opts.ReleasesDir, opts.Artifact, opts.OwnerUID); err != nil {
		return nil, err
	}
	if opts.MaxSnapshotBytes <= 0 || opts.MaxSnapshotBytes > n.policy.SnapshotBytes {
		return nil, fmt.Errorf("INVALID_CANDIDATE_SNAPSHOT_BUDGET")
	}
	if opts.MaxDeltaBytes == 0 {
		opts.MaxDeltaBytes = n.policy.DeltaBytes
	}
	if opts.MaxDeltaBytes < 0 || opts.MaxDeltaBytes > n.policy.DeltaBytes {
		return nil, fmt.Errorf("INVALID_CANDIDATE_DELTA_BUDGET")
	}
	config, err := os.Open(opts.ConfigPath)
	if err != nil {
		return nil, err
	}
	defer config.Close()
	var files []*os.File
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, s := range opts.Listeners {
		if s == nil {
			return nil, fmt.Errorf("MISSING_CANDIDATE_LISTENER")
		}
		f, err := s.File()
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	files = append(files, config)
	conn, child, err := peerPair()
	if err != nil {
		return nil, err
	}
	files = append(files, child)
	c := &CandidateProcess{channel: &peerChannel{conn: conn}, done: make(chan struct{}),
		maxSnapshotBytes: opts.MaxSnapshotBytes, maxDeltaBytes: opts.MaxDeltaBytes, policy: n.policy}
	defer func() {
		if err != nil {
			if c.cmd != nil && c.cmd.Process != nil {
				_ = c.cmd.Process.Kill()
				<-c.done
				if c.waitErr != nil {
					err = fmt.Errorf("%w; child exit: %v", err, c.waitErr)
				}
				if retireErr := n.shared.Retire(c.target.Incarnation); retireErr != nil {
					// Preserve the record so the next Start cannot skip retirement.
					n.candidate = c
					err = fmt.Errorf("%w; candidate retirement: %v", err, retireErr)
				}
			}
			c.closeTransport()
		}
	}()
	// Private paths are short even when incarnations and artifact digests are
	// long. The directory belongs to this exact attempt and is never reused.
	root := n.privateRoot
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	c.dir, err = os.MkdirTemp(root, "")
	if err != nil {
		return nil, err
	}
	slot := "a"
	if n.target.Instance == "a" {
		slot = "b"
	}
	c.target = p.Target{Artifact: opts.Artifact, Release: filepath.Base(filepath.Dir(opts.Artifact)), Instance: slot,
		Incarnation: p.ID("process_"), URL: "http://gateway", Socket: filepath.Join(c.dir, "exec.sock")}
	stateSocket := filepath.Join(c.dir, "state.sock")
	token := p.ID("peer_")
	c.token = token
	c.listener, err = net.Listen("unix", stateSocket)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(stateSocket, 0600); err != nil {
		return nil, err
	}
	c.stateOwner.Store(n.shared)
	c.runtimeOwner.Store(n)
	c.server = &http.Server{Handler: c.sourceHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = c.server.Serve(c.listener) }()
	c.cmd = exec.Command(opts.Artifact, args...)
	c.cmd.ExtraFiles = files
	c.cmd.Stdout, c.cmd.Stderr = opts.Output, opts.Output
	if err = c.cmd.Start(); err != nil {
		return nil, err
	}
	go func() { c.waitErr = c.cmd.Wait(); close(c.done) }()
	for _, f := range files {
		f.Close()
	}
	files = nil
	boot := candidateBoot{Protocol: peerProtocol, SnapshotFormat: gatewaySnapshotFormat, ConfigPath: opts.ConfigPath,
		Source: n.target, Target: c.target, ParentPID: os.Getpid(), StateSocket: stateSocket, StateEpoch: n.owner.Decision().Epoch,
		ConfigRevision: n.core.WorkerIdentity(n.identity).ConfigRevision, Token: token, MaxSnapshotBytes: opts.MaxSnapshotBytes,
		MaxDeltaBytes: opts.MaxDeltaBytes, DeltaFormat: shared.DeltaFormat, Policy: c.policy,
		Service: opts.Service, ReleasesDir: opts.ReleasesDir, Unit: opts.Unit, ArtifactUID: opts.OwnerUID}
	reply, err := c.channel.call(ctx, peerMessage{Action: "boot", Boot: &boot})
	if err != nil {
		return nil, err
	}
	if err = c.checkIdentity(reply.Identity); err != nil || !reply.Identity.Ready || reply.Identity.ConfigRevision != boot.ConfigRevision || reply.Policy == nil || *reply.Policy != c.policy {
		return nil, fmt.Errorf("CANDIDATE_NOT_READY: %v", err)
	}
	// An explicit capability request preserves both upgrade and rollback with
	// checkpoint-only peers. A lost or malformed reply is never a downgrade.
	reply, err = c.channel.call(ctx, peerMessage{Action: "operation_stream"})
	if err != nil {
		if reply.Error != "UNKNOWN_CANDIDATE_COMMAND" {
			return nil, err
		}
	} else {
		if !reply.Operations || c.checkIdentity(reply.Identity) != nil {
			return nil, fmt.Errorf("INVALID_OPERATION_NEGOTIATION")
		}
		c.operationStream = true
	}
	if err = n.entry.SetPeer(c.target, token); err != nil {
		return nil, err
	}
	n.candidate = c
	return c, nil
}

func (c *CandidateProcess) checkIdentity(id p.Identity) error {
	if id.PID != c.PID() || id.Release != c.target.Release || id.Incarnation != c.target.Incarnation || id.Instance != c.target.Instance || id.Protocol != p.Version || id.StateProtocol != 1 {
		return fmt.Errorf("CANDIDATE_IDENTITY_MISMATCH")
	}
	return nil
}

func (c *CandidateProcess) Identity(ctx context.Context) (p.Identity, error) {
	reply, err := c.channel.call(ctx, peerMessage{Action: "identity"})
	if err != nil {
		return reply.Identity, err
	}
	return reply.Identity, c.checkIdentity(reply.Identity)
}

// ValidateHandoff transfers the exact sealed image before the source lets go
// of any writer lock. A successful return is an acknowledgement of all chunks
// and full business invariants, bound to the entire durable decision.
func (c *CandidateProcess) ValidateHandoff(ctx context.Context, source *Gateway, d handoff.Decision, maxBytes int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.followDecision.ID == d.ID && !c.followDeadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, c.followDeadline)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if maxBytes <= 0 || maxBytes > c.maxSnapshotBytes {
		return fmt.Errorf("INVALID_HANDOFF_SNAPSHOT_BUDGET")
	}
	if d.Phase != handoff.Sealed || d.Target != (handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}) {
		return handoff.ErrConflict
	}
	source.handoffMu.Lock()
	if source.candidate != c || source.owner.Decision() != d {
		source.handoffMu.Unlock()
		return handoff.ErrConflict
	}
	raw, err := source.owner.LoadSnapshot(d.ID, maxBytes)
	source.handoffMu.Unlock()
	if err != nil {
		return err
	}
	if c.verified == d {
		return nil
	}
	c.verified = handoff.Decision{}
	if c.followCursor.Handoff == d.ID {
		if _, err = c.sendOperations(ctx, source, d, false); err != nil {
			return err
		}
	} else if _, err = c.sendHandoffImage(ctx, d, raw, maxBytes, false); err != nil {
		return err
	}
	// Final validation advances the acknowledged base too. A repeated
	// validation must not resend a delta against the previous preview.
	c.followRaw, c.followDecision = raw, d
	c.verified = d
	return nil
}

func (c *CandidateProcess) Commit(ctx context.Context, d handoff.Decision) (handoff.Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verified != d || d.Phase != handoff.Sealed {
		return handoff.Decision{}, handoff.ErrConflict
	}
	reply, err := c.channel.call(ctx, peerMessage{Action: "commit", Decision: d})
	if err != nil {
		return reply.Decision, err
	}
	expected := d
	expected.Phase, expected.Owner, expected.Epoch, expected.ColdStart = handoff.Committed, d.Target, d.NextEpoch, d.Target.Release
	if reply.Decision != expected || c.checkIdentity(reply.Identity) != nil {
		return reply.Decision, handoff.ErrConflict
	}
	c.followRaw = nil
	return reply.Decision, nil
}

func (c *CandidateProcess) Activate(ctx context.Context) error {
	_, err := c.channel.call(ctx, peerMessage{Action: "activate"})
	return err
}

// RecoverCommitted confirms both the durable choice and the same live child
// before retrying activation over its authenticated private endpoint. It never
// resends COMMIT or interprets a transport timeout as permission to roll back.
func (c *CandidateProcess) RecoverCommitted(ctx context.Context, dataDir string, sealed handoff.Decision) (handoff.Decision, error) {
	expected := sealed
	expected.Phase, expected.Owner, expected.Epoch, expected.ColdStart = handoff.Committed, sealed.Target, sealed.NextEpoch, sealed.Target.Release
	d, err := handoff.ReadDecision(dataDir)
	if err != nil {
		return d, err
	}
	if d.Phase == handoff.Completed {
		expected.Phase = handoff.Completed
	}
	if d != expected || d.Target != (handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}) {
		return d, handoff.ErrConflict
	}
	for _, action := range []string{"identity", "activate"} {
		reply, err := c.recoverCall(ctx, peerMessage{Action: action})
		if err != nil {
			return d, err
		}
		if reply.Decision != d || c.checkIdentity(reply.Identity) != nil || !reply.Identity.Ready {
			return d, handoff.ErrConflict
		}
	}
	return d, nil
}

func (c *CandidateProcess) recoverCall(ctx context.Context, m peerMessage) (peerMessage, error) {
	var reply peerMessage
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", c.target.Socket)
	}}
	defer transport.CloseIdleConnections()
	raw, err := json.Marshal(m)
	if err != nil {
		return reply, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway/_process/recover", bytes.NewReader(raw))
	if err != nil {
		return reply, err
	}
	req.Header.Set(p.AuthHeader, c.token)
	req.Header.Set(p.WorkerHeader, c.target.Incarnation)
	req.Header.Set(p.RequestHeader, p.ID("recovery_"))
	res, err := (&http.Client{Transport: transport, Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return reply, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("CANDIDATE_RECOVERY_HTTP_%d", res.StatusCode)
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, 16<<10)).Decode(&reply); err != nil {
		return reply, err
	}
	if reply.Error != "" {
		return reply, fmt.Errorf("candidate: %s", reply.Error)
	}
	if reply.Action != m.Action {
		return reply, fmt.Errorf("PEER_REPLY_MISMATCH")
	}
	return reply, nil
}

// RestoreSource reconnects an existing candidate to the latest source image.
// restored must already own all writer locks and a durable ABORTED decision.
func (c *CandidateProcess) RestoreSource(ctx context.Context, restored *Gateway, sealed handoff.Decision) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := restored.owner.Decision()
	expected := sealed
	expected.Phase = handoff.Aborted
	if d != expected || d.Target != (handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}) || !restored.Ready() {
		return handoff.ErrConflict
	}
	// An uncertain abort reply must not make this still-live child disappear
	// from the restored process's two-instance accounting.
	restored.handoffMu.Lock()
	if restored.candidate != nil && restored.candidate != c {
		restored.handoffMu.Unlock()
		return handoff.ErrConflict
	}
	restored.candidate = c
	restored.handoffMu.Unlock()
	c.stateOwner.Store(restored.shared)
	c.runtimeOwner.Store(restored)
	if err := restored.entry.SetPeer(c.target, c.token); err != nil {
		return err
	}
	if _, err := c.channel.call(ctx, peerMessage{Action: "abort", Decision: d}); err != nil {
		return err
	}
	c.verified = handoff.Decision{}
	c.followRaw, c.followDecision, c.followErr = nil, handoff.Decision{}, nil
	c.followCursor = state.Cursor{}
	c.followDeadline = time.Time{}
	return nil
}

// Stop only terminates this owned child after its execution and state leases
// drain. It refuses a committed target even when the commit acknowledgement
// was lost. Shutdown after commit belongs to the new service owner.
func (c *CandidateProcess) Stop(ctx context.Context, source *Gateway) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, err := handoff.ReadDecision(source.dataDir)
	if err != nil {
		return err
	}
	if d.Owner.Incarnation == c.target.Incarnation && (d.Phase == handoff.Committed || d.Phase == handoff.Completed) {
		return handoff.ErrCommitted
	}
	select {
	case <-c.done:
	default:
		id, err := c.Identity(ctx)
		if err != nil {
			return err
		}
		if id.Inflight != 0 || source.shared.Count(c.target.Incarnation) != 0 || source.entry.IngressActive(c.target.Incarnation) != 0 {
			return fmt.Errorf("CANDIDATE_NOT_DRAINED")
		}
		if err = c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err = source.shared.Retire(c.target.Incarnation); err != nil {
		return err
	}
	if err = source.entry.RetireIngress(c.target.Incarnation); err != nil {
		return err
	}
	c.closeTransport()
	return nil
}

// Detach closes only source-owned communication. It never signals the child.
func (c *CandidateProcess) Detach() { c.closeTransport() }

func (c *CandidateProcess) closeTransport() {
	if c.channel != nil {
		c.channel.conn.Close()
	}
	if c.server != nil {
		c.server.Close()
	}
	if c.listener != nil {
		c.listener.Close()
	}
	if c.dir != "" {
		_ = os.Remove(c.dir) // succeeds only after the child's endpoint is gone
	}
}
