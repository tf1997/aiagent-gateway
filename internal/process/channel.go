package process

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

const peerProtocol = 2
const maxPeerFrame = 2 << 20 // includes JSON/base64 framing of a 1 MiB chunk

// peerMessage travels only over the inherited socketpair. Credentials never
// appear in argv, persisted reports, or a public management endpoint.
type peerMessage struct {
	Sequence   uint64                  `json:"sequence"`
	Action     string                  `json:"action"`
	Deadline   time.Time               `json:"deadline,omitempty"`
	Boot       *candidateBoot          `json:"boot,omitempty"`
	Identity   p.Identity              `json:"identity,omitempty"`
	Decision   handoff.Decision        `json:"decision,omitempty"`
	Manifest   shared.SnapshotManifest `json:"manifest,omitempty"`
	Chunk      *shared.SnapshotChunk   `json:"chunk,omitempty"`
	Operations bool                    `json:"operations,omitempty"`
	Cursor     *state.Cursor           `json:"cursor,omitempty"`
	Preview    bool                    `json:"preview,omitempty"`
	Delta      bool                    `json:"delta,omitempty"`
	View       *shared.ImageVersion    `json:"view,omitempty"`
	Policy     *p.HandoffPolicy        `json:"handoff_policy,omitempty"`
	State      *shared.BarrierStatus   `json:"state,omitempty"`
	Stats      *p.Statistics           `json:"stats,omitempty"`
	Lifecycle  bool                    `json:"lifecycle_ready,omitempty"`
	Error      string                  `json:"error,omitempty"`
}

type candidateBoot struct {
	Protocol         int             `json:"protocol"`
	SnapshotFormat   string          `json:"snapshot_format"`
	ConfigPath       string          `json:"config_path"`
	Source           p.Target        `json:"source"`
	Target           p.Target        `json:"target"`
	ParentPID        int             `json:"parent_pid"`
	StateSocket      string          `json:"state_socket"`
	StateEpoch       string          `json:"state_epoch"`
	ConfigRevision   uint64          `json:"config_revision"`
	Token            string          `json:"token"`
	MaxSnapshotBytes int             `json:"max_snapshot_bytes"`
	MaxDeltaBytes    int             `json:"max_delta_bytes"`
	DeltaFormat      string          `json:"delta_format"`
	Policy           p.HandoffPolicy `json:"handoff_policy"`
	Service          bool            `json:"service,omitempty"`
	ReleasesDir      string          `json:"releases_dir,omitempty"`
	Unit             string          `json:"unit,omitempty"`
	ArtifactUID      uint32          `json:"artifact_uid"`
}

type peerChannel struct {
	conn net.Conn
	mu   sync.Mutex
	next uint64
}

func peerPair() (net.Conn, *os.File, error) {
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	f := os.NewFile(uintptr(fds[0]), "candidate-parent")
	child := os.NewFile(uintptr(fds[1]), "candidate-child")
	conn, err := net.FileConn(f)
	f.Close()
	if err != nil {
		child.Close()
		return nil, nil, err
	}
	return conn, child, nil
}

func readPeer(r io.Reader) (peerMessage, error) {
	var m peerMessage
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return m, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > maxPeerFrame {
		return m, fmt.Errorf("INVALID_PEER_FRAME_SIZE")
	}
	raw := make([]byte, n)
	if _, err := io.ReadFull(r, raw); err != nil {
		return m, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, fmt.Errorf("INVALID_PEER_FRAME")
	}
	if d.Decode(new(any)) != io.EOF {
		return m, fmt.Errorf("INVALID_PEER_FRAME")
	}
	return m, nil
}

func writePeer(w io.Writer, m peerMessage) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(raw) > maxPeerFrame {
		return fmt.Errorf("PEER_FRAME_CAPACITY")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err = io.Copy(w, bytes.NewReader(header[:])); err != nil {
		return err
	}
	_, err = io.Copy(w, bytes.NewReader(raw))
	return err
}

// call never retries a lost reply. In particular, an uncertain commit must be
// resolved through owner.lock and the durable decision, not a second command.
func (c *peerChannel) call(ctx context.Context, m peerMessage) (peerMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return peerMessage{}, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return peerMessage{}, err
	}
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	defer stop()
	c.next++
	m.Sequence, m.Deadline = c.next, deadline
	if err := writePeer(c.conn, m); err != nil {
		c.conn.Close()
		return peerMessage{}, err
	}
	reply, err := readPeer(c.conn)
	if err != nil {
		c.conn.Close()
		return reply, err
	}
	if reply.Sequence != m.Sequence || reply.Action != m.Action {
		c.conn.Close()
		return reply, fmt.Errorf("PEER_REPLY_MISMATCH")
	}
	if reply.Error != "" {
		return reply, fmt.Errorf("candidate: %s", reply.Error)
	}
	return reply, nil
}
