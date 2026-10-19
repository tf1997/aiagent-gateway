package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

type ChildOptions struct {
	ConfigPath, ReleasesDir, Token string
	OwnerUID                       uint32
	Freeze                         func(bool)
	Retire                         func(string)
	Count                          func(string) int64
	Output                         io.Writer
}

type child struct {
	target p.Target
	cmd    *exec.Cmd
	parent *os.File
	done   chan struct{}
}

// ChildProcessDriver only manages children it created. Persisted PIDs and
// listeners never authorize adopting or terminating another process.
type ChildProcessDriver struct {
	mu       sync.Mutex
	opts     ChildOptions
	children map[string]*child
	closed   bool
}

func NewChildProcessDriver(opts ChildOptions) *ChildProcessDriver {
	return &ChildProcessDriver{opts: opts, children: make(map[string]*child)}
}

// ValidateArtifact checks every component inside the trusted release tree.
// The root is selected by the owner-protected deployment manifest.
func ValidateArtifact(root, artifact string, uid uint32) error {
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) || filepath.Clean(artifact) != artifact || filepath.Dir(filepath.Dir(artifact)) != root || filepath.Base(artifact) != "gateway" {
		return fmt.Errorf("artifact outside release directory")
	}
	digest := filepath.Base(filepath.Dir(artifact))
	if len(digest) != 64 {
		return fmt.Errorf("invalid release digest")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return fmt.Errorf("invalid release digest")
	}
	for _, path := range []string{root, filepath.Dir(artifact), artifact} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != uid || info.Mode().Perm()&0022 != 0 || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("untrusted release path: %s", path)
		}
		if path == artifact {
			if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
				return fmt.Errorf("release is not executable")
			}
		} else if !info.IsDir() {
			return fmt.Errorf("release parent is not a directory")
		}
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	// On macOS /tmp and /var are system symlinks; validate the resolved ancestors
	// too, permitting only the sticky system temporary directory to be writable.
	for path := resolved; ; path = filepath.Dir(path) {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (st.Uid != 0 && st.Uid != uid) || (info.Mode().Perm()&0022 != 0 && !(st.Uid == 0 && info.Mode()&os.ModeSticky != 0)) {
			return fmt.Errorf("untrusted release ancestor: %s", path)
		}
		if filepath.Dir(path) == path {
			break
		}
	}
	f, err := os.Open(artifact)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return fmt.Errorf("artifact digest mismatch")
	}
	return nil
}

func (d *ChildProcessDriver) Start(ctx context.Context, artifact, slot string) (p.Target, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return p.Target{}, fmt.Errorf("host is stopping")
	}
	if slot != "a" && slot != "b" {
		return p.Target{}, fmt.Errorf("invalid worker slot")
	}
	if err := ctx.Err(); err != nil {
		return p.Target{}, err
	}
	if err := ValidateArtifact(d.opts.ReleasesDir, artifact, d.opts.OwnerUID); err != nil {
		return p.Target{}, err
	}
	if old := d.children[slot]; old != nil {
		select {
		case <-old.done:
			old.parent.Close()
			d.opts.Retire(old.target.Incarnation)
			delete(d.children, slot)
		default:
			if old.target.Artifact != artifact {
				return p.Target{}, fmt.Errorf("slot is occupied by another release")
			}
			id, err := d.identity(ctx, old)
			if err != nil || !id.Ready {
				return p.Target{}, fmt.Errorf("existing worker is not ready: %v", err)
			}
			return old.target, nil
		}
	}
	// Reserve private ports until immediately before exec. Handshake identity
	// (including the actual child PID) prevents accepting a raced listener.
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return p.Target{}, err
	}
	defer data.Close()
	admin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return p.Target{}, err
	}
	defer admin.Close()
	t := p.Target{Artifact: artifact, Release: filepath.Base(filepath.Dir(artifact)), Instance: slot, Incarnation: slot + ":" + p.ID(""), URL: "http://" + data.Addr().String(), AdminURL: "http://" + admin.Addr().String()}
	reader, writer, err := os.Pipe()
	if err != nil {
		return t, err
	}
	defer reader.Close()
	cmd := exec.Command(artifact, "worker", "--config="+d.opts.ConfigPath, "--slot="+slot, "--release="+t.Release, "--incarnation="+t.Incarnation, "--parent-fd=3", "--listen="+data.Addr().String(), "--admin="+admin.Addr().String())
	cmd.ExtraFiles = []*os.File{reader}
	cmd.Stdout, cmd.Stderr = d.opts.Output, d.opts.Output
	data.Close()
	admin.Close()
	if err = cmd.Start(); err != nil {
		writer.Close()
		return t, err
	}
	c := &child{target: t, cmd: cmd, parent: writer, done: make(chan struct{})}
	d.children[slot] = c
	go func() { _ = cmd.Wait(); close(c.done) }()
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		id, e := d.identity(readyCtx, c)
		if e == nil && id.Ready {
			return t, nil
		}
		select {
		case <-c.done:
			writer.Close()
			d.opts.Retire(t.Incarnation)
			delete(d.children, slot)
			return t, fmt.Errorf("worker exited before readiness")
		case <-readyCtx.Done():
			_ = cmd.Process.Kill()
			<-c.done
			writer.Close()
			d.opts.Retire(t.Incarnation)
			delete(d.children, slot)
			return t, fmt.Errorf("worker readiness: %w", readyCtx.Err())
		case <-tick.C:
		}
	}
}

func (d *ChildProcessDriver) identity(ctx context.Context, c *child) (p.Identity, error) {
	select {
	case <-c.done:
		return p.Identity{}, fmt.Errorf("worker exited")
	default:
	}
	id, err := workerIdentity(ctx, d.opts.Token, c.target)
	if err != nil {
		return id, err
	}
	if id.PID != c.cmd.Process.Pid || id.Incarnation != c.target.Incarnation || id.Release != c.target.Release || id.Instance != c.target.Instance || id.Protocol != p.Version || id.StateProtocol != 1 {
		return id, fmt.Errorf("child identity/protocol mismatch")
	}
	return id, nil
}

func (d *ChildProcessDriver) Identity(ctx context.Context, target p.Target) (p.Identity, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.children[target.Instance]
	if c == nil || c.target != target {
		return p.Identity{}, fmt.Errorf("worker is not owned by this host")
	}
	return d.identity(ctx, c)
}

func (d *ChildProcessDriver) Stop(ctx context.Context, target p.Target) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.children[target.Instance]
	if c != nil && c.target.Incarnation == target.Incarnation && c.target != target {
		return fmt.Errorf("worker target does not match owned child")
	}
	for _, other := range d.children {
		if other != c && other.target.Incarnation == target.Incarnation {
			return fmt.Errorf("worker target does not match owned slot")
		}
	}
	if c != nil && c.target == target {
		select {
		case <-c.done:
		default:
			id, err := d.identity(ctx, c)
			if err != nil {
				return err
			}
			if id.Inflight != 0 || d.opts.Count(target.Incarnation) != 0 {
				return fmt.Errorf("worker still has active execution")
			}
			if err = c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				return err
			}
			select {
			case <-c.done:
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(35 * time.Second):
				return fmt.Errorf("worker did not exit")
			}
		}
		c.parent.Close()
		delete(d.children, target.Instance)
	}
	d.opts.Retire(target.Incarnation)
	return nil
}

func (d *ChildProcessDriver) Retire(ctx context.Context, target p.Target) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.children {
		if c.target.Incarnation == target.Incarnation {
			select {
			case <-c.done:
			default:
				return fmt.Errorf("cannot retire live worker")
			}
		}
	}
	d.opts.Retire(target.Incarnation)
	return nil
}

func (d *ChildProcessDriver) Freeze(ctx context.Context, frozen bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.opts.Freeze(frozen)
	return nil
}

// Close is used for host shutdown, not rollout. Closing the parent pipe also
// terminates children on hosts without systemd; rollout Stop drains first.
func (d *ChildProcessDriver) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, c := range d.children {
		c.parent.Close()
	}
	for slot, c := range d.children {
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			_ = c.cmd.Process.Kill()
			<-c.done
		}
		d.opts.Retire(c.target.Incarnation)
		delete(d.children, slot)
	}
}
