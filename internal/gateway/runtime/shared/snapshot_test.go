package shared

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSnapshotChunksPreserveBytesAndEnforceEveryBoundary(t *testing.T) {
	raw := bytes.Repeat([]byte("a"), SnapshotChunkBytes+17)
	snapshot, err := NewSnapshot(raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshot.Manifest()
	if manifest.Chunks != 2 {
		t.Fatal(manifest)
	}
	if _, err := NewSnapshotReceiver(manifest, len(raw)-1); err == nil {
		t.Fatal("total limit ignored")
	}
	if _, err := snapshot.Chunk(-1); err == nil {
		t.Fatal("negative chunk index accepted")
	}
	if _, err := snapshot.Chunk(2); err == nil {
		t.Fatal("extra chunk index accepted")
	}
	for name, corrupt := range map[string]func(*SnapshotChunk){
		"snapshot": func(c *SnapshotChunk) { c.ID = "another" },
		"gap":      func(c *SnapshotChunk) { c.Index++ },
		"offset":   func(c *SnapshotChunk) { c.Offset++ },
		"short":    func(c *SnapshotChunk) { c.Data = c.Data[:len(c.Data)-1] },
		"oversize": func(c *SnapshotChunk) { c.Data = append(c.Data, 'x') },
		"corrupt":  func(c *SnapshotChunk) { c.Data[0] ^= 1 },
		"digest":   func(c *SnapshotChunk) { c.Digest = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			r, err := NewSnapshotReceiver(manifest, len(raw))
			if err != nil {
				t.Fatal(err)
			}
			chunk, _ := snapshot.Chunk(0)
			corrupt(&chunk)
			if err := r.Add(chunk); err == nil {
				t.Fatal("invalid chunk accepted")
			}
			valid, _ := snapshot.Chunk(0)
			if err := r.Add(valid); err == nil {
				t.Fatal("invalid transfer silently recovered")
			}
			if data, err := r.Finish(); err == nil || data != nil {
				t.Fatal("partial corrupt image published")
			}
		})
	}
	r, err := NewSnapshotReceiver(manifest, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	first, _ := snapshot.Chunk(0)
	if err := r.Add(first); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Finish(); err == nil {
		t.Fatal("truncated transfer published")
	}
	first.Data[0] = '!'
	last, _ := snapshot.Chunk(1)
	if len(last.Data) != 17 {
		t.Fatal("invalid final chunk length")
	}
	if err := r.Add(last); err != nil {
		t.Fatal(err)
	}
	got, err := r.Finish()
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatal("snapshot changed or retained caller buffer", err)
	}
	if err := r.Add(last); err == nil {
		t.Fatal("finished transfer accepted another chunk")
	}
	badManifest := manifest
	badManifest.Digest = strings.Repeat("0", 64)
	r, err = NewSnapshotReceiver(badManifest, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < manifest.Chunks; i++ {
		chunk, _ := snapshot.Chunk(i)
		if err := r.Add(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := r.Finish(); err == nil || got != nil {
		t.Fatal("whole-image digest ignored")
	}
}

func TestSnapshotCaptureKeepsQueueDeadlinesAndRejectsConcurrentResume(t *testing.T) {
	s := NewServer("token", Backend{Execute: func(Request, *Leases) Response { return Response{} }})
	if err := s.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	started, proceed := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(proceed) })
	defer finish()
	want := errors.New("encoding failed")
	done := make(chan error, 1)
	go func() {
		done <- s.Capture(func(Transfer) error { close(started); <-proceed; return want })
	}()
	<-started
	if s.BarrierStatus().Captures != 1 {
		t.Fatal("capture was not pinned")
	}
	if err := s.Resume(); err == nil {
		t.Fatal("resume reopened owner during capture")
	}
	c := NewLocalClient(s, "candidate")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	call := make(chan error, 1)
	go func() { call <- c.CallContext(ctx, "update", Request{}, nil) }()
	select {
	case err := <-call:
		if err == nil {
			t.Fatal("state call executed during capture")
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot capture blocked the request deadline")
	}
	finish()
	if err := <-done; !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := s.Resume(); err != nil {
		t.Fatal("failed capture stranded admission", err)
	}
	if err := c.Call("update", Request{}, nil); err != nil {
		t.Fatal("snapshot timeout poisoned state client", err)
	}
}
