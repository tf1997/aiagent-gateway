package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"aiagent-gateway/internal/releaseprotocol"
)

const SnapshotChunkBytes = 1 << 20

type SnapshotManifest struct {
	ID     string `json:"snapshot_id"`
	Bytes  int    `json:"bytes"`
	Chunks int    `json:"chunks"`
	Digest string `json:"sha256"`
}

type SnapshotChunk struct {
	ID     string `json:"snapshot_id"`
	Index  int    `json:"index"`
	Offset int    `json:"offset"`
	Data   []byte `json:"data"`
	Digest string `json:"sha256"`
}

// Snapshot is immutable and serves bounded, detached chunks. The manifest must
// be bound to the authenticated handoff; a digest alone is not authentication.
type Snapshot struct {
	manifest SnapshotManifest
	raw      []byte
}

func NewSnapshot(raw []byte, maxBytes int) (*Snapshot, error) {
	if maxBytes <= 0 || len(raw) == 0 {
		return nil, fmt.Errorf("INVALID_SNAPSHOT_BUDGET_OR_CONTENT")
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("SNAPSHOT_CAPACITY")
	}
	copy := bytes.Clone(raw)
	sum := sha256.Sum256(copy)
	return &Snapshot{SnapshotManifest{releaseprotocol.ID("snapshot_"), len(copy), 1 + (len(copy)-1)/SnapshotChunkBytes, hex.EncodeToString(sum[:])}, copy}, nil
}

func (s *Snapshot) Manifest() SnapshotManifest { return s.manifest }

func (s *Snapshot) Chunk(index int) (SnapshotChunk, error) {
	if index < 0 || index >= s.manifest.Chunks {
		return SnapshotChunk{}, fmt.Errorf("INVALID_SNAPSHOT_CHUNK_INDEX")
	}
	offset := index * SnapshotChunkBytes
	data := bytes.Clone(s.raw[offset : offset+min(SnapshotChunkBytes, len(s.raw)-offset)])
	sum := sha256.Sum256(data)
	return SnapshotChunk{s.manifest.ID, index, offset, data, hex.EncodeToString(sum[:])}, nil
}

// SnapshotReceiver accepts one ordered stream. It never publishes a partial
// image; a malformed chunk invalidates the receiver until a fresh transfer.
// This in-memory staging does not persist a SEALED decision or activate state.
type SnapshotReceiver struct {
	mu       sync.Mutex
	manifest SnapshotManifest
	next     int
	raw      []byte
	failed   error
	finished bool
}

func NewSnapshotReceiver(manifest SnapshotManifest, maxBytes int) (*SnapshotReceiver, error) {
	digest, err := hex.DecodeString(manifest.Digest)
	if maxBytes <= 0 || manifest.ID == "" || manifest.Bytes <= 0 || manifest.Bytes > maxBytes || manifest.Chunks != 1+(manifest.Bytes-1)/SnapshotChunkBytes || err != nil || len(digest) != sha256.Size {
		return nil, fmt.Errorf("INVALID_SNAPSHOT_MANIFEST_OR_CAPACITY")
	}
	return &SnapshotReceiver{manifest: manifest}, nil
}

func (r *SnapshotReceiver) Add(chunk SnapshotChunk) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed != nil {
		return r.failed
	}
	if r.finished {
		return fmt.Errorf("SNAPSHOT_ALREADY_FINISHED")
	}
	fail := func() error {
		r.failed = fmt.Errorf("INVALID_SNAPSHOT_CHUNK")
		r.raw = nil
		return r.failed
	}
	if chunk.ID != r.manifest.ID || chunk.Index != r.next || chunk.Offset != len(r.raw) || r.next >= r.manifest.Chunks || len(chunk.Data) != min(SnapshotChunkBytes, r.manifest.Bytes-len(r.raw)) {
		return fail()
	}
	sum := sha256.Sum256(chunk.Data)
	if hex.EncodeToString(sum[:]) != chunk.Digest {
		return fail()
	}
	// Allocate only after a valid first chunk; the configured total budget is
	// checked before allocation and append never grows beyond the manifest.
	if r.raw == nil {
		r.raw = make([]byte, 0, r.manifest.Bytes)
	}
	r.raw = append(r.raw, chunk.Data...)
	r.next++
	return nil
}

func (r *SnapshotReceiver) Finish() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed != nil {
		return nil, r.failed
	}
	if r.finished || r.next != r.manifest.Chunks || len(r.raw) != r.manifest.Bytes {
		return nil, fmt.Errorf("SNAPSHOT_INCOMPLETE_OR_FINISHED")
	}
	sum := sha256.Sum256(r.raw)
	if hex.EncodeToString(sum[:]) != r.manifest.Digest {
		r.failed = fmt.Errorf("SNAPSHOT_DIGEST_MISMATCH")
		r.raw = nil
		return nil, r.failed
	}
	raw := r.raw
	r.raw = nil
	r.finished = true
	return raw, nil
}
