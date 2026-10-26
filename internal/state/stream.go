package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sync"
	"sync/atomic"
)

const OperationFormat = "gateway-state-operations-1"

// Cursor binds an ordered stream to one baseline and owner. Digest is the
// operation hash chain, not a claim about a serialized full snapshot.
type Cursor struct {
	Epoch    string `json:"epoch"`
	Handoff  string `json:"handoff_id"`
	Revision uint64 `json:"logical_revision"`
	Digest   string `json:"digest"`
}

type Change struct {
	Revision uint64                     `json:"revision"`
	Put      map[string]json.RawMessage `json:"put,omitempty"`
	Delete   []string                   `json:"delete,omitempty"`
}

type OperationBatch struct {
	Format string            `json:"format"`
	From   Cursor            `json:"from"`
	To     Cursor            `json:"to"`
	Events []json.RawMessage `json:"events"`
}

// Recorder is a bounded, temporary replication log. It never runs business
// operations and never fails an already committed business write. Overflow
// poisons this preparation, which must be aborted before ownership can move.
// Writers publish while holding their domain lock, before reopening admission.
type Recorder struct {
	mu       sync.Mutex
	active   atomic.Bool
	base, at Cursor
	records  map[string]json.RawMessage
	events   []json.RawMessage
	cursors  []Cursor
	bytes    int
	limit    int
	err      error
}

func (r *Recorder) Active() bool { return r != nil && r.active.Load() }

func (r *Recorder) Start(at Cursor, records map[string]json.RawMessage, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validCursor(at) || limit <= 0 || records == nil {
		return fmt.Errorf("INVALID_OPERATION_BASELINE")
	}
	for key, raw := range records {
		if key == "" || !json.Valid(raw) {
			return fmt.Errorf("INVALID_OPERATION_RECORD")
		}
	}
	r.records = cloneRecords(records)
	r.base, r.at, r.limit, r.bytes, r.err = at, at, limit, 0, nil
	r.events, r.cursors = nil, nil
	r.active.Store(true)
	return nil
}

func (r *Recorder) Stop() Cursor {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active.Store(false)
	r.records, r.events, r.cursors = nil, nil, nil
	return r.at
}

func (r *Recorder) Cursor() Cursor { r.mu.Lock(); defer r.mu.Unlock(); return r.at }
func (r *Recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return nil
	}
	return r.err
}

func (r *Recorder) Put(key string, value any) {
	if !r.Active() {
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		r.fail(err)
		return
	}
	r.Patch(map[string]json.RawMessage{key: raw}, nil)
}

func (r *Recorder) Delete(keys ...string) {
	if r.Active() {
		r.Patch(nil, keys)
	}
}

func (r *Recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active.Load() && r.err == nil {
		r.err = err
	}
}

func (r *Recorder) Patch(put map[string]json.RawMessage, remove []string) {
	if !r.Active() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() || r.err != nil {
		return
	}
	change := Change{Revision: r.at.Revision + 1, Put: map[string]json.RawMessage{}}
	for key, raw := range put {
		if key == "" || !json.Valid(raw) {
			r.err = fmt.Errorf("INVALID_OPERATION_RECORD")
			return
		}
		if !bytes.Equal(raw, r.records[key]) {
			change.Put[key] = raw
		}
	}
	seen := map[string]bool{}
	for _, key := range remove {
		if _, conflict := put[key]; key == "" || conflict {
			r.err = fmt.Errorf("INVALID_OPERATION_DELETION")
			return
		}
		if _, exists := r.records[key]; exists && !seen[key] {
			change.Delete = append(change.Delete, key)
			seen[key] = true
		}
	}
	if len(change.Put) == 0 && len(change.Delete) == 0 {
		return
	}
	r.append(change)
}

// Advance also covers admitted operations which changed no record. Sequence
// numbers never depend on journal compaction or wall-clock timestamps.
func (r *Recorder) Advance() {
	if !r.Active() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active.Load() && r.err == nil {
		r.append(Change{Revision: r.at.Revision + 1})
	}
}

func (r *Recorder) append(change Change) {
	raw, err := json.Marshal(change)
	// Reserve room for batch identity/framing in addition to event payloads.
	if err != nil || len(raw)+1 > r.limit-1024-r.bytes || change.Revision == 0 {
		r.err = fmt.Errorf("OPERATION_DELTA_CAPACITY")
		return
	}
	for key, value := range change.Put {
		r.records[key] = bytes.Clone(value)
	}
	for _, key := range change.Delete {
		delete(r.records, key)
	}
	r.at = advanceCursor(r.at, raw)
	r.events = append(r.events, raw)
	r.cursors = append(r.cursors, r.at)
	r.bytes += len(raw) + 1
}

func (r *Recorder) Batch(from Cursor) ([]byte, Cursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, Cursor{}, r.err
	}
	if !r.active.Load() || from != r.base {
		return nil, Cursor{}, fmt.Errorf("OPERATION_BASE_MISMATCH")
	}
	raw, err := json.Marshal(OperationBatch{OperationFormat, from, r.at, r.events})
	if len(raw) > r.limit {
		return nil, Cursor{}, fmt.Errorf("OPERATION_DELTA_CAPACITY")
	}
	return raw, r.at, err
}

// Acknowledge releases only the exact prefix confirmed by the candidate.
func (r *Recorder) Acknowledge(at Cursor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() || r.err != nil {
		return fmt.Errorf("OPERATION_STREAM_UNAVAILABLE")
	}
	if at == r.base {
		return nil
	}
	for i, cursor := range r.cursors {
		if cursor != at {
			continue
		}
		for _, raw := range r.events[:i+1] {
			r.bytes -= len(raw) + 1
		}
		r.events = append([]json.RawMessage(nil), r.events[i+1:]...)
		r.cursors = append([]Cursor(nil), r.cursors[i+1:]...)
		r.base = at
		return nil
	}
	return fmt.Errorf("OPERATION_ACK_MISMATCH")
}

func (r *Recorder) Records() (map[string]json.RawMessage, Cursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return nil, Cursor{}, fmt.Errorf("OPERATION_STREAM_INACTIVE")
	}
	if r.err != nil {
		return nil, Cursor{}, r.err
	}
	return cloneRecords(r.records), r.at, nil
}

func validCursor(c Cursor) bool {
	raw, err := hex.DecodeString(c.Digest)
	return c.Epoch != "" && c.Handoff != "" && err == nil && len(raw) == sha256.Size
}

func advanceCursor(at Cursor, raw []byte) Cursor {
	hash := sha256.New()
	hash.Write([]byte(at.Digest))
	hash.Write(raw)
	at.Digest = hex.EncodeToString(hash.Sum(nil))
	at.Revision++
	return at
}

func cloneRecords(records map[string]json.RawMessage) map[string]json.RawMessage {
	copy := maps.Clone(records)
	for key, raw := range copy {
		copy[key] = bytes.Clone(raw)
	}
	return copy
}

// ApplyOperations validates an entire batch before modifying the isolated
// mirror. No backend, timers, expiry processing, or upstream calls are involved.
func ApplyOperations(records map[string]json.RawMessage, at Cursor, raw []byte, maxBatch, maxImage int) (Cursor, error) {
	if len(raw) > maxBatch || maxBatch <= 0 || maxImage <= 0 || !validCursor(at) || records == nil {
		return at, fmt.Errorf("OPERATION_DELTA_CAPACITY")
	}
	var batch OperationBatch
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		return at, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return at, fmt.Errorf("INVALID_OPERATION_BATCH")
	}
	if batch.Format != OperationFormat || batch.From != at || !validCursor(batch.To) || batch.To.Epoch != at.Epoch || batch.To.Handoff != at.Handoff {
		return at, fmt.Errorf("OPERATION_BASE_MISMATCH")
	}
	staged := map[string]json.RawMessage{}
	removed := map[string]bool{}
	end := at
	for _, raw := range batch.Events {
		var change Change
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&change); err != nil {
			return at, err
		}
		if decoder.Decode(new(any)) != io.EOF {
			return at, fmt.Errorf("INVALID_OPERATION_CHANGE")
		}
		if change.Revision == 0 || change.Revision != end.Revision+1 {
			return at, fmt.Errorf("OPERATION_REVISION_GAP")
		}
		for key, value := range change.Put {
			if key == "" || !json.Valid(value) {
				return at, fmt.Errorf("INVALID_OPERATION_RECORD")
			}
			staged[key], removed[key] = value, false
		}
		for _, key := range change.Delete {
			if _, conflict := change.Put[key]; key == "" || conflict {
				return at, fmt.Errorf("INVALID_OPERATION_DELETION")
			}
			delete(staged, key)
			removed[key] = true
		}
		end = advanceCursor(end, raw)
	}
	if end != batch.To {
		return at, fmt.Errorf("OPERATION_DIGEST_MISMATCH")
	}
	size := 0
	for key, value := range records {
		if _, replaced := staged[key]; !replaced && !removed[key] {
			size += len(key) + len(value)
		}
	}
	for key, value := range staged {
		size += len(key) + len(value)
	}
	if size > maxImage {
		return at, fmt.Errorf("OPERATION_IMAGE_CAPACITY")
	}
	for key := range removed {
		if removed[key] {
			delete(records, key)
		}
	}
	for key, value := range staged {
		records[key] = bytes.Clone(value)
	}
	return end, nil
}
