// Package state provides a single-writer, durable metadata journal.
package state

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type frame struct {
	Version  int                        `json:"version"`
	Sequence uint64                     `json:"sequence"`
	Values   map[string]json.RawMessage `json:"values"`
	Delete   []string                   `json:"delete,omitempty"`
}
type envelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

// Journal is exclusively locked for its lifetime. A failed commit poisons the
// handle: callers must fail closed and reopen rather than guess what committed.
type Journal struct {
	mu        sync.Mutex
	f         journalFile
	lock      *os.File
	path      string
	sequence  uint64
	values    map[string]json.RawMessage
	failed    error
	commits   int
	admission func() (func(), error)
	compactIO compactionIO
}

// journalFile keeps failures local to one writer, including uncertain writes
// whose bytes reached storage before the operation reported an error.
type journalFile interface {
	io.Reader
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
	Close() error
}

const maxFrame = 256 << 20

func Open(dir string) (*Journal, error) {
	if err := EnsureDirectory(dir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "state.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("state already locked: %w", err)
	}
	j := &Journal{lock: lock, path: filepath.Join(dir, "state.log"), values: map[string]json.RawMessage{}, compactIO: systemCompactionIO()}
	fail := func(e error) (*Journal, error) { j.Close(); return nil, e }
	if info, e := os.Lstat(j.path); e == nil && !info.Mode().IsRegular() {
		return fail(fmt.Errorf("state journal must be a regular file"))
	}
	j.f, err = os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fail(err)
	}
	values, sequence, offset, partial, err := readSnapshot(j.f)
	if err != nil {
		return fail(err)
	}
	j.values, j.sequence = values, sequence
	if partial {
		if err = j.f.Truncate(offset); err != nil {
			return fail(err)
		}
		if err = j.f.Sync(); err != nil {
			return fail(err)
		}
	}
	if _, err = j.f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fail(err)
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return fail(err)
	}
	return j, nil
}

// ReadSnapshot reads a committed journal prefix without taking its writer lock
// or repairing an incomplete tail. A launcher must recheck its decision after
// obtaining the owner lock: a concurrent writer may have committed since this
// read. An already-open file remains valid across atomic journal compaction.
func ReadSnapshot(dir string) (map[string]json.RawMessage, error) {
	path := filepath.Join(dir, "state.log")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("state journal must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return nil, err
	}
	values, _, _, _, err := readSnapshot(io.NewSectionReader(f, 0, info.Size()))
	return values, err
}

func readSnapshot(reader io.Reader) (map[string]json.RawMessage, uint64, int64, bool, error) {
	r := bufio.NewReader(reader)
	values := map[string]json.RawMessage{}
	var sequence uint64
	var offset int64
	for {
		raw, e := readLine(r)
		if e == io.EOF {
			return values, sequence, offset, len(raw) > 0, nil
		}
		if e != nil {
			return nil, 0, offset, false, e
		}
		var env envelope
		var f frame
		if json.Unmarshal(raw, &env) != nil {
			return nil, 0, offset, false, fmt.Errorf("invalid journal record at %d", offset)
		}
		sum := sha256.Sum256(env.Payload)
		if hex.EncodeToString(sum[:]) != env.SHA256 || json.Unmarshal(env.Payload, &f) != nil || f.Version != 1 || f.Sequence != sequence+1 {
			return nil, 0, offset, false, fmt.Errorf("corrupt or unsupported journal record at %d", offset)
		}
		for k, v := range f.Values {
			values[k] = bytes.Clone(v)
		}
		for _, k := range f.Delete {
			delete(values, k)
		}
		sequence = f.Sequence
		offset += int64(len(raw))
	}
}
func readLine(r *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		b, e := r.ReadSlice('\n')
		if len(out)+len(b) > maxFrame {
			return nil, fmt.Errorf("journal record too large")
		}
		out = append(out, b...)
		if e != bufio.ErrBufferFull {
			return out, e
		}
	}
}
func (j *Journal) Snapshot() map[string]json.RawMessage {
	j.mu.Lock()
	defer j.mu.Unlock()
	m := map[string]json.RawMessage{}
	for k, v := range j.values {
		m[k] = bytes.Clone(v)
	}
	return m
}
func (j *Journal) Healthy() error { j.mu.Lock(); defer j.mu.Unlock(); return j.failed }

// SetAdmission connects domain commits to their process owner's write barrier.
// It is configured before the domain controller begins serving requests.
func (j *Journal) SetAdmission(admit func() (func(), error)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.admission = admit
}

// Replace commits only changes in prefix. It never overwrites another domain.
func (j *Journal) Replace(prefix string, values map[string]json.RawMessage) error {
	j.mu.Lock()
	admit := j.admission
	j.mu.Unlock()
	if admit != nil {
		leave, err := admit()
		if err != nil {
			return err
		}
		defer leave()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	f := frame{Version: 1, Sequence: j.sequence + 1, Values: map[string]json.RawMessage{}}
	for k, v := range values {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix || !json.Valid(v) {
			return fmt.Errorf("invalid journal key/value")
		}
		if !bytes.Equal(j.values[k], v) {
			f.Values[k] = v
		}
	}
	for k := range j.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			if _, ok := values[k]; !ok {
				f.Delete = append(f.Delete, k)
			}
		}
	}
	if len(f.Values) == 0 && len(f.Delete) == 0 {
		return nil
	}
	raw, _ := json.Marshal(f)
	sum := sha256.Sum256(raw)
	encoded, _ := json.Marshal(envelope{raw, hex.EncodeToString(sum[:])})
	encoded = append(encoded, '\n')
	if len(encoded) > maxFrame {
		return fmt.Errorf("journal transaction too large")
	}
	written, err := j.f.Write(encoded)
	if err == nil && written != len(encoded) {
		err = io.ErrShortWrite
	}
	if err != nil {
		j.failed = err
		return err
	}
	if err := j.f.Sync(); err != nil {
		j.failed = err
		return err
	}
	for k, v := range f.Values {
		j.values[k] = bytes.Clone(v)
	}
	for _, k := range f.Delete {
		delete(j.values, k)
	}
	j.sequence = f.Sequence
	j.commits++
	if j.commits >= 1024 {
		if err := j.compact(); err != nil {
			j.failed = err
			return err
		}
	}
	return nil
}
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.failed = fmt.Errorf("journal closed")
	var err error
	if j.f != nil {
		err = j.f.Close()
		j.f = nil
	}
	if j.lock != nil {
		err = errors.Join(err, syscall.Flock(int(j.lock.Fd()), syscall.LOCK_UN), j.lock.Close())
		j.lock = nil
	}
	return err
}
