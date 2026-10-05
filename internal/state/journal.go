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
	mu       sync.Mutex
	f, lock  *os.File
	path     string
	sequence uint64
	values   map[string]json.RawMessage
	failed   error
	commits  int
}

const maxFrame = 256 << 20

func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
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
	j := &Journal{lock: lock, path: filepath.Join(dir, "state.log"), values: map[string]json.RawMessage{}}
	fail := func(e error) (*Journal, error) { j.Close(); return nil, e }
	if info, e := os.Lstat(j.path); e == nil && !info.Mode().IsRegular() {
		return fail(fmt.Errorf("state journal must be a regular file"))
	}
	j.f, err = os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fail(err)
	}
	r := bufio.NewReader(j.f)
	var offset int64
	for {
		raw, e := readLine(r)
		if e == io.EOF {
			if len(raw) > 0 {
				if e = j.f.Truncate(offset); e != nil {
					return fail(e)
				}
				if e = j.f.Sync(); e != nil {
					return fail(e)
				}
			}
			break
		}
		if e != nil {
			return fail(e)
		}
		var env envelope
		var f frame
		if json.Unmarshal(raw, &env) != nil {
			return fail(fmt.Errorf("invalid journal record at %d", offset))
		}
		sum := sha256.Sum256(env.Payload)
		if hex.EncodeToString(sum[:]) != env.SHA256 || json.Unmarshal(env.Payload, &f) != nil || f.Version != 1 || f.Sequence != j.sequence+1 {
			return fail(fmt.Errorf("corrupt or unsupported journal record at %d", offset))
		}
		for k, v := range f.Values {
			j.values[k] = bytes.Clone(v)
		}
		for _, k := range f.Delete {
			delete(j.values, k)
		}
		j.sequence = f.Sequence
		offset += int64(len(raw))
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

// Replace commits only changes in prefix. It never overwrites another domain.
func (j *Journal) Replace(prefix string, values map[string]json.RawMessage) error {
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
	if _, err := j.f.Write(encoded); err != nil {
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
func (j *Journal) compact() error {
	raw, _ := json.Marshal(frame{Version: 1, Sequence: 1, Values: j.values})
	sum := sha256.Sum256(raw)
	encoded, _ := json.Marshal(envelope{raw, hex.EncodeToString(sum[:])})
	encoded = append(encoded, '\n')
	if len(encoded) > maxFrame {
		return fmt.Errorf("state snapshot exceeds capacity")
	}
	f, err := os.CreateTemp(filepath.Dir(j.path), ".snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(encoded); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), j.path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(j.path))
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return err
	}
	next, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	j.f.Close()
	j.f = next
	j.sequence = 1
	j.commits = 0
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
