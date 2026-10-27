package handoff

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type snapshotFile interface {
	io.Writer
	Name() string
	Sync() error
	Close() error
}

type snapshotIO struct {
	createTemp func(string, string) (snapshotFile, error)
	link       func(string, string) error
	syncDir    func(string) error
}

func systemSnapshotIO() snapshotIO {
	return snapshotIO{
		createTemp: func(dir, pattern string) (snapshotFile, error) { return os.CreateTemp(dir, pattern) },
		link:       os.Link, syncDir: syncDirectory,
	}
}

// PersistSnapshot publishes the immutable image before SEALED can reference
// it. The filename comes from the verified content digest, never from a peer.
// A failed sync leaves the decision unchanged and must be treated as failure.
func (o *Owner) PersistSnapshot(id string, raw []byte, revision uint64, maxBytes int) (Snapshot, error) {
	return o.persistSnapshot(id, raw, revision, maxBytes, systemSnapshotIO())
}

func (o *Owner) persistSnapshot(id string, raw []byte, revision uint64, maxBytes int, storage snapshotIO) (Snapshot, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var snapshot Snapshot
	if err := o.ready(); err != nil {
		return snapshot, err
	}
	d := o.decision
	if d.Owner != o.identity || d.ID != id || d.Phase != Prepared {
		return snapshot, ErrConflict
	}
	if maxBytes <= 0 || len(raw) == 0 || len(raw) > maxBytes {
		return snapshot, fmt.Errorf("SNAPSHOT_CAPACITY")
	}
	sum := sha256.Sum256(raw)
	snapshot = Snapshot{Digest: hex.EncodeToString(sum[:]), Bytes: int64(len(raw)), Revision: revision}
	dir := filepath.Join(o.dir, "handoff", "snapshots")
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return Snapshot{}, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return Snapshot{}, err
	}
	if !info.IsDir() {
		return Snapshot{}, fmt.Errorf("INVALID_SNAPSHOT_DIRECTORY")
	}
	// Persist creation of the snapshots directory itself.
	if err = storage.syncDir(filepath.Dir(dir)); err != nil {
		return Snapshot{}, err
	}
	file, err := storage.createTemp(dir, ".incoming-*")
	if err != nil {
		return Snapshot{}, err
	}
	defer os.Remove(file.Name())
	written, err := file.Write(raw)
	if err == nil && written != len(raw) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return Snapshot{}, err
	}
	path := filepath.Join(dir, snapshot.Digest+".snapshot")
	// Link publishes without replacing any already referenced immutable image.
	if err = storage.link(file.Name(), path); err != nil {
		if !os.IsExist(err) {
			return Snapshot{}, err
		}
		if _, err = readSnapshotFile(path, snapshot, maxBytes); err != nil {
			return Snapshot{}, err
		}
	}
	if err = storage.syncDir(dir); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// LoadSnapshot only accepts the snapshot in the currently locked decision.
// Candidates validate before Commit; cold recovery must not replay this image
// over newer business journals after a committed owner has started writing.
func (o *Owner) LoadSnapshot(id string, maxBytes int) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return nil, err
	}
	d := o.decision
	if d.ID != id || (d.Phase != Sealed && d.Phase != Committed && d.Phase != Completed) || (o.identity != d.Source && o.identity != d.Target) {
		return nil, ErrConflict
	}
	dir := filepath.Join(o.dir, "handoff", "snapshots")
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("INVALID_SNAPSHOT_DIRECTORY")
	}
	return readSnapshotFile(filepath.Join(dir, d.Snapshot.Digest+".snapshot"), d.Snapshot, maxBytes)
}

func readSnapshotFile(path string, expected Snapshot, maxBytes int) ([]byte, error) {
	if !validDigest(expected.Digest) || maxBytes <= 0 || expected.Bytes <= 0 || expected.Bytes > int64(maxBytes) {
		return nil, fmt.Errorf("INVALID_SNAPSHOT_OR_CAPACITY")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "handoff-snapshot")
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != expected.Bytes {
		return nil, fmt.Errorf("INVALID_SNAPSHOT_FILE")
	}
	raw, err := io.ReadAll(io.LimitReader(f, expected.Bytes+1))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	if int64(len(raw)) != expected.Bytes || hex.EncodeToString(sum[:]) != expected.Digest {
		return nil, fmt.Errorf("SNAPSHOT_DIGEST_MISMATCH")
	}
	return raw, nil
}

// MatchSnapshot rechecks the durable image byte for byte against an already
// validated in-memory image, without allocating a second full snapshot.
func (o *Owner) MatchSnapshot(id string, maxBytes int, raw []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.ID != id || (d.Phase != Sealed && d.Phase != Committed && d.Phase != Completed) || (o.identity != d.Source && o.identity != d.Target) {
		return ErrConflict
	}
	if maxBytes <= 0 || len(raw) == 0 || len(raw) > maxBytes || int64(len(raw)) != d.Snapshot.Bytes {
		return ErrConflict
	}
	dir := filepath.Join(o.dir, "handoff", "snapshots")
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("INVALID_SNAPSHOT_DIRECTORY")
	}
	fd, err := syscall.Open(filepath.Join(dir, d.Snapshot.Digest+".snapshot"), syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "match-snapshot")
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != d.Snapshot.Bytes {
		return fmt.Errorf("INVALID_SNAPSHOT_FILE")
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	for offset := 0; offset < len(raw); {
		size := min(len(buffer), len(raw)-offset)
		if _, err = io.ReadFull(f, buffer[:size]); err != nil {
			return err
		}
		if !bytes.Equal(buffer[:size], raw[offset:offset+size]) {
			return ErrConflict
		}
		hash.Write(buffer[:size])
		offset += size
	}
	var tail [1]byte
	if n, err := f.Read(tail[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("SNAPSHOT_SIZE_CHANGED")
	}
	if hex.EncodeToString(hash.Sum(nil)) != d.Snapshot.Digest {
		return fmt.Errorf("SNAPSHOT_DIGEST_MISMATCH")
	}
	return nil
}

// VerifySnapshot checks a retained recovery image without allocating its full
// contents. Callers performing maintenance must hold the directory's owner lock.
func VerifySnapshot(dir string, expected Snapshot) error {
	if !validDigest(expected.Digest) || expected.Bytes <= 0 || expected.Bytes == int64(^uint64(0)>>1) {
		return fmt.Errorf("INVALID_SNAPSHOT_OR_CAPACITY")
	}
	path := filepath.Join(dir, "handoff", "snapshots")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("INVALID_SNAPSHOT_DIRECTORY")
	}
	fd, err := syscall.Open(filepath.Join(path, expected.Digest+".snapshot"), syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "verify-snapshot")
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != expected.Bytes {
		return fmt.Errorf("INVALID_SNAPSHOT_FILE")
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(f, expected.Bytes+1))
	if err != nil {
		return err
	}
	if n != expected.Bytes || hex.EncodeToString(hash.Sum(nil)) != expected.Digest {
		return fmt.Errorf("SNAPSHOT_DIGEST_MISMATCH")
	}
	return nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
