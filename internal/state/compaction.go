package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type compactionFile interface {
	journalFile
	Name() string
}

// I/O dependencies belong to one journal, so fault injection cannot affect a
// different owner or a concurrent domain writer.
type compactionIO struct {
	createTemp func(string, string) (compactionFile, error)
	rename     func(string, string) error
	syncDir    func(string) error
}

func systemCompactionIO() compactionIO {
	return compactionIO{
		createTemp: func(dir, pattern string) (compactionFile, error) { return os.CreateTemp(dir, pattern) },
		rename:     os.Rename,
		syncDir: func(path string) error {
			d, err := os.Open(path)
			if err != nil {
				return err
			}
			return errors.Join(d.Sync(), d.Close())
		},
	}
}

// The appended transaction is already durable. Either the old log or the
// compacted image must retain it through every failure below. Keep the exact
// synced file open across rename; reopening the path is unnecessary.
func (j *Journal) compact() (err error) {
	raw, _ := json.Marshal(frame{Version: 1, Sequence: 1, Values: j.values})
	sum := sha256.Sum256(raw)
	encoded, _ := json.Marshal(envelope{raw, hex.EncodeToString(sum[:])})
	encoded = append(encoded, '\n')
	if len(encoded) > maxFrame {
		return fmt.Errorf("state snapshot exceeds capacity")
	}
	dir := filepath.Dir(j.path)
	f, err := j.compactIO.createTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			err = errors.Join(err, f.Close())
		}
		_ = os.Remove(f.Name())
	}()
	written, err := f.Write(encoded)
	if err != nil {
		return err
	}
	if written != len(encoded) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = j.compactIO.rename(f.Name(), j.path); err != nil {
		return err
	}
	if err = j.compactIO.syncDir(dir); err != nil {
		return err
	}
	old := j.f
	j.f, j.sequence, j.commits = f, 1, 0
	installed = true
	// Propagate delayed errors from the old inode as well. Replace poisons
	// this journal on any failure; Close will then close the installed file.
	return old.Close()
}
