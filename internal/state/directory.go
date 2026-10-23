package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type directoryFile interface {
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

type directoryIO struct {
	mkdirAll func(string, os.FileMode) error
	open     func(string) (directoryFile, error)
}

func systemDirectoryIO() directoryIO {
	return directoryIO{os.MkdirAll, func(path string) (directoryFile, error) { return os.Open(path) }}
}

// EnsureDirectory persists the directory chain before a writer can acknowledge
// records below it. Syncing only the leaf loses a newly created ancestor after
// a crash. Existing ancestors are synced too, covering a retry after a previous
// initialization failed between mkdir and the parent directory's fsync.
func EnsureDirectory(path string) error {
	return ensureDirectory(path, systemDirectoryIO())
}

// Dependencies are scoped to this initialization, never shared by writers.
func ensureDirectory(path string, fs directoryIO) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err = fs.mkdirAll(path, 0700); err != nil {
		return err
	}
	for {
		dir, err := fs.open(path)
		if err != nil {
			return err
		}
		info, statErr := dir.Stat()
		if statErr != nil || !info.IsDir() {
			if statErr != nil {
				return errors.Join(statErr, dir.Close())
			}
			return errors.Join(fmt.Errorf("state parent is not a directory"), dir.Close())
		}
		if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
			return err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}
