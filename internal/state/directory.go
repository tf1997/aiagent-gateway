package state

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureDirectory persists the directory chain before a writer can acknowledge
// records below it. Syncing only the leaf loses a newly created ancestor after
// a crash. Existing ancestors are synced too, covering a retry after a previous
// initialization failed between mkdir and the parent directory's fsync.
func EnsureDirectory(path string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(path, 0700); err != nil {
		return err
	}
	for {
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		info, statErr := dir.Stat()
		if statErr != nil || !info.IsDir() {
			dir.Close()
			if statErr != nil {
				return statErr
			}
			return fmt.Errorf("state parent is not a directory")
		}
		err = dir.Sync()
		closeErr := dir.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}
