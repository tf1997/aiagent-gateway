package state

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type directoryProbe struct {
	directoryFile
	statErr, syncErr, closeErr error
	synced, closed             bool
}

func (f *directoryProbe) Stat() (os.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return f.directoryFile.Stat()
}

func (f *directoryProbe) Sync() error {
	f.synced = true
	return errors.Join(f.directoryFile.Sync(), f.syncErr)
}

func (f *directoryProbe) Close() error {
	f.closed = true
	return errors.Join(f.directoryFile.Close(), f.closeErr)
}

func TestEnsureDirectoryFailureAndRetry(t *testing.T) {
	fault := errors.New("injected directory failure")
	for _, stage := range []string{"mkdir-before", "mkdir-after", "open", "stat", "sync", "close", "sync-and-close"} {
		levels := []string{"leaf", "parent", "root"}
		if stage == "mkdir-before" || stage == "mkdir-after" {
			levels = levels[:1]
		}
		for _, level := range levels {
			t.Run(stage+"/"+level, func(t *testing.T) {
				base := t.TempDir()
				path := filepath.Join(base, "new", "parent", "state")
				ancestors := []string{path}
				for parent := filepath.Dir(path); parent != ancestors[len(ancestors)-1]; parent = filepath.Dir(parent) {
					ancestors = append(ancestors, parent)
				}
				at := path
				if level == "parent" {
					at = filepath.Dir(path)
				} else if level == "root" {
					at = ancestors[len(ancestors)-1]
				}
				fs := systemDirectoryIO()
				mkdirAll := fs.mkdirAll
				fs.mkdirAll = func(p string, mode os.FileMode) error {
					if stage == "mkdir-before" {
						return fault
					}
					if err := mkdirAll(p, mode); err != nil {
						return err
					}
					if stage == "mkdir-after" {
						return fault
					}
					return nil
				}
				var opened []*directoryProbe
				fs.open = func(p string) (directoryFile, error) {
					if p == at && stage == "open" {
						return nil, fault
					}
					f, err := os.Open(p)
					if err != nil {
						return nil, err
					}
					probe := &directoryProbe{directoryFile: f}
					if p == at {
						switch stage {
						case "stat":
							probe.statErr = fault
						case "sync":
							probe.syncErr = fault
						case "close":
							probe.closeErr = fault
						case "sync-and-close":
							probe.syncErr, probe.closeErr = fault, os.ErrInvalid
						}
					}
					opened = append(opened, probe)
					return probe, nil
				}
				err := ensureDirectory(path, fs)
				if !errors.Is(err, fault) || (stage == "sync-and-close" && !errors.Is(err, os.ErrInvalid)) {
					t.Fatal("initialization hid a storage failure", err)
				}
				for _, f := range opened {
					if !f.closed {
						t.Fatal("failed initialization leaked a directory handle")
					}
				}
				// The retry must sync even existing directories: the failed
				// attempt may have created them without persisting their names.
				fs = systemDirectoryIO()
				var retried []string
				opened = nil
				fs.open = func(p string) (directoryFile, error) {
					f, err := os.Open(p)
					if err != nil {
						return nil, err
					}
					retried = append(retried, p)
					probe := &directoryProbe{directoryFile: f}
					opened = append(opened, probe)
					return probe, nil
				}
				if err := ensureDirectory(path, fs); err != nil || !reflect.DeepEqual(retried, ancestors) {
					t.Fatal("retry skipped an ancestor", retried, err)
				}
				for _, f := range opened {
					if !f.synced || !f.closed {
						t.Fatal("retry did not sync and close the complete directory chain")
					}
				}
				j, err := Open(path)
				if err != nil {
					t.Fatal("retry did not allow a writer to open", err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestEnsureDirectoryRejectsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirectory(filepath.Join(path, "state")); err == nil {
		t.Fatal("initialized state under a regular file")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "preserved" {
		t.Fatal("initialization changed the existing file", err)
	}
}
