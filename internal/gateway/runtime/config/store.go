package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	metadataFile = "config.json"
	routesFile   = "routes.json"
	logsFile     = "route-logs.json"
	pendingFile  = ".config.pending.json"
)

// The storage version is separate from the route schema and shared wire format.
// Every part carries the same revision, so incomplete restores fail closed.
type storeHeader struct {
	Storage  int    `json:"storage_version"`
	Schema   int    `json:"schema_version"`
	Revision uint64 `json:"config_revision"`
}

type routeStore struct {
	storeHeader
	Routes []Route `json:"routes"`
}

type logStore struct {
	storeHeader
	Receipts     map[string]Receipt `json:"idempotency"`
	ReceiptOrder []string           `json:"idempotency_order"`
	Audit        []Audit            `json:"audit"`
}

type pendingStore struct {
	Storage int    `json:"storage_version"`
	Config  Config `json:"config"`
}

// LoadStore requires the caller's exclusive data-directory lock. Old single-file
// stores stay untouched until a successful management write; raw is the original
// content to back up before that write. Pending commits are recovered first.
func LoadStore(dataDir string) (c Config, raw []byte, err error) {
	if err = recoverStore(dataDir); err != nil {
		return c, nil, err
	}
	raw, err = readStoreFile(dataDir, metadataFile)
	if errors.Is(err, os.ErrNotExist) {
		// A missing manifest with surviving parts is data loss, not a new store.
		for _, name := range []string{routesFile, logsFile} {
			if _, e := os.Lstat(filepath.Join(dataDir, name)); !errors.Is(e, os.ErrNotExist) {
				if e != nil {
					return c, nil, e
				}
				return c, nil, fmt.Errorf("missing %s with existing %s", metadataFile, name)
			}
		}
		c = Config{Schema: 2, Revision: 1, Routes: []Route{}, Receipts: map[string]Receipt{}}
		_, err = Save(dataDir, c)
		return c, nil, err
	}
	if err != nil {
		return c, nil, err
	}
	var header storeHeader
	if err = json.Unmarshal(raw, &header); err != nil {
		return c, nil, err
	}
	if header.Storage == 0 {
		// A legacy manifest alongside split parts can be an incomplete restore
		// or damaged metadata. Do not load it and discard the surviving routes.
		for _, name := range []string{routesFile, logsFile} {
			if _, e := os.Lstat(filepath.Join(dataDir, name)); !errors.Is(e, os.ErrNotExist) {
				if e != nil {
					return c, nil, e
				}
				return c, nil, fmt.Errorf("legacy %s with existing %s", metadataFile, name)
			}
		}
		c, _, err = Load(raw)
		if err == nil {
			err = Validate(&c)
		}
		return c, raw, err
	}
	if err = decode(raw, &header); err != nil {
		return c, nil, err
	}
	if header.Storage != 1 || header.Schema != 2 || header.Revision == 0 {
		return c, nil, fmt.Errorf("invalid or unsupported configuration storage version")
	}
	var routes routeStore
	var logs logStore
	for _, part := range []struct {
		name  string
		value any
	}{{routesFile, &routes}, {logsFile, &logs}} {
		b, e := readStoreFile(dataDir, part.name)
		if e == nil {
			e = decode(b, part.value)
		}
		if e != nil {
			return c, nil, fmt.Errorf("read %s: %w", part.name, e)
		}
	}
	if routes.storeHeader != header || logs.storeHeader != header {
		return c, nil, fmt.Errorf("configuration files have mismatched versions")
	}
	if routes.Routes == nil || logs.Receipts == nil {
		return c, nil, fmt.Errorf("configuration files are missing routes or idempotency records")
	}
	c = Config{Schema: header.Schema, Revision: header.Revision, Routes: routes.Routes,
		Receipts: logs.Receipts, ReceiptOrder: logs.ReceiptOrder, Audit: logs.Audit}
	return c, nil, Validate(&c)
}

// Save first commits a redo record, then replaces the three files and removes
// the record. Once committed is true, any failure requires reopening the store:
// recovery must finish the same revision before serving traffic or more writes.
func Save(dataDir string, c Config) (committed bool, err error) {
	if _, err = os.Lstat(filepath.Join(dataDir, pendingFile)); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return false, err
		}
		return true, fmt.Errorf("pending configuration requires recovery")
	}
	if c.Routes == nil {
		c.Routes = []Route{}
	}
	if c.Receipts == nil {
		c.Receipts = map[string]Receipt{}
	}
	committed, err = writeStoreFile(dataDir, pendingFile, pendingStore{Storage: 1, Config: c})
	if err != nil {
		return committed, err
	}
	return true, finishStore(dataDir, c)
}

func recoverStore(dataDir string) error {
	raw, err := readStoreFile(dataDir, pendingFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var pending pendingStore
	if err = decode(raw, &pending); err != nil {
		return fmt.Errorf("invalid pending configuration: %w", err)
	}
	if pending.Storage != 1 || pending.Config.Revision == 0 || pending.Config.Routes == nil || pending.Config.Receipts == nil {
		return fmt.Errorf("invalid pending configuration")
	}
	if err = Validate(&pending.Config); err != nil {
		return fmt.Errorf("invalid pending configuration: %w", err)
	}
	return finishStore(dataDir, pending.Config)
}

func finishStore(dataDir string, c Config) error {
	header := storeHeader{Storage: 1, Schema: c.Schema, Revision: c.Revision}
	for _, part := range []struct {
		name  string
		value any
	}{
		{routesFile, routeStore{storeHeader: header, Routes: c.Routes}},
		{logsFile, logStore{storeHeader: header, Receipts: c.Receipts, ReceiptOrder: c.ReceiptOrder, Audit: c.Audit}},
		{metadataFile, header},
	} {
		if _, err := writeStoreFile(dataDir, part.name, part.value); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(dataDir, pendingFile)); err != nil {
		return err
	}
	return syncDirectory(dataDir)
}

func readStoreFile(dataDir, name string) ([]byte, error) {
	path := filepath.Join(dataDir, name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", name)
	}
	return os.ReadFile(path)
}

func writeStoreFile(dataDir, name string, value any) (renamed bool, err error) {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return false, err
	}
	f, err := os.CreateTemp(dataDir, ".config-*.tmp")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), filepath.Join(dataDir, name)); err != nil {
		return false, err
	}
	return true, syncDirectory(dataDir)
}

func syncDirectory(dataDir string) error {
	d, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func Clone(c Config) Config {
	b, _ := json.Marshal(c)
	var out Config
	_ = json.Unmarshal(b, &out)
	return out
}

// Save the exact pre-migration bytes before replacing config.json. An atomic
// link publishes a fully synced backup without overwriting an earlier backup.
func BackupLegacy(dataDir string, raw []byte) error {
	if raw == nil {
		return nil
	}
	var header struct {
		Schema int `json:"schema_version"`
	}
	if json.Unmarshal(raw, &header) != nil || (header.Schema != 1 && header.Schema != 2) {
		return fmt.Errorf("invalid legacy configuration")
	}
	path := filepath.Join(dataDir, fmt.Sprintf("config.schema-%d.backup.json", header.Schema))
	f, err := os.CreateTemp(dataDir, ".config-backup-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(raw); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(f.Name(), path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("invalid legacy backup")
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil || string(b) != string(raw) {
			return fmt.Errorf("legacy backup differs; preserve or move it before migrating")
		}
	}
	return syncDirectory(dataDir)
}
