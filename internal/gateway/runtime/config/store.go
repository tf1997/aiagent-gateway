package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func Save(dataDir string, c Config) (renamed bool, err error) {
	b, err := json.MarshalIndent(c, "", "  ")
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
	if err = os.Rename(f.Name(), filepath.Join(dataDir, "config.json")); err != nil {
		return false, err
	}
	d, err := os.Open(dataDir)
	if err != nil {
		return true, err
	}
	defer d.Close()
	return true, d.Sync()
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
	path := filepath.Join(dataDir, "config.schema-1.backup.json")
	f, err := os.CreateTemp(dataDir, ".schema-1-*.tmp")
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
	d, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
