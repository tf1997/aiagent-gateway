package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func storeFixture() Config {
	rt := Route{ID: "rt_existing", Key: "test", Expose: []string{"proxy"}, Enabled: true, Version: 3,
		Endpoints: []Endpoint{{ID: "ep_existing", Name: "one", Weight: 100, State: "active", Version: 2,
			Interfaces: map[string]EndpointInterface{"proxy": {URL: "http://localhost:1234", Token: "upstream-secret"}}}}}
	Defaults(&rt)
	return Config{Schema: 2, Revision: 7, Routes: []Route{rt},
		Receipts:     map[string]Receipt{"request-key": {Hash: "request-hash", Body: json.RawMessage(`{"id":"rt_existing"}`), ETag: Tag(3)}},
		ReceiptOrder: []string{"request-key"},
		Audit:        []Audit{{At: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), Actor: "admin", Action: "POST", Path: "/admin/v1/routes", Revision: 7}}}
}

func readFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sameConfig(a, b Config) bool {
	left, err := json.Marshal(a)
	right, otherErr := json.Marshal(b)
	return err == nil && otherErr == nil && bytes.Equal(left, right)
}

func TestSplitStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := storeFixture()
	if committed, err := Save(dir, c); err != nil || !committed {
		t.Fatal(committed, err)
	}
	for _, part := range []struct {
		name string
		keys []string
	}{
		{metadataFile, []string{"storage_version", "schema_version", "config_revision"}},
		{routesFile, []string{"storage_version", "schema_version", "config_revision", "routes"}},
		{logsFile, []string{"storage_version", "schema_version", "config_revision", "idempotency", "idempotency_order", "audit"}},
	} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(readFile(t, dir, part.name), &fields); err != nil {
			t.Fatal(err)
		}
		if len(fields) != len(part.keys) {
			t.Fatalf("unexpected fields in %s: %v", part.name, fields)
		}
		for _, key := range part.keys {
			if _, ok := fields[key]; !ok {
				t.Fatalf("missing %s in %s", key, part.name)
			}
		}
		info, err := os.Stat(filepath.Join(dir, part.name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("store permissions", part.name, err)
		}
	}
	got, legacy, err := LoadStore(dir)
	if err != nil || legacy != nil || !sameConfig(got, c) {
		t.Fatal("split store did not preserve configuration", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, pendingFile)); !os.IsNotExist(err) {
		t.Fatal("completed transaction retained redo record", err)
	}
}

func TestSplitStoreRecoversInterruptedWrites(t *testing.T) {
	for _, blocked := range []string{routesFile, logsFile, metadataFile} {
		t.Run(blocked, func(t *testing.T) {
			dir := t.TempDir()
			c := storeFixture()
			if _, err := Save(dir, c); err != nil {
				t.Fatal(err)
			}
			// A directory at one destination causes a real rename failure at
			// each stage, after the redo record has committed.
			path := filepath.Join(dir, blocked)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			c.Revision++
			c.Routes[0].Version++
			c.Routes[0].Enabled = false
			c.Audit = append(c.Audit, Audit{Action: "PATCH", Revision: c.Revision})
			if committed, err := Save(dir, c); err == nil || !committed {
				t.Fatal("expected failure after commit", committed, err)
			}
			pending := readFile(t, dir, pendingFile)
			if _, _, err := LoadStore(dir); err == nil {
				t.Fatal("recovery ignored write failure")
			}
			if _, err := Save(dir, storeFixture()); err == nil || !bytes.Equal(pending, readFile(t, dir, pendingFile)) {
				t.Fatal("new write replaced a pending commit", err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			got, legacy, err := LoadStore(dir)
			if err != nil || legacy != nil || !sameConfig(got, c) {
				t.Fatal("recovery lost committed configuration", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, pendingFile)); !os.IsNotExist(err) {
				t.Fatal("recovery retained redo record", err)
			}
			if _, err := Save(dir, c); err != nil {
				t.Fatal("recovered store cannot accept writes", err)
			}
		})
	}
}

func TestSplitStoreRejectsDamagedFiles(t *testing.T) {
	for _, tc := range []struct {
		name, file, replacement string
	}{
		{"missing-metadata", metadataFile, ""},
		{"missing-routes", routesFile, ""},
		{"missing-logs", logsFile, ""},
		{"corrupt-metadata", metadataFile, "broken"},
		{"corrupt-routes", routesFile, "broken"},
		{"corrupt-logs", logsFile, "broken"},
		{"corrupt-pending", pendingFile, "broken"},
		{"missing-storage-version", metadataFile, `{"schema_version":2,"config_revision":7}`},
		{"legacy-metadata-with-split-parts", metadataFile, `{"schema_version":2,"config_revision":7,"routes":[]}`},
		{"unsupported-storage", metadataFile, `{"storage_version":2,"schema_version":2,"config_revision":7}`},
		{"mismatched-revision", routesFile, `{"storage_version":1,"schema_version":2,"config_revision":8,"routes":[]}`},
		{"mismatched-schema", logsFile, `{"storage_version":1,"schema_version":1,"config_revision":7,"idempotency":{}}`},
		{"missing-route-list", routesFile, `{"storage_version":1,"schema_version":2,"config_revision":7}`},
		{"missing-receipts", logsFile, `{"storage_version":1,"schema_version":2,"config_revision":7}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := Save(dir, storeFixture()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, tc.file)
			var err error
			if tc.replacement == "" {
				err = os.Remove(path)
			} else {
				err = os.WriteFile(path, []byte(tc.replacement), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := map[string][]byte{}
			for _, name := range []string{metadataFile, routesFile, logsFile, pendingFile} {
				before[name], _ = os.ReadFile(filepath.Join(dir, name))
			}
			if _, _, err := LoadStore(dir); err == nil {
				t.Fatal("accepted damaged store")
			}
			for name, want := range before {
				got, err := os.ReadFile(filepath.Join(dir, name))
				if !bytes.Equal(got, want) || (want == nil && !os.IsNotExist(err)) {
					t.Fatal("damaged store was overwritten", name, err)
				}
			}
		})
	}
}

func TestSplitStoreRecoversInitialization(t *testing.T) {
	dir := t.TempDir()
	c := storeFixture()
	if _, err := writeStoreFile(dir, pendingFile, pendingStore{Storage: 1, Config: c}); err != nil {
		t.Fatal(err)
	}
	got, _, err := LoadStore(dir)
	if err != nil || !sameConfig(got, c) {
		t.Fatal("pending first commit was not recovered", err)
	}
}

func TestSplitStoreMarshalFailureKeepsCommittedFiles(t *testing.T) {
	dir := t.TempDir()
	c := storeFixture()
	if _, err := Save(dir, c); err != nil {
		t.Fatal(err)
	}
	invalid := Clone(c)
	invalid.Receipts["broken"] = Receipt{Body: json.RawMessage(`{`)}
	if committed, err := Save(dir, invalid); err == nil || committed {
		t.Fatal("invalid JSON was committed", committed, err)
	}
	got, _, err := LoadStore(dir)
	if err != nil || !sameConfig(got, c) {
		t.Fatal("failed write changed configuration", err)
	}
}
