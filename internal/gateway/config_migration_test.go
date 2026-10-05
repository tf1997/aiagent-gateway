package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func legacyFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/config-schema-1.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func writeConfig(t *testing.T, dir string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func assertFile(t *testing.T, path string, want []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(b, want) {
		t.Fatalf("file %s changed: %v", path, err)
	}
}
func TestSchema1MigrationAndReceiptReplay(t *testing.T) {
	dir := t.TempDir()
	raw := legacyFixture(t)
	var fixture map[string]any
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	request := `{"key":"test","protocol":"proxy","endpoints":[{"url":"http://localhost:1234/base"}]}`
	sum := sha256.Sum256([]byte("POST /admin/v1/routes\n" + request))
	oldResponse := fixture["routes"].([]any)[0]
	fixture["idempotency"] = map[string]any{"existing-key": map[string]any{"hash": hex.EncodeToString(sum[:]), "body": oldResponse, "etag": `"1"`}}
	fixture["idempotency_order"] = []string{"existing-key"}
	raw, _ = json.MarshalIndent(fixture, "", "  ")
	writeConfig(t, dir, raw)
	g, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if g != nil {
			g.Close()
		}
	}()
	assertFile(t, filepath.Join(dir, "config.json"), raw)
	if _, err := os.Stat(filepath.Join(dir, "config.schema-1.backup.json")); !os.IsNotExist(err) {
		t.Fatal("read-only startup created backup")
	}
	c := g.current.Load().Config
	rt := c.Routes[0]
	if c.Schema != 2 || c.Revision != 9 || rt.ID != "rt_existing" || rt.Version != 5 || rt.AgentType != "generic" || rt.Expose[0] != "proxy" || rt.Endpoints[0].ID != "ep_existing" || rt.Endpoints[0].Version != 3 || rt.Endpoints[0].State != "draining" || rt.Endpoints[0].Interfaces["proxy"].URL != "http://localhost:1234/base" {
		t.Fatalf("migration changed identity/config: %+v", c)
	}
	get := admin(t, g, "GET", "/admin/v1/routes/rt_existing", "", "", "")
	if bytes.Contains(get.Body.Bytes(), []byte(`"protocol":`)) || !bytes.Contains(get.Body.Bytes(), []byte(`"interfaces":`)) {
		t.Fatal(get.Body.String())
	}
	replay := admin(t, g, "POST", "/admin/v1/routes", request, "", "existing-key")
	if replay.Code != 201 || !bytes.Contains(replay.Body.Bytes(), []byte(`"protocol":"proxy"`)) || replay.Header().Get("ETag") != `"1"` || g.current.Load().Config.Revision != 9 {
		t.Fatal("old receipt not preserved", replay.Body.String())
	}
	if w := admin(t, g, "PATCH", "/admin/v1/routes/rt_existing", `{"enabled":false}`, `"99"`, ""); w.Code != 412 {
		t.Fatal(w.Body.String())
	}
	assertFile(t, filepath.Join(dir, "config.json"), raw)
	if w := admin(t, g, "PATCH", "/admin/v1/routes/rt_existing", `{"enabled":false}`, `"5"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	backup := filepath.Join(dir, "config.schema-1.backup.json")
	assertFile(t, backup, raw)
	info, err := os.Stat(backup)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("backup permissions", err)
	}
	persisted, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(persisted, []byte(`"schema_version": 1`)) {
		t.Fatal("schema 2 was not persisted")
	}
	if g.current.Load().Config.Revision != 10 || g.current.Load().Config.Routes[0].Version != 6 {
		t.Fatal("revision was not advanced exactly once")
	}
	g.Close()
	g = nil
	g, err = Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	again := admin(t, g, "POST", "/admin/v1/routes", request, "", "existing-key")
	if again.Body.String() != replay.Body.String() || len(g.current.Load().Config.Routes) != 1 {
		t.Fatal("migration replay duplicated resource")
	}
	assertFile(t, backup, raw)
}

func TestMigrationWriteFailures(t *testing.T) {
	for _, mode := range []string{"backup-conflict", "backup-symlink", "persist"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			raw := legacyFixture(t)
			writeConfig(t, dir, raw)
			g, err := Open(testOptions(dir))
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			before := g.current.Load()
			backup := filepath.Join(dir, "config.schema-1.backup.json")
			switch mode {
			case "backup-conflict":
				if err := os.WriteFile(backup, []byte("older backup"), 0600); err != nil {
					t.Fatal(err)
				}
			case "backup-symlink":
				if err := os.Symlink(filepath.Join(dir, "config.json"), backup); err != nil {
					t.Fatal(err)
				}
			case "persist":
				g.persist = func(Config) (bool, error) { return false, fmt.Errorf("disk full") }
			}
			w := admin(t, g, "PATCH", "/admin/v1/routes/rt_existing", `{"enabled":false}`, `"5"`, "")
			if w.Code != 503 || g.current.Load() != before || !g.healthy.Load() {
				t.Fatal("failed migration changed snapshot", w.Body.String())
			}
			assertFile(t, filepath.Join(dir, "config.json"), raw)
			if mode == "persist" {
				assertFile(t, backup, raw)
				g.persist = g.save
			} else {
				if err := os.Remove(backup); err != nil {
					t.Fatal(err)
				}
			}
			w = admin(t, g, "PATCH", "/admin/v1/routes/rt_existing", `{"enabled":false}`, `"5"`, "")
			if w.Code != 200 {
				t.Fatal("retry failed", w.Body.String())
			}
			assertFile(t, backup, raw)
		})
	}
}

func TestConfigSchemasAndLegacyA2A(t *testing.T) {
	raw := legacyFixture(t)
	card := `,"agent_card":{"name":"test","description":"test","version":"1","skills":[],"defaultInputModes":[],"defaultOutputModes":[]}`
	a2a := bytes.Replace(raw, []byte(`"protocol": "proxy"`), []byte(`"protocol": "a2a"`+card), 1)
	c, migrated, err := loadConfig(a2a)
	if err != nil || !migrated || validate(&c) != nil || c.Routes[0].Endpoints[0].Interfaces["a2a"].ProtocolVersion != "1.0" {
		t.Fatal("legacy A2A migration", err)
	}
	for _, raw := range [][]byte{
		[]byte(`{"schema_version":3}`),
		bytes.Replace(raw, []byte(`"protocol": "proxy"`), []byte(`"protocol": "proxy", "expose":["proxy"]`), 1),
		bytes.Replace(raw, []byte(`"schema_version": 1`), []byte(`"schema_version": 2`), 1),
		[]byte(`{"schema_version":2} {}`),
	} {
		dir := t.TempDir()
		writeConfig(t, dir, raw)
		if g, err := Open(testOptions(dir)); err == nil {
			g.Close()
			t.Fatalf("accepted invalid schema: %s", raw)
		}
		assertFile(t, filepath.Join(dir, "config.json"), raw)
	}
}

func TestNewModelValidation(t *testing.T) {
	cases := []struct {
		body   string
		status int
	}{
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 201},
		{`{"key":"test","agent_type":"hermes","expose":["proxy","a2a"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"},"a2a":{"url":"http://localhost:2/rpc","protocol_version":"1.0"}}}]}`, 201},
		{`{"key":"test","expose":[]}`, 400},
		{`{"key":"test","agent_type":"other","expose":["proxy"]}`, 400},
		{`{"key":"test","expose":["proxy","proxy"]}`, 400},
		{`{"key":"test","expose":["proxy","a2a"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 400},
		{`{"key":"test","protocol":"proxy","expose":["proxy"]}`, 409},
		{`{"key":"test","protocol":"proxy","endpoints":[{"interfaces":{}}]}`, 409},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"url":"http://localhost:1"}]}`, 409},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","protocol_version":"1.0"}}}]}`, 400},
		{`{"key":"test","expose":["a2a"],"endpoints":[{"interfaces":{"a2a":{"url":"http://localhost:1","protocol_version":"1.0.1"}}}]}`, 400},
		{`{"key":"test","expose":["a2a"],"endpoints":[{"interfaces":{"a2a":{"url":"http://localhost:1","protocol_version":"1.0"}}},{"interfaces":{"a2a":{"url":"http://localhost:2","protocol_version":"0.3.0"}}}]}`, 400},
		{`{"key":"test","expose":["proxy"],"endpoints":[{"interfaces":{"proxy":{"url":"http://localhost:1","extra":true}}}]}`, 400},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			g := setup(t)
			w := admin(t, g, "POST", "/admin/v1/routes", tc.body, "", "")
			if w.Code != tc.status {
				t.Fatalf("want %d got %d: %s", tc.status, w.Code, w.Body.String())
			}
			if tc.status == 201 && (bytes.Contains(w.Body.Bytes(), []byte(`"protocol":`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"agent_type":`))) {
				t.Fatal("legacy response shape", w.Body.String())
			}
		})
	}
}

func TestLegacyManagementUpdatesNormalizeAndPreserveInterfaces(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	path := "/admin/v1/routes/" + rt.ID
	epPath := path + "/endpoints/" + rt.Endpoints[0].ID
	w := admin(t, g, "PATCH", epPath, `{"url":"http://localhost:2","health_url":"http://localhost:2/health"}`, `"1"`, "")
	if w.Code != 200 || bytes.Contains(w.Body.Bytes(), []byte(`"url":"http://localhost:2","weight"`)) {
		t.Fatal(w.Body.String())
	}
	ep := g.current.Load().Routes["test"].Endpoints[0]
	if ep.Interfaces["proxy"].URL != "http://localhost:2" || ep.Version != 2 {
		t.Fatal(ep)
	}
	w = admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"health_url":null}}}`, `"2"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	ep = g.current.Load().Routes["test"].Endpoints[0]
	if ep.Interfaces["proxy"].URL != "http://localhost:2" || ep.Interfaces["proxy"].HealthURL != "" {
		t.Fatal("nested merge replaced URL", ep)
	}
	w = admin(t, g, "POST", path+"/endpoints", `{"url":"http://localhost:3"}`, "", "")
	if w.Code != 201 || !bytes.Contains(w.Body.Bytes(), []byte(`"interfaces":`)) {
		t.Fatal(w.Body.String())
	}
	before := g.current.Load().Routes["test"]
	w = admin(t, g, "PATCH", path, `{"protocol":"a2a"}`, tag(before.Version), "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	after := g.current.Load().Routes["test"]
	for i, e := range after.Endpoints {
		if len(e.Interfaces) != 1 || e.Interfaces["a2a"].ProtocolVersion != "1.0" || e.Version != before.Endpoints[i].Version+1 {
			t.Fatal("legacy protocol switch lost endpoint version/config", e)
		}
	}
	if !exposes(before, "proxy") || before.Endpoints[0].Interfaces["proxy"].URL != "http://localhost:2" {
		t.Fatal("published snapshot was mutated")
	}
}
