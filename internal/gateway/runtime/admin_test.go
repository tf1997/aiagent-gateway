package runtime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
)

const namedRouteInput = `{"consumers":[{"id":"alice","token":"alice-secret"},{"id":"bob","token":"bob-secret"}],"key":"test","agent_type":"generic","expose":["proxy"],"endpoints":[{"name":"one","interfaces":{"proxy":{"url":"http://localhost:8001"}}},{"name":"two","interfaces":{"proxy":{"url":"http://localhost:8002"}}}]}`

func TestRouteUpsertReconcilesEndpointsAndProtectsBindings(t *testing.T) {
	g := setup(t)
	if w := admin(t, g, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	original := g.current.Load().Routes["test"]
	l, err := g.acquire("alice", "test", "sticky", original.Endpoints[0].ID, "proxy", original)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
	input := `{"key":"test","endpoints":[{"name":"two","interfaces":{"proxy":{"url":"http://localhost:8002"}}},{"name":"one","weight":200,"state":"draining","interfaces":{"proxy":{"url":"http://localhost:8001"}}},{"name":"three","interfaces":{"proxy":{"url":"http://localhost:8003"}}}]}`
	w := admin(t, g, "POST", "/admin/v1/routes", input, "", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	rt := g.current.Load().Routes["test"]
	if rt.Endpoints[0].ID != original.Endpoints[1].ID || rt.Endpoints[0].Version != 1 || rt.Endpoints[1].ID != original.Endpoints[0].ID || rt.Endpoints[1].Version != 2 || rt.Endpoints[2].ID == "" {
		t.Fatal("endpoint identity/version changed", rt.Endpoints)
	}
	if original.Endpoints[0].Weight != 100 || original.Endpoints[0].State != "active" {
		t.Fatal("old snapshot mutated")
	}
	l, err = g.acquire("alice", "test", "sticky", "", "proxy", rt)
	if err != nil {
		t.Fatal(err)
	}
	if l.ep.ID != original.Endpoints[0].ID {
		t.Fatal("binding moved")
	}
	l.release()
	before := g.current.Load()
	for _, body := range []string{
		strings.Replace(input, "localhost:8001", "localhost:9001", 1),
		`{"key":"test","endpoints":[]}`,
		`{"key":"test","agent_type":"hermes"}`,
		`{"key":"test","expose":["a2a"]}`,
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", body, "", "")
		if w.Code != 409 || g.current.Load() != before {
			t.Fatal("referenced update was not atomic", body, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "DELETE", "/admin/v1/routes/test", "", "", ""); w.Code != 409 {
		t.Fatal("deleted referenced route", w.Code)
	}
	w = admin(t, g, "GET", "/admin/v1/routes/test/endpoints/"+rt.Endpoints[1].ID, "", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "resource_references") {
		t.Fatal("key endpoint lookup failed", w.Code)
	}
}

func TestRouteUpsertValidationAndAtomicPersistence(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"enabled":false}`, 400},
		{`{"key":"test","id":""}`, 400},
		{`{"key":"test","version":0}`, 400},
		{`{"key":"test","unknown":true}`, 400},
		{`{"key":"test","endpoints":null}`, 400},
		{`{"key":"test","endpoints":[{"id":"invented","interfaces":{"proxy":{"url":"http://localhost:1"}}}]}`, 400},
		{`{"key":"test","endpoints":[{"name":"same","interfaces":{"proxy":{"url":"http://localhost:1"}}},{"name":"same","interfaces":{"proxy":{"url":"http://localhost:2"}}}]}`, 409},
		{`{"key":"test","protocol":"proxy","expose":["proxy"]}`, 409},
		{`{"key":"test","timeout":{"response_header_ms":-1}}`, 400},
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", tc.body, "", "")
		if w.Code != tc.code || g.current.Load() != before {
			t.Fatal(tc.body, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"missing","expose":["proxy"]}`, `"1"`, ""); w.Code != 412 {
		t.Fatal("created with stale precondition", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","protocol":"proxy","endpoints":[{"url":"http://localhost:1"}]}`, "", ""); w.Code != 200 {
		t.Fatal("legacy input failed", w.Code, w.Body.String())
	}
	if g.current.Load().Routes["test"].Endpoints[0].ID != before.Routes["test"].Endpoints[0].ID {
		t.Fatal("legacy endpoint identity lost")
	}
	before = g.current.Load()
	g.configFrozen.Store(true)
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","enabled":false}`, "", ""); w.Code != 409 {
		t.Fatal("upsert bypassed rollout freeze")
	}
	g.configFrozen.Store(false)
	g.persist = func(protocol.Config) (bool, error) { return false, fmt.Errorf("disk full") }
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","enabled":false}`, "", "update"); w.Code != 503 || g.current.Load() != before {
		t.Fatal("failed persistence published update", w.Code)
	}
}

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
				g.persist = func(protocol.Config) (bool, error) { return false, fmt.Errorf("disk full") }
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
	c, migrated, err := configuration.Load(a2a)
	if err != nil || !migrated || configuration.Validate(&c) != nil || c.Routes[0].Endpoints[0].Interfaces["a2a"].ProtocolVersion != "1.0" {
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
	w = admin(t, g, "PATCH", path, `{"protocol":"a2a"}`, configuration.Tag(before.Version), "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	after := g.current.Load().Routes["test"]
	for i, e := range after.Endpoints {
		if len(e.Interfaces) != 1 || e.Interfaces["a2a"].ProtocolVersion != "1.0" || e.Version != before.Endpoints[i].Version+1 {
			t.Fatal("legacy protocol switch lost endpoint version/config", e)
		}
	}
	if !protocol.Exposes(before, "proxy") || before.Endpoints[0].Interfaces["proxy"].URL != "http://localhost:2" {
		t.Fatal("published snapshot was mutated")
	}
}

func TestConsumersValidationAtomicityAndRouteScope(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	for _, input := range []string{
		`null`, `{}`, `[{}]`, `[{"id":"new"}]`, `[{"id":"new","token":""}]`,
		`[{"id":"alice","token":null}]`,
		`[{"id":"new","token":"admin-secret"}]`, `[{"id":"invalid/id","token":"secret"}]`,
		`[{"id":"new","token_hash":"sensitive-value"}]`, `[{"id":"new","token":{}}]`,
		`[{"id":"new","token":"has\nnewline"}]`,
		`[{"id":"same","token":"one"},{"id":"same","token":"two"}]`,
		`[{"id":"one","token":"same"},{"id":"two","token":"same"}]`,
	} {
		w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":`+input+`}`, "", "")
		if w.Code != 400 || g.current.Load() != before || strings.Contains(w.Body.String(), "sensitive-value") {
			t.Fatal(input, w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"other","expose":["proxy"]}`, "", ""); w.Code != 201 {
		t.Fatal(w.Code)
	}
	if w := consumerCall(g, "/agents/other/chat", "alice-secret", "session"); w.Code != 401 {
		t.Fatal("token granted access to another route", w.Code)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"other","consumers":[{"id":"different-owner","token":"alice-secret"}]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if caller := configuration.Authenticate(g.current.Load().Routes["other"], "Bearer alice-secret"); caller == nil || caller.ID != "different-owner" {
		t.Fatal("consumer identity not route scoped")
	}
	before = g.current.Load()
	g.persist = func(protocol.Config) (bool, error) { return false, fmt.Errorf("disk full") }
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[]}`, "", "failed-revoke"); w.Code != 503 || g.current.Load() != before {
		t.Fatal("failed persistence changed access")
	}
}

func TestPersistenceFailure(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1")
	before := g.current.Load()
	g.persist = func(protocol.Config) (bool, error) { return false, fmt.Errorf("disk full") }
	w := admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"enabled":false}`, `"1"`, "")
	if w.Code != 503 || g.current.Load() != before || !g.healthy.Load() {
		t.Fatal("pre-rename failure changed state")
	}
	g.persist = func(protocol.Config) (bool, error) { return true, fmt.Errorf("directory fsync") }
	w = admin(t, g, "PATCH", "/admin/v1/routes/"+rt.ID, `{"enabled":false}`, `"1"`, "")
	if w.Code != 503 || g.healthy.Load() {
		t.Fatal("uncertain commit remained ready")
	}
}
