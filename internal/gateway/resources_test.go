package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func directoryTest() (*resourceDirectory, *atomic.Int64) {
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	d := newResourceDirectory(Options{MaxResources: 100, MaxResourceAliases: 100, MaxDataIdempotency: 100, MaxSessions: 100, MaxTasks: 100})
	d.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return d, clock
}
func reserveTest(t *testing.T, d *resourceDirectory, owner string, kind resourceKind, alias, parent string, parentKind resourceKind, idem string) creationReservation {
	t.Helper()
	r, err := d.reserve(owner, "route", resourceCreation{Kind: kind, Alias: alias, ParentKind: parentKind, ParentID: parent, Operation: string(kind) + "/create", IdempotencyKey: idem, Fingerprint: "fingerprint"}, func(parent *resource) (string, error) {
		if parent != nil {
			return parent.EndpointID, nil
		}
		return "ep", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func updateTest(t *testing.T, d *resourceDirectory, r resource, state resourceState) {
	t.Helper()
	if err := d.update(r, state); err != nil {
		t.Fatal(err)
	}
}
func cleanTest(d *resourceDirectory) { d.cleanup(func(resource) bool { return false }) }

func TestResourceConcurrentReservationAndIsolation(t *testing.T) {
	g := setup(t)
	createDual(t, g, localInterfaces(), localInterfaces())
	rt := g.current.Load().Routes["test"]
	spec := resourceCreation{Kind: hermesSession, Alias: "client-session", Operation: "sessions/create", IdempotencyKey: "request-1", Fingerprint: "body-1"}
	var wg sync.WaitGroup
	results := make(chan creationReservation, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := g.reserveResource("alice", rt, spec)
			if err != nil {
				t.Error(err)
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	var first creationReservation
	created := 0
	for r := range results {
		if first.Resource.ID == "" {
			first = r
		}
		if r.Resource.ID != first.Resource.ID || r.Resource.EndpointID != first.Resource.EndpointID || r.Resource.Upstream != first.Resource.Upstream || r.UpstreamIdempotencyKey != first.UpstreamIdempotencyKey {
			t.Fatal("split concurrent reservation")
		}
		if r.First {
			created++
		}
	}
	if created != 1 || first.Resource.Upstream == spec.Alias || first.Resource.ID == spec.Alias || first.Resource.Generation == "" {
		t.Fatal("reservation lacks isolated identity", created, first)
	}
	bob, err := g.reserveResource("bob", rt, spec)
	if err != nil {
		t.Fatal(err)
	}
	if bob.Resource.ID == first.Resource.ID || bob.Resource.Upstream == first.Resource.Upstream || bob.UpstreamIdempotencyKey == first.UpstreamIdempotencyKey {
		t.Fatal("callers share namespace")
	}
	spec.Fingerprint = "different-body"
	if _, err := g.reserveResource("alice", rt, spec); !errors.Is(err, errResourceConflict) {
		t.Fatal("idempotency conflict missing", err)
	}
	spec.Fingerprint = "body-1"
	if _, err := g.reserveResource("unknown-caller", rt, spec); !errors.Is(err, errResourceNotFound) {
		t.Fatal("unknown caller reserved resource", err)
	}
	if _, err := g.resources.lookup(resourceKey{"bob", rt.ID, hermesSession, first.Resource.ID}); !errors.Is(err, errResourceNotFound) {
		t.Fatal("cross caller lookup", err)
	}
	if _, err := g.resources.lookup(resourceKey{"alice", rt.ID, hermesRun, first.Resource.ID}); !errors.Is(err, errResourceNotFound) {
		t.Fatal("cross kind lookup", err)
	}
	alias, err := g.resources.lookup(resourceKey{"alice", rt.ID, hermesSession, spec.Alias})
	if err != nil || alias.ID != first.Resource.ID {
		t.Fatal("alias not bound", err)
	}
	if len(g.resources.list("alice", rt.ID, "", "")) != 1 || len(g.resources.list("bob", rt.ID, "", "")) != 1 {
		t.Fatal("list leaks another caller")
	}
}

func TestResourceLineageAndAliasConflicts(t *testing.T) {
	d, _ := directoryTest()
	parent := reserveTest(t, d, "alice", hermesSession, "original", "", "", "").Resource
	updateTest(t, d, parent, resourceLive)
	child := reserveTest(t, d, "alice", hermesSession, "fork", parent.ID, hermesSession, "").Resource
	if child.EndpointID != parent.EndpointID || child.ParentID != parent.ID || child.Upstream == parent.Upstream {
		t.Fatal("fork changed instance/identity", child)
	}
	if err := d.attachAlias(parent, "compacted", true); err != nil {
		t.Fatal(err)
	}
	if err := d.attachAlias(parent, "public-lineage", false); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"original", "public-lineage", parent.ID} {
		r, err := d.lookup(resourceKey{"alice", "route", hermesSession, alias})
		if err != nil || r.ID != parent.ID || r.Upstream != "compacted" {
			t.Fatal(alias, r, err)
		}
	}
	if err := d.attachAlias(child, "compacted", true); !errors.Is(err, errResourceConflict) {
		t.Fatal("upstream alias collision accepted", err)
	}
	if err := d.attachAlias(child, "original", false); !errors.Is(err, errResourceConflict) {
		t.Fatal("client alias collision accepted", err)
	}
	if err := d.attachAlias(child, parent.ID, false); !errors.Is(err, errResourceConflict) {
		t.Fatal("canonical ID shadowed", err)
	}
	if err := d.forget(parent); !errors.Is(err, errResourceInUse) {
		t.Fatal("parent lost with live fork", err)
	}
	if err := d.abandon(child); err != nil {
		t.Fatal(err)
	}
	if err := d.forget(parent); err != nil {
		t.Fatal(err)
	}
	if d.aliasCount != 0 || len(d.aliases) != 0 || len(d.reverse) != 0 || len(d.items) != 0 {
		t.Fatal("lineage indexes leaked")
	}
	stale := parent
	stale.Generation = "obsolete"
	if err := d.attachAlias(stale, "new", true); !errors.Is(err, errResourceNotFound) {
		t.Fatal("stale generation accepted")
	}
}

func TestResourceDrainAndReferenceProtection(t *testing.T) {
	g := setup(t)
	createDual(t, g, localInterfaces(), localInterfaces())
	rt := g.current.Load().Routes["test"]
	parent, err := g.reserveResource("alice", rt, resourceCreation{Kind: hermesSession, Alias: "parent", Operation: "sessions/create", Fingerprint: "create"})
	if err != nil {
		t.Fatal(err)
	}
	updateTest(t, g.resources, parent.Resource, resourceLive)
	routePath := "/admin/v1/routes/" + rt.ID
	epPath := routePath + "/endpoints/" + parent.Resource.EndpointID
	if w := admin(t, g, "DELETE", epPath, "", `"1"`, ""); w.Code != 409 {
		t.Fatal("reservation did not protect endpoint", w.Body.String())
	}
	if w := admin(t, g, "PATCH", epPath, `{"state":"draining"}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	rt = g.current.Load().Routes["test"]
	fork := resourceCreation{Kind: hermesSession, ParentKind: hermesSession, ParentID: "parent", Operation: "sessions/fork", Fingerprint: "fork"}
	if _, err := g.reserveResource("alice", rt, fork); err == nil {
		t.Fatal("new fork allowed on draining instance")
	}
	run, err := g.reserveResource("alice", rt, resourceCreation{Kind: hermesRun, ParentKind: hermesSession, ParentID: "parent", Operation: "runs/create", Fingerprint: "run", IdempotencyKey: "run-idem"})
	if err != nil {
		t.Fatal("existing session cannot submit run while draining", err)
	}
	if run.Resource.EndpointID != parent.Resource.EndpointID {
		t.Fatal("run moved from parent")
	}
	l, got, err := g.acquireResource("alice", rt, hermesRun, run.Resource.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EndpointID != run.Resource.EndpointID {
		t.Fatal("wrong endpoint")
	}
	l.release()
	l.release()
	current, err := g.resources.lookup(run.Resource.key())
	if err != nil || current.ActiveRefs != 0 {
		t.Fatal("pin leaked", err, current)
	}
	if _, _, err := g.acquireResource("alice", rt, hermesRun, "unknown-run"); !errors.Is(err, errResourceNotFound) {
		t.Fatal("unknown run was scheduled", err)
	}
	g.state(rt.ID, parent.Resource.EndpointID).proxy.healthy.Store(false)
	if l, _, err := g.acquireResource("alice", rt, hermesRun, run.Resource.ID); err == nil {
		l.release()
		t.Fatal("unhealthy resource was reselected")
	}
	if w := admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"url":"http://localhost:9"}}}`, `"2"`, ""); w.Code != 409 {
		t.Fatal("changed reserved endpoint", w.Body.String())
	}
	if w := admin(t, g, "DELETE", routePath, "", tag(rt.Version), ""); w.Code != 409 {
		t.Fatal("deleted referenced route", w.Body.String())
	}
	view := admin(t, g, "GET", routePath+"/runs?owner_id=alice", "", "", "")
	if view.Code != 200 || !strings.Contains(view.Body.String(), run.Resource.ID) {
		t.Fatal(view.Body.String())
	}
	detail := admin(t, g, "GET", epPath, "", "", "")
	if !strings.Contains(detail.Body.String(), `"nonterminal_runs":1`) || !strings.Contains(detail.Body.String(), `"reserved_resources":1`) {
		t.Fatal(detail.Body.String())
	}
}

func TestResourceReservationCapacityAndAbandon(t *testing.T) {
	for _, limit := range []string{"resources", "aliases", "operations"} {
		t.Run(limit, func(t *testing.T) {
			d, _ := directoryTest()
			switch limit {
			case "resources":
				d.max = 1
			case "aliases":
				d.maxAliases = 1
			case "operations":
				d.maxOperations = 1
			}
			first := reserveTest(t, d, "alice", hermesSession, "one", "", "", "idem-one")
			called := false
			_, err := d.reserve("alice", "route", resourceCreation{Kind: hermesSession, Alias: "two", Operation: "sessions/create", IdempotencyKey: "idem-two", Fingerprint: "two"}, func(*resource) (string, error) { called = true; return "other", nil })
			if !errors.Is(err, errResourceCapacity) || called || len(d.items) != 1 || len(d.operations) != 1 || d.aliasCount != 1 {
				t.Fatal("capacity failure left partial reservation", err, called)
			}
			if err := d.abandon(first.Resource); err != nil {
				t.Fatal(err)
			}
			if len(d.items) != 0 || len(d.operations) != 0 || len(d.reverse) != 0 || len(d.aliases) != 0 {
				t.Fatal("abandoned reservation leaked indexes")
			}
			second := reserveTest(t, d, "alice", hermesSession, "one", "", "", "idem-one")
			if first.Resource.Generation == second.Resource.Generation || first.Resource.ID == second.Resource.ID {
				t.Fatal("new generation reused abandoned identity")
			}
			if err := d.update(first.Resource, resourceUnknown); !errors.Is(err, errResourceNotFound) {
				t.Fatal("late old response affected new resource")
			}
			updateTest(t, d, second.Resource, resourceUnknown)
			if err := d.abandon(second.Resource); !errors.Is(err, errResourceInUse) {
				t.Fatal("uncertain request abandoned")
			}
			if err := d.update(second.Resource, resourceReserved); !errors.Is(err, errResourceConflict) {
				t.Fatal("uncertain resource returned to unsent state")
			}
		})
	}
}

func TestAtomicA2AObservationAndTerminalState(t *testing.T) {
	g := setup(t)
	g.resources.maxTasks = 1
	initial := map[string]any{"id": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.mapResult(initial, "alice", "route", "ep", "s"); err != nil {
		t.Fatal(err)
	}
	publicTask := initial["id"].(string)
	publicContext := initial["contextId"].(string)
	for _, bad := range []map[string]any{
		{"id": "two", "contextId": "another", "status": map[string]any{"state": "TASK_STATE_WORKING"}},
		{"id": "one", "contextId": "another", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}},
		{"id": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}, "history": []any{map[string]any{"messageId": "m", "role": "ROLE_AGENT", "parts": []any{map[string]any{"text": "ok"}}, "contextId": "extra", "referenceTaskIds": []any{"missing"}}}},
	} {
		before, _ := json.Marshal(bad)
		if err := g.mapResult(bad, "alice", "route", "ep", "s"); err == nil {
			t.Fatal("invalid/oversize batch accepted")
		}
		after, _ := json.Marshal(bad)
		if string(before) != string(after) {
			t.Fatal("failed response partly rewritten")
		}
		if len(g.resources.list("", "route", "", "")) != 2 {
			t.Fatal("failed batch left partial context/task")
		}
		got, err := g.resources.lookup(resourceKey{"alice", "route", a2aTask, publicTask})
		if err != nil || got.State != resourceLive {
			t.Fatal("failed batch changed old task", got, err)
		}
	}
	repeated := map[string]any{"id": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}, "history": []any{map[string]any{"messageId": "m", "role": "ROLE_AGENT", "parts": []any{map[string]any{"text": "ok"}}, "taskId": "one", "contextId": "context"}}}
	if err := g.mapResult(repeated, "alice", "route", "ep", "s"); err != nil {
		t.Fatal(err)
	}
	ctx, _ := g.resources.lookup(resourceKey{"alice", "route", a2aContext, publicContext})
	if ctx.ChildRefs != 1 {
		t.Fatal("duplicate observations duplicated parent refs", ctx)
	}
	late := map[string]any{"taskId": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.mapObject(late, "statusUpdate", "alice", "route", "ep", "s"); err != nil {
		t.Fatal(err)
	}
	task, _ := g.resources.lookup(resourceKey{"alice", "route", a2aTask, publicTask})
	if task.State != resourceTerminal {
		t.Fatal("late event resurrected terminal task")
	}
}

func TestResourceLifecycleRetentionAndCleanup(t *testing.T) {
	d, clock := directoryTest()
	parent := reserveTest(t, d, "alice", hermesSession, "session", "", "", "").Resource
	updateTest(t, d, parent, resourceLive)
	run := reserveTest(t, d, "alice", hermesRun, "run", parent.ID, hermesSession, "run-key").Resource
	if err := d.attachAlias(run, "upstream-run", true); err != nil {
		t.Fatal(err)
	}
	updateTest(t, d, run, resourceUnknown)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 || len(d.operations) != 1 {
		t.Fatal("unknown run lost ownership/parent/idempotency")
	}
	updateTest(t, d, run, resourceWaiting)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 {
		t.Fatal("approval wait expired")
	}
	updateTest(t, d, run, resourceLive)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 {
		t.Fatal("active run expired")
	}
	updateTest(t, d, run, resourceTerminal)
	pinned, release, err := d.pin(run.key())
	if err != nil {
		t.Fatal(err)
	}
	clock.Add(int64(25 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 || pinned.ActiveRefs != 1 {
		t.Fatal("active read lost mapping")
	}
	release()
	release()
	clock.Add(int64(25 * time.Hour))
	cleanTest(d)
	if len(d.items) != 0 || len(d.reverse) != 0 || len(d.operations) != 0 || d.aliasCount != 0 {
		t.Fatal("terminal graph/indexes did not expire")
	}
	for _, key := range []resourceKey{parent.key(), run.key(), {"alice", "route", hermesSession, "session"}, {"alice", "route", hermesRun, "run"}} {
		if _, err := d.lookup(key); !errors.Is(err, errResourceNotFound) {
			t.Fatal("expired alias still resolves", key, err)
		}
	}
}

func TestResourceReverseNamespaceAndUnknownStates(t *testing.T) {
	g := setup(t)
	ids := map[string]bool{}
	for _, owner := range []string{"alice", "bob"} {
		for _, ep := range []string{"ep-1", "ep-2"} {
			body := map[string]any{"id": "same-upstream", "contextId": "same-context", "status": map[string]any{"state": "TASK_STATE_FUTURE_UNKNOWN"}, "artifacts": []any{map[string]any{"artifactId": "a", "parts": []any{map[string]any{"data": map[string]any{"id": "same-upstream", "contextId": "same-context"}}}}}}
			if err := g.mapResult(body, owner, "route", ep, "session"); err != nil {
				t.Fatal(err)
			}
			id := body["id"].(string)
			if ids[id] {
				t.Fatal("upstream IDs collided across owner/endpoint")
			}
			ids[id] = true
			got, _ := g.resources.lookup(resourceKey{owner, "route", a2aTask, id})
			if got.State != resourceUnknown {
				t.Fatal("unknown state treated as terminal")
			}
			raw, _ := json.Marshal(body)
			if !strings.Contains(string(raw), `"id":"same-upstream"`) {
				t.Fatal("business data rewritten")
			}
		}
	}
	if len(g.resources.list("alice", "route", a2aTask, "")) != 2 || len(g.resources.list("alice", "route", a2aTask, "ep-1")) != 1 {
		t.Fatal("scoped list incorrect")
	}
}

func TestDataFingerprintAndIdempotencyNamespace(t *testing.T) {
	h := http.Header{"X-Hermes-Session-Key": []string{"session-a"}}
	a := dataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "X-Hermes-Session-Key")
	if a != dataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "x-hermes-session-key") {
		t.Fatal("header casing changes fingerprint")
	}
	h.Set("X-Hermes-Session-Key", "session-b")
	if a == dataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "X-Hermes-Session-Key") {
		t.Fatal("semantic header omitted from fingerprint")
	}
	if a == dataFingerprint("POST", "/v1/runs", []byte(`{"input":"different"}`), nil, "X-Hermes-Session-Key") {
		t.Fatal("body omitted from fingerprint")
	}
	keys := map[string]bool{}
	for _, parts := range [][4]string{{"a", "bc", "run", "key"}, {"ab", "c", "run", "key"}, {"a", "bc", "response", "key"}, {"a", "bc", "run", "other-key"}} {
		k := namespacedIdempotency(parts[0], parts[1], parts[2], parts[3])
		if keys[k] || len(k) > 256 {
			t.Fatal("idempotency namespace collision")
		}
		keys[k] = true
	}
}

func TestResourceRestartDropsReservations(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	createDual(t, g, localInterfaces())
	rt := g.current.Load().Routes["test"]
	reserved, err := g.reserveResource("alice", rt, resourceCreation{Kind: hermesSession, Alias: "old", Operation: "sessions/create", IdempotencyKey: "idem", Fingerprint: "body"})
	if err != nil {
		g.Close()
		t.Fatal(err)
	}
	g.Close()
	g, err = Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	rt = g.current.Load().Routes["test"]
	for _, id := range []string{reserved.Resource.ID, "old"} {
		if l, _, err := g.acquireResource("alice", rt, hermesSession, id); !errors.Is(err, errResourceNotFound) {
			if l != nil {
				l.release()
			}
			t.Fatal("old resource recovered by scheduling", err)
		}
	}
	if len(g.resources.items) != 0 || len(g.resources.operations) != 0 {
		t.Fatal("runtime resources persisted")
	}
}

func TestA2AResourceFailureNeverLeaksPartialIDs(t *testing.T) {
	var invalid atomic.Bool
	invalid.Store(true)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]any{"id": "up", "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_INPUT_REQUIRED"}}
		if invalid.Load() {
			result["history"] = []any{map[string]any{"messageId": "m", "role": "ROLE_AGENT", "contextId": "ctx", "parts": []any{map[string]any{"text": "ok"}}, "referenceTaskIds": []any{"missing"}}}
		}
		var q rpc
		_ = json.NewDecoder(r.Body).Decode(&q)
		writeTestTask(w, q, result)
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	first := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "s")
	if !strings.Contains(first.Body.String(), `"error"`) || strings.Contains(first.Body.String(), `"task_`) || len(g.resources.items) != 0 {
		t.Fatal("partial response/mapping escaped", first.Body.String())
	}
	invalid.Store(false)
	second := dataRequest(g, "POST", "/a2a/test", sendV1, "alice", "s")
	var result struct {
		Result struct {
			Task struct {
				ID string `json:"id"`
			} `json:"task"`
		} `json:"result"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &result); err != nil || result.Result.Task.ID == "" {
		t.Fatal(second.Body.String(), err)
	}
	query := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, result.Result.Task.ID)
	rt := g.current.Load().Routes["test"]
	view := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/tasks?owner_id=alice", "", "", "")
	if view.Code != 200 || !strings.Contains(view.Body.String(), `"gateway_task_id":"`+result.Result.Task.ID+`"`) || !strings.Contains(view.Body.String(), `"resource_kind":"a2a_task"`) {
		t.Fatal("legacy task view changed", view.Body.String())
	}
	denied := dataRequest(g, "POST", "/agents/test/a2a", query, "bob", "")
	if !strings.Contains(denied.Body.String(), `"code":-32001`) {
		t.Fatal("cross caller task access", denied.Body.String())
	}
}

func TestOpaqueUpstreamIDsDoNotUseClientAliasRules(t *testing.T) {
	g := setup(t)
	body := map[string]any{"id": "tenant/tasks/one", "contextId": "tenant/contexts/one", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.mapResult(body, "alice", "route", "ep", "session"); err != nil {
		t.Fatal(err)
	}
	task, err := g.resources.lookup(resourceKey{"alice", "route", a2aTask, body["id"].(string)})
	if err != nil || task.Upstream != "tenant/tasks/one" {
		t.Fatal("opaque upstream ID changed", task, err)
	}
	if err := g.resources.attachAlias(task, "client/alias", false); !errors.Is(err, errResourceInvalid) {
		t.Fatal("path separator allowed in client alias")
	}
}

func TestResponseChainParentsAndFingerprintHeaderValues(t *testing.T) {
	d, _ := directoryTest()
	first := reserveTest(t, d, "alice", hermesResponse, "conversation", "", "", "").Resource
	if first.Upstream != "" {
		t.Fatal("server-generated response ID invented before response")
	}
	if err := d.attachAlias(first, "resp-upstream-1", true); err != nil {
		t.Fatal(err)
	}
	updateTest(t, d, first, resourceTerminal)
	next := reserveTest(t, d, "alice", hermesResponse, "", first.ID, hermesResponse, "").Resource
	if next.EndpointID != first.EndpointID || next.ParentID != first.ID {
		t.Fatal("response continuation lost origin")
	}
	if err := d.forget(first); !errors.Is(err, errResourceInUse) {
		t.Fatal("response parent discarded")
	}
	_, err := d.reserve("bob", "route", resourceCreation{Kind: hermesResponse, ParentKind: hermesResponse, ParentID: first.ID, Operation: "responses/create", Fingerprint: "new"}, func(*resource) (string, error) { t.Fatal("foreign parent triggered scheduling"); return "", nil })
	if !errors.Is(err, errResourceNotFound) {
		t.Fatal(err)
	}
	h := http.Header{"X-Hermes-Session-Key": []string{"a"}}
	before := dataFingerprint("POST", "/v1/runs", nil, h, "X-Hermes-Session-Key")
	h.Add("X-Hermes-Session-Key", "b")
	if before == dataFingerprint("POST", "/v1/runs", nil, h, "X-Hermes-Session-Key") {
		t.Fatal("additional semantic header value omitted")
	}
}
