package runtime

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
)

func updateTest(t *testing.T, d *resourcedir.Directory, r protocol.Resource, state protocol.ResourceState) {
	t.Helper()
	if err := d.Update(r, state); err != nil {
		t.Fatal(err)
	}
}

func TestResourceConcurrentReservationAndIsolation(t *testing.T) {
	g := setup(t)
	createDual(t, g, localInterfaces(), localInterfaces())
	rt := g.current.Load().Routes["test"]
	spec := protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "client-session", Operation: "sessions/create", IdempotencyKey: "request-1", Fingerprint: "body-1"}
	var wg sync.WaitGroup
	results := make(chan protocol.CreationReservation, 64)
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
	var first protocol.CreationReservation
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
	if _, err := g.reserveResource("alice", rt, spec); !errors.Is(err, protocol.ErrResourceConflict) {
		t.Fatal("idempotency conflict missing", err)
	}
	spec.Fingerprint = "body-1"
	if _, err := g.reserveResource("unknown-caller", rt, spec); !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal("unknown caller reserved resource", err)
	}
	if _, err := g.resources.Lookup(protocol.ResourceKey{Owner: "bob", Route: rt.ID, Kind: protocol.HermesSession, ID: first.Resource.ID}); !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal("cross caller lookup", err)
	}
	if _, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesRun, ID: first.Resource.ID}); !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal("cross kind lookup", err)
	}
	alias, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.HermesSession, ID: spec.Alias})
	if err != nil || alias.ID != first.Resource.ID {
		t.Fatal("alias not bound", err)
	}
	if len(g.resources.List("alice", rt.ID, "", "")) != 1 || len(g.resources.List("bob", rt.ID, "", "")) != 1 {
		t.Fatal("list leaks another caller")
	}
}

func TestResourceDrainAndReferenceProtection(t *testing.T) {
	g := setup(t)
	createDual(t, g, localInterfaces(), localInterfaces())
	rt := g.current.Load().Routes["test"]
	parent, err := g.reserveResource("alice", rt, protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "parent", Operation: "sessions/create", Fingerprint: "create"})
	if err != nil {
		t.Fatal(err)
	}
	updateTest(t, g.resources, parent.Resource, protocol.ResourceLive)
	routePath := "/admin/v1/routes/" + rt.ID
	epPath := routePath + "/endpoints/" + parent.Resource.EndpointID
	if w := admin(t, g, "DELETE", epPath, "", `"1"`, ""); w.Code != 409 {
		t.Fatal("reservation did not protect endpoint", w.Body.String())
	}
	if w := admin(t, g, "PATCH", epPath, `{"state":"draining"}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	rt = g.current.Load().Routes["test"]
	fork := protocol.ResourceCreation{Kind: protocol.HermesSession, ParentKind: protocol.HermesSession, ParentID: "parent", Operation: "sessions/fork", Fingerprint: "fork"}
	if _, err := g.reserveResource("alice", rt, fork); err == nil {
		t.Fatal("new fork allowed on draining instance")
	}
	run, err := g.reserveResource("alice", rt, protocol.ResourceCreation{Kind: protocol.HermesRun, ParentKind: protocol.HermesSession, ParentID: "parent", Operation: "runs/create", Fingerprint: "run", IdempotencyKey: "run-idem"})
	if err != nil {
		t.Fatal("existing session cannot submit run while draining", err)
	}
	if run.Resource.EndpointID != parent.Resource.EndpointID {
		t.Fatal("run moved from parent")
	}
	l, got, err := g.acquireResource("alice", rt, protocol.HermesRun, run.Resource.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EndpointID != run.Resource.EndpointID {
		t.Fatal("wrong endpoint")
	}
	l.release()
	l.release()
	current, err := g.resources.Lookup(run.Resource.Key())
	if err != nil || current.ActiveRefs != 0 {
		t.Fatal("pin leaked", err, current)
	}
	if _, _, err := g.acquireResource("alice", rt, protocol.HermesRun, "unknown-run"); !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal("unknown run was scheduled", err)
	}
	g.state(rt.ID, parent.Resource.EndpointID).proxy.healthy.Store(false)
	if l, _, err := g.acquireResource("alice", rt, protocol.HermesRun, run.Resource.ID); err == nil {
		l.release()
		t.Fatal("unhealthy resource was reselected")
	}
	if w := admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"url":"http://localhost:9"}}}`, `"2"`, ""); w.Code != 409 {
		t.Fatal("changed reserved endpoint", w.Body.String())
	}
	if w := admin(t, g, "DELETE", routePath, "", configuration.Tag(rt.Version), ""); w.Code != 409 {
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

func TestAtomicA2AObservationAndTerminalState(t *testing.T) {
	options := testOptions(t.TempDir())
	options.MaxTasks = 1
	g, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	initial := map[string]any{"id": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.a2aHandler.MapResult(initial, "alice", "route", "ep", "s"); err != nil {
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
		if err := g.a2aHandler.MapResult(bad, "alice", "route", "ep", "s"); err == nil {
			t.Fatal("invalid/oversize batch accepted")
		}
		after, _ := json.Marshal(bad)
		if string(before) != string(after) {
			t.Fatal("failed response partly rewritten")
		}
		if len(g.resources.List("", "route", "", "")) != 2 {
			t.Fatal("failed batch left partial context/task")
		}
		got, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: "route", Kind: protocol.A2ATask, ID: publicTask})
		if err != nil || got.State != protocol.ResourceLive {
			t.Fatal("failed batch changed old task", got, err)
		}
	}
	repeated := map[string]any{"id": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}, "history": []any{map[string]any{"messageId": "m", "role": "ROLE_AGENT", "parts": []any{map[string]any{"text": "ok"}}, "taskId": "one", "contextId": "context"}}}
	if err := g.a2aHandler.MapResult(repeated, "alice", "route", "ep", "s"); err != nil {
		t.Fatal(err)
	}
	ctx, _ := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: "route", Kind: protocol.A2AContext, ID: publicContext})
	if ctx.ChildRefs != 1 {
		t.Fatal("duplicate observations duplicated parent refs", ctx)
	}
	late := map[string]any{"taskId": "one", "contextId": "context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.a2aHandler.MapObject(late, "statusUpdate", "alice", "route", "ep", "s"); err != nil {
		t.Fatal(err)
	}
	task, _ := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: "route", Kind: protocol.A2ATask, ID: publicTask})
	if task.State != protocol.ResourceTerminal {
		t.Fatal("late event resurrected terminal task")
	}
}

func TestResourceReverseNamespaceAndUnknownStates(t *testing.T) {
	g := setup(t)
	ids := map[string]bool{}
	for _, owner := range []string{"alice", "bob"} {
		for _, ep := range []string{"ep-1", "ep-2"} {
			body := map[string]any{"id": "same-upstream", "contextId": "same-context", "status": map[string]any{"state": "TASK_STATE_FUTURE_UNKNOWN"}, "artifacts": []any{map[string]any{"artifactId": "a", "parts": []any{map[string]any{"data": map[string]any{"id": "same-upstream", "contextId": "same-context"}}}}}}
			if err := g.a2aHandler.MapResult(body, owner, "route", ep, "session"); err != nil {
				t.Fatal(err)
			}
			id := body["id"].(string)
			if ids[id] {
				t.Fatal("upstream IDs collided across owner/endpoint")
			}
			ids[id] = true
			got, _ := g.resources.Lookup(protocol.ResourceKey{Owner: owner, Route: "route", Kind: protocol.A2ATask, ID: id})
			if got.State != protocol.ResourceUnknown {
				t.Fatal("unknown state treated as terminal")
			}
			raw, _ := json.Marshal(body)
			if !strings.Contains(string(raw), `"id":"same-upstream"`) {
				t.Fatal("business data rewritten")
			}
		}
	}
	if len(g.resources.List("alice", "route", protocol.A2ATask, "")) != 2 || len(g.resources.List("alice", "route", protocol.A2ATask, "ep-1")) != 1 {
		t.Fatal("scoped list incorrect")
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
	reserved, err := g.reserveResource("alice", rt, protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "old", Operation: "sessions/create", IdempotencyKey: "idem", Fingerprint: "body"})
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
		if l, _, err := g.acquireResource("alice", rt, protocol.HermesSession, id); !errors.Is(err, protocol.ErrResourceNotFound) {
			if l != nil {
				l.release()
			}
			t.Fatal("old resource recovered by scheduling", err)
		}
	}
	if resources, _, operations := g.resources.Sizes(); resources != 0 || operations != 0 {
		t.Fatal("runtime resources persisted")
	}
}

func TestOpaqueUpstreamIDsDoNotUseClientAliasRules(t *testing.T) {
	g := setup(t)
	body := map[string]any{"id": "tenant/tasks/one", "contextId": "tenant/contexts/one", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.a2aHandler.MapResult(body, "alice", "route", "ep", "session"); err != nil {
		t.Fatal(err)
	}
	task, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: "route", Kind: protocol.A2ATask, ID: body["id"].(string)})
	if err != nil || task.Upstream != "tenant/tasks/one" {
		t.Fatal("opaque upstream ID changed", task, err)
	}
	if err := g.resources.AttachAlias(task, "client/alias", false); !errors.Is(err, protocol.ErrResourceInvalid) {
		t.Fatal("path separator allowed in client alias")
	}
}
