package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

func waitForSealing(t *testing.T, s *SharedServer) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !s.BarrierStatus().Sealing {
		select {
		case <-deadline:
			t.Fatal("seal did not close admission")
		case <-time.After(time.Millisecond):
		}
	}
}

func awaitBarrierSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not reach barrier test checkpoint")
	}
}

func TestSealWaitsForAdminPersistence(t *testing.T) {
	f := sharedSetup(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(proceed) })
	defer finish()
	f.core.persist = func(c protocol.Config) (bool, error) {
		close(entered)
		<-proceed
		return f.core.save(c)
	}
	updated := make(chan *httptest.ResponseRecorder, 1)
	go func() { updated <- admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", "once") }()
	awaitBarrierSignal(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sealed := make(chan error, 1)
	go func() { sealed <- f.server.Seal(ctx) }()
	waitForSealing(t, f.server)
	if status := f.server.BarrierStatus(); status.Active != 1 || status.Sealed {
		t.Fatal("configuration persistence was not counted", status)
	}
	if err := f.server.Resume(); err == nil {
		t.Fatal("resume reopened admission while seal was still waiting")
	}
	if err := f.server.Freeze(false); !errors.Is(err, shared.ErrSealed) {
		t.Fatal("control bypassed pending seal", err)
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", "late"); w.Code != 503 {
		t.Fatal("late management write entered seal", w.Code, w.Body.String())
	}
	if w := admin(t, f.core, "GET", "/admin/v1/config/status", "", "", ""); w.Code != 200 {
		t.Fatal("seal held a global lock over pending persistence", w.Code)
	}
	finish()
	if w := <-updated; w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := <-sealed; err != nil {
		t.Fatal(err)
	}
	if _, ok := f.core.current.Load().Config.Receipts["once"]; !ok {
		t.Fatal("seal returned before the management receipt was published")
	}
	if _, err := f.server.Export(); err != nil {
		t.Fatal(err)
	}
}

type delayedAdminBody struct {
	reader  *strings.Reader
	started chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (b *delayedAdminBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.proceed
	return b.reader.Read(p)
}

func TestAdminBodyFinishingAfterSealCannotWrite(t *testing.T) {
	f := sharedSetup(t)
	body := &delayedAdminBody{reader: strings.NewReader(namedRouteInput), started: make(chan struct{}), proceed: make(chan struct{})}
	finish := sync.OnceFunc(func() { close(body.proceed) })
	defer finish()
	r := httptest.NewRequest("POST", "/admin/v1/routes", body)
	r.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); f.core.AdminHandler().ServeHTTP(w, r) }()
	awaitBarrierSignal(t, body.started)
	before := f.j.Snapshot()
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	finish()
	awaitBarrierSignal(t, done)
	if w.Code != 503 || len(f.core.current.Load().Routes) != 0 || !reflect.DeepEqual(before, f.j.Snapshot()) {
		t.Fatal("slow body wrote after seal", w.Code, w.Body.String())
	}
	if err := f.server.Resume(); err != nil {
		t.Fatal(err)
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal("aborted handoff did not restore management writes", w.Code)
	}
}

func TestSealWaitsForOwnerHealthDiscoveryAndReconciliation(t *testing.T) {
	for _, kind := range []string{"health", "discovery", "reconcile"} {
		t.Run(kind, func(t *testing.T) {
			f := sharedSetup(t)
			entered, proceed := make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(proceed) })
			var first sync.Once
			var calls atomic.Int64
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				first.Do(func() { close(entered) })
				<-proceed
				switch kind {
				case "health":
					w.WriteHeader(503)
				case "discovery":
					protocol.WriteJSON(w, 200, map[string]any{"features": map[string]any{}})
				case "reconcile":
					var q a2a.RPC
					if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
						t.Error(err)
						return
					}
					writeTestTask(w, q, map[string]any{"id": "task-up", "contextId": "context-up", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}})
				}
			}))
			defer up.Close()
			defer finish()
			iface := protocol.EndpointInterface{URL: up.URL}
			if kind == "health" {
				iface.HealthURL = up.URL + "/health"
			}
			rt := createDual(t, f.core, map[string]protocol.EndpointInterface{"proxy": iface, "a2a": {URL: up.URL, ProtocolVersion: "1.0"}})
			if kind == "discovery" {
				if w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test","agent_type":"hermes"}`, "", ""); w.Code != 200 {
					t.Fatal(w.Body.String())
				}
			}
			var ref protocol.Resource
			probe := func() { f.core.checkHealth(context.Background()) }
			if kind == "reconcile" {
				value := map[string]any{"id": "task-up", "contextId": "context-up", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
				if err := f.core.a2aHandler.MapResult(value, "alice", rt.ID, rt.Endpoints[0].ID, ""); err != nil {
					t.Fatal(err)
				}
				var err error
				ref, err = f.core.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.A2ATask, ID: value["id"].(string)})
				if err != nil {
					t.Fatal(err)
				}
				probe = func() { f.core.reconcileResource(context.Background(), ref) }
			}
			done := make(chan struct{})
			go func() { defer close(done); probe() }()
			awaitBarrierSignal(t, entered)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			sealed := make(chan error, 1)
			go func() { sealed <- f.server.Seal(ctx) }()
			waitForSealing(t, f.server)
			if _, err := f.server.Export(); err == nil {
				t.Fatal("exported while a network probe could still write")
			}
			finish()
			awaitBarrierSignal(t, done)
			if err := <-sealed; err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "health":
				if f.core.states[rt.ID+"/"+rt.Endpoints[0].ID].proxy.healthy.Load() {
					t.Fatal("seal lost the completed health result")
				}
			case "discovery":
				if f.core.states[rt.ID+"/"+rt.Endpoints[0].ID].proxy.discovery.Load() == nil {
					t.Fatal("seal lost the completed discovery result")
				}
			case "reconcile":
				refs := f.core.resources.List("alice", rt.ID, protocol.A2ATask, "")
				if len(refs) != 1 || refs[0].State != protocol.ResourceTerminal || refs[0].ActiveRefs != 0 || f.core.inflight.Load() != 0 {
					t.Fatal("seal did not include reconciliation and lease cleanup", refs)
				}
			}
			before, count := f.j.Snapshot(), calls.Load()
			probe()
			if calls.Load() != count || !reflect.DeepEqual(before, f.j.Snapshot()) {
				t.Fatal("sealed owner started another probe or persisted state")
			}
		})
	}
}

func TestSealedMaintenanceAndAdminReadsDoNotMutateState(t *testing.T) {
	f := sharedSetup(t)
	rt := create(t, f.core, "proxy", "http://localhost:8001")
	key := sessionKey{"alice", rt.ID, "expired", "proxy"}
	sh := f.core.shard(key)
	sh.items[key] = &binding{Endpoint: rt.Endpoints[0].ID, Seen: time.Now().Add(-time.Hour), TTL: time.Second}
	f.core.bindings.Store(1)
	if err := f.core.persistSessions(); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := f.j.Snapshot()
	cursor := 0
	for i := 0; i < 16; i++ {
		f.core.maintainState(&cursor)
	}
	if cursor != 0 || f.core.bindings.Load() != 1 || !reflect.DeepEqual(before, f.j.Snapshot()) {
		t.Fatal("maintenance changed sealed sessions or storage")
	}
	if w := admin(t, f.core, "GET", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, "", "", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(f.core.states) != 0 {
		t.Fatal("administrative read populated a sealed health cache")
	}
	if err := f.server.Resume(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		f.core.maintainState(&cursor)
	}
	if f.core.bindings.Load() != 0 || reflect.DeepEqual(before, f.j.Snapshot()) {
		t.Fatal("maintenance did not resume after abort")
	}
}

func TestSealTimeoutRestoresAdmissionAndLeavesBusinessStreamsIndependent(t *testing.T) {
	f := sharedSetup(t)
	worker := f.localWorker(t, "candidate")
	done, ok := f.core.registerBackground()
	if !ok {
		t.Fatal("owner task rejected before seal")
	}
	finish := sync.OnceFunc(done)
	defer finish()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := f.server.Seal(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("seal ignored an owner background task", err)
	}
	if _, err := f.server.Export(); err == nil {
		t.Fatal("timed out seal enabled export")
	}
	if w := admin(t, f.core, "POST", "/admin/v1/routes", namedRouteInput, "", ""); w.Code != 201 {
		t.Fatal("timeout left admission closed", w.Code)
	}
	finish()
	streamDone, ok := worker.registerBackground()
	if !ok {
		t.Fatal("business stream registration failed")
	}
	defer streamDone()
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal("business stream incorrectly pins old owner", err)
	}
	if done, ok := f.core.registerBackground(); ok {
		done()
		t.Fatal("sealed owner accepted a background task")
	}
	if w := consumerCall(f.core, "/proxy/test/late", "alice-secret", "session"); w.Code != 503 || !strings.Contains(w.Body.String(), "STATE_HANDOFF_SEALED") {
		t.Fatal("direct owner ingress bypassed seal", w.Code, w.Body.String())
	}
}
