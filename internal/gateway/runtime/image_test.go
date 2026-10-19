package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

func stateImageFixture(t *testing.T) (*sharedFixture, []byte, string) {
	t.Helper()
	f := sharedSetup(t)
	rt := createDual(t, f.core, localInterfaces())
	if w := admin(t, f.core, "POST", "/admin/v1/routes", `{"key":"test"}`, "", "admin-once"); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	route := f.core.current.Load().Routes[rt.Key]
	c := shared.NewLocalClient(f.server, "candidate")
	t.Cleanup(c.Close)
	call := func(action string, q shared.Request) shared.Response {
		t.Helper()
		var out shared.Response
		if err := c.Call(action, q, &out); err != nil {
			t.Fatal(action, err)
		}
		return out
	}
	for i := 0; i < 2; i++ {
		call("acquire", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Entry: "proxy", Session: "existing"})
	}
	session := call("reserve", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version,
		Creation: protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "client-session", Operation: "create", IdempotencyKey: "session-once", Fingerprint: "session-body"}}).Reservation.Resource
	call("update", shared.Request{Resource: session, State: protocol.ResourceLive})
	call("reserve", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version,
		Creation: protocol.ResourceCreation{Kind: protocol.HermesRun, ParentID: session.ID, ParentKind: session.Kind, Operation: "run", IdempotencyKey: "run-once", Fingerprint: "run-body"}})
	call("alias", shared.Request{Resource: session, Endpoint: "upstream-after-compression", Flag: true})
	call("pin", shared.Request{Key: protocol.ResourceKey{Owner: "alice", Route: route.ID, Kind: session.Kind, ID: "client-session"}})
	call("observe", shared.Request{Owner: "alice", Route: route.ID, Endpoint: route.Endpoints[0].ID, Observations: []protocol.ResourceObservation{
		{Kind: protocol.A2AContext, Upstream: "context-up", State: protocol.ResourceLive},
		{Kind: protocol.A2ATask, Upstream: "task-up", ParentUpstream: "context-up", State: protocol.ResourceWaiting},
	}})
	call("request_result", shared.Request{})
	call("request_result", shared.Request{Flag: true})
	for _, kind := range []string{"tasks", "sessions"} {
		call("cache_acquire", shared.Request{Entry: kind})
		payload := json.RawMessage(`{ "public_id": "kept-exactly", "state": "unknown" }`)
		_, err := f.core.listCache(kind).Put(protocol.ListSnapshot{Owner: "alice", Route: route.ID, Revision: route.Version,
			Filter: kind, Expires: time.Now().Add(-time.Second), Size: 10, Bytes: len(payload), Tasks: []json.RawMessage{payload}})
		if err != nil {
			t.Fatal(err)
		}
	}
	st := f.core.state(route.ID, route.Endpoints[0].ID)
	st.proxy.healthy.Store(false)
	iface := route.Endpoints[0].Interfaces["proxy"]
	iface.Token = "previous-discovery-credential"
	st.proxy.discovery.Store(&protocol.Capabilities{Interface: iface, Checked: time.Now().Add(-time.Minute), Ready: false, Features: map[string]any{"session_chat": true}})
	f.core.sched["removed-route/proxy"] = &routeSchedule{weights: map[string]int64{"removed-endpoint": -42}}
	if err := f.server.Freeze(true); err != nil {
		t.Fatal(err)
	}
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, digest, err := f.core.ExportState(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	return f, raw, digest
}

func TestStateImagePreservesLiveStateWithoutStorageOrUpstreamEffects(t *testing.T) {
	f, raw, digest := stateImageFixture(t)
	before := f.j.Snapshot()
	o := f.core.opts
	o.DataDir = filepath.Join(t.TempDir(), "must-not-be-created")
	o.dialContext = func(context.Context, string, string) (net.Conn, error) {
		t.Error("read-only import contacted an upstream")
		return nil, context.Canceled
	}
	snapshot, err := shared.NewSnapshot(raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := shared.NewSnapshotReceiver(snapshot.Manifest(), len(raw))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < snapshot.Manifest().Chunks; i++ {
		chunk, err := snapshot.Chunk(i)
		if err != nil {
			t.Fatal(err)
		}
		if err := receiver.Add(chunk); err != nil {
			t.Fatal(err)
		}
	}
	assembled, err := receiver.Finish()
	if err != nil {
		t.Fatal(err)
	}
	v, err := OpenStateView(assembled, snapshot.Manifest().Digest, len(raw), o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.DataDir); !os.IsNotExist(err) {
		t.Fatal("read-only import opened candidate storage", err)
	}
	meta := v.Metadata()
	if meta.Digest != digest || meta.Bytes != len(raw) || meta.Sessions != 1 || meta.Resources != 4 || meta.Operations != 2 || meta.Leases != 5 || meta.ConfigRevision != f.core.current.Load().Config.Revision {
		t.Fatal("incomplete image", meta)
	}
	if !v.image.ConfigFrozen || v.image.Inflight != 2 || v.image.Requests != 2 || v.image.Failures != 1 {
		t.Fatal("lost freeze, accounting or metrics")
	}
	for _, r := range f.core.resources.List("", "", "", "") {
		got, ok := v.Resource(r.Key())
		if !ok || !got.Seen.Equal(r.Seen) || got.ActiveRefs != r.ActiveRefs || got.ChildRefs != r.ChildRefs || got.WorkerOwner != r.WorkerOwner || got.EndpointID != r.EndpointID {
			t.Fatal("resource identity or exact lifetime changed", r, got)
		}
	}
	for _, kind := range []string{"tasks", "sessions"} {
		original := f.core.listCache(kind).ExportImage().Snapshots[0]
		got, ok := v.List(kind, original.Key)
		if !ok || !got.Expires.Equal(original.Expires) || !bytes.Equal(got.Tasks[0], original.Tasks[0]) || got.Bytes != original.Bytes {
			t.Fatal("list cursor expired, bytes changed or TTL extended during import", got)
		}
		got.Tasks[0][0] = '!'
		again, _ := v.List(kind, original.Key)
		if !json.Valid(again.Tasks[0]) {
			t.Fatal("view returned mutable list storage")
		}
	}
	for key, ep := range v.image.Endpoints {
		original := f.core.states[key]
		if ep.Proxy.Healthy != original.proxy.healthy.Load() || ep.Proxy.Active != original.proxy.active.Load() {
			t.Fatal("lost health or interface capacity")
		}
		if ep.Proxy.Discovery != nil && ep.Proxy.Discovery.Interface != original.proxy.discovery.Load().Interface {
			t.Fatal("json-hidden discovery interface was lost")
		}
	}
	if v.image.Schedules["removed-route/proxy"]["removed-endpoint"] != -42 {
		t.Fatal("scheduler history was dropped before its normal retirement")
	}
	config := v.Config()
	config.Routes[0].Consumers[0].TokenHash = "modified"
	if v.Config().Routes[0].Consumers[0].TokenHash == "modified" {
		t.Fatal("view configuration was not detached")
	}
	copy := v.Bytes()
	copy[0] = '!'
	if !bytes.Equal(v.Bytes(), raw) {
		t.Fatal("view exposed snapshot backing bytes")
	}
	again, nextDigest, err := f.core.ExportState(1 << 20)
	if err != nil || nextDigest != digest || !bytes.Equal(again, raw) || !reflect.DeepEqual(before, f.j.Snapshot()) {
		t.Fatal("import changed the old owner or export was nondeterministic", err)
	}
}

func TestStateImageRejectsInvalidReferencesAndAccounting(t *testing.T) {
	_, raw, _ := stateImageFixture(t)
	tests := map[string]func(*stateImage){
		"format":             func(v *stateImage) { v.Format = "future" },
		"duplicate session":  func(v *stateImage) { v.Sessions = append(v.Sessions, v.Sessions[0]); v.Bindings++ },
		"session endpoint":   func(v *stateImage) { v.Sessions[0].Endpoint = "missing" },
		"session TTL":        func(v *stateImage) { v.Sessions[0].TTL = 0 },
		"session accounting": func(v *stateImage) { v.Sessions[0].Active++ },
		"binding accounting": func(v *stateImage) { v.Bindings++ },
		"global accounting":  func(v *stateImage) { v.Inflight++ },
		"caller accounting":  func(v *stateImage) { v.Callers["alice"]++ },
		"endpoint accounting": func(v *stateImage) {
			for k, ep := range v.Endpoints {
				ep.Proxy.Active++
				v.Endpoints[k] = ep
				break
			}
		},
		"lease endpoint state":    func(v *stateImage) { v.Endpoints = nil },
		"resource endpoint":       func(v *stateImage) { v.Resources.Resources[0].Resource.EndpointID = "missing" },
		"resource active refs":    func(v *stateImage) { v.Resources.Resources[0].Resource.ActiveRefs++ },
		"resource child refs":     func(v *stateImage) { v.Resources.Resources[0].Resource.ChildRefs++ },
		"resource operation refs": func(v *stateImage) { v.Resources.Resources[0].OperationRefs++ },
		"orphan operation":        func(v *stateImage) { v.Resources.Operations[0].Resource.ID = "missing" },
		"operation fingerprint":   func(v *stateImage) { v.Resources.Operations[0].Fingerprint = "changed" },
		"duplicate operation": func(v *stateImage) {
			v.Resources.Operations = append(v.Resources.Operations, v.Resources.Operations[0])
		},
		"public alias collision":  func(v *stateImage) { r := &v.Resources.Resources[0]; r.Aliases = append(r.Aliases, r.Resource.ID) },
		"missing active upstream": func(v *stateImage) { v.Resources.Resources[0].Resource.Upstream = "missing" },
		"parent cycle": func(v *stateImage) {
			var session, run *protocol.Resource
			for i := range v.Resources.Resources {
				r := &v.Resources.Resources[i].Resource
				if r.Kind == protocol.HermesSession {
					session = r
				}
				if r.Kind == protocol.HermesRun {
					run = r
				}
			}
			session.ParentID, session.ParentKind = run.ID, run.Kind
			run.ChildRefs++
		},
		"lost pin lease": func(v *stateImage) {
			for i, l := range v.Transfer.Leases {
				if l.Kind == "pin" {
					v.Transfer.Leases = append(v.Transfer.Leases[:i], v.Transfer.Leases[i+1:]...)
					break
				}
			}
		},
		"duplicate lease":      func(v *stateImage) { v.Transfer.Leases = append(v.Transfer.Leases, v.Transfer.Leases[0]) },
		"lease holder":         func(v *stateImage) { v.Transfer.Leases[0].Worker = "" },
		"receipt epoch":        func(v *stateImage) { v.Transfer.Receipts[0].Response.Epoch = "other" },
		"duplicate receipt":    func(v *stateImage) { v.Transfer.Receipts = append(v.Transfer.Receipts, v.Transfer.Receipts[0]) },
		"config receipt order": func(v *stateImage) { v.Config.ReceiptOrder[0] = "missing" },
		"config receipt hash": func(v *stateImage) {
			r := v.Config.Receipts["admin-once"]
			r.Hash = "invalid"
			v.Config.Receipts["admin-once"] = r
		},
		"list accounting":     func(v *stateImage) { v.Tasks.Active++ },
		"list bytes":          func(v *stateImage) { v.Tasks.Snapshots[0].Bytes++ },
		"list missing expiry": func(v *stateImage) { v.Tasks.Snapshots[0].Expires = time.Time{} },
		"duplicate cursor":    func(v *stateImage) { v.Tasks.Snapshots = append(v.Tasks.Snapshots, v.Tasks.Snapshots[0]) },
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			var v stateImage
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatal(err)
			}
			corrupt(&v)
			changed, digest, err := shared.EncodeTransfer(v, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := OpenStateView(changed, digest, 1<<20, Options{}); err == nil || got != nil {
				t.Fatal("corrupt image accepted", name)
			}
		})
	}
}

func TestStateImageEnforcesBudgetsAndCanonicalEncoding(t *testing.T) {
	f, raw, digest := stateImageFixture(t)
	for name, limits := range map[string]Options{
		"sessions": {MaxSessions: 1}, "resources": {MaxResources: 1}, "aliases": {MaxResourceAliases: 1},
		"operations": {MaxDataIdempotency: 1}, "tasks": {MaxTasks: 1}, "inflight": {MaxInflight: 1},
		"caller": {MaxPerCaller: 1}, "endpoint": {MaxPerEndpoint: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if v, err := OpenStateView(raw, digest, len(raw), limits); err == nil || v != nil {
				t.Fatal("candidate capacity ignored")
			}
		})
	}
	if _, _, err := f.core.ExportState(len(raw) - 1); err == nil {
		t.Fatal("export truncated oversized image")
	}
	if _, err := OpenStateView(raw, digest, len(raw)-1, Options{}); err == nil {
		t.Fatal("import exceeded byte budget")
	}
	if _, err := OpenStateView(raw, strings.Repeat("0", 64), len(raw), Options{}); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	for _, bad := range [][]byte{
		append(append([]byte{}, raw...), '\n'),
		append([]byte(`{"format":"ignored",`), raw[1:]...),
		append(append([]byte{}, raw...), []byte(`{}`)...),
	} {
		sum := sha256.Sum256(bad)
		if _, err := OpenStateView(bad, hex.EncodeToString(sum[:]), len(bad), Options{}); err == nil {
			t.Fatal("ambiguous or trailing JSON accepted")
		}
	}
}

func TestStateExportRequiresSealedHealthyOwner(t *testing.T) {
	f := sharedSetup(t)
	if _, _, err := f.core.ExportState(1 << 20); err == nil {
		t.Fatal("exported live owner")
	}
	if err := f.server.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.ExportState(0); err == nil {
		t.Fatal("accepted missing budget")
	}
	f.core.healthy.Store(false)
	if _, _, err := f.core.ExportState(1 << 20); err == nil {
		t.Fatal("exported failed storage")
	}
	if status := f.server.BarrierStatus(); status.Captures != 0 {
		t.Fatal("failed capture stranded admission", status)
	}
}
