package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/protocol"
)

func TestReconcileContinuesAfterConsumerRevocation(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q a2a.RPC
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		writeTestTask(w, q, map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_COMPLETED"}})
	}))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]protocol.EndpointInterface{"proxy": {URL: up.URL}, "a2a": {URL: up.URL, ProtocolVersion: "1.0"}})
	value := map[string]any{"id": "up-task", "contextId": "up-context", "status": map[string]any{"state": "TASK_STATE_WORKING"}}
	if err := g.a2aHandler.MapResult(value, "alice", rt.ID, rt.Endpoints[0].ID, ""); err != nil {
		t.Fatal(err)
	}
	id := value["id"].(string)
	ref, err := g.resources.Lookup(protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.A2ATask, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	g.reconcileResource(context.Background(), ref)
	got, err := g.resources.Lookup(ref.Key())
	if err != nil || got.State != protocol.ResourceTerminal {
		t.Fatal("revocation stranded background execution", got.State, err)
	}
	if _, _, err := g.acquireResource("alice", g.current.Load().Routes["test"], protocol.A2ATask, id); err == nil {
		t.Fatal("revoked caller accessed resource")
	}
}
