package process

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
)

func TestGatewayHandoffKeepsCandidateLeaseAndLatestState(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{"ok":true}`)) }))
	defer up.Close()
	a, cfg, _, _ := completeGateway(t, t.TempDir(), "A")
	configureGateway(t, a, up.URL)
	if w := processRequest(a, "/agents/test/ok", "existing"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	bID := p.Identity{Incarnation: "B", Release: strings.Repeat("b", 64), Instance: "b"}
	bTarget := p.Target{Incarnation: bID.Incarnation, Release: bID.Release, Instance: bID.Instance, URL: "http://gateway", Socket: filepath.Join(cfg.DataDir, "b.sock")}
	client := shared.NewLocalClient(a.shared, bID.Incarnation)
	defer client.Close()
	opts := cfg.Options()
	opts.StateClient, opts.WorkerID, opts.DeferBackground = client, bID.Incarnation, true
	business, err := gateway.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer business.Close()
	var config shared.Response
	if err = client.Call("config", shared.Request{}, &config); err != nil {
		t.Fatal(err)
	}
	rt := config.Config.Routes[0]
	q := shared.Request{ID: "held-acquire", Owner: "alice", Route: rt.Key, Revision: rt.Version, Session: "existing", Entry: "proxy"}
	var acquired shared.Response
	if err = client.Call("acquire", q, &acquired); err != nil {
		t.Fatal(err)
	}
	release := client.Release(acquired.Lease)
	decision, err := a.PrepareHandoff(context.Background(), "h", "r", handoff.Identity{Incarnation: "B", Release: bID.Release}, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if repeated, err := a.PrepareHandoff(context.Background(), "h", "r", decision.Target, 4<<20); err != nil || repeated != decision {
		t.Fatal("lost preparation acknowledgement changed the decision", repeated, err)
	}
	snapshot, err := a.HandoffSnapshot("h", 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := shared.NewSnapshotReceiver(snapshot.Manifest(), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < snapshot.Manifest().Chunks; i++ {
		chunk, err := snapshot.Chunk(i)
		if err != nil {
			t.Fatal(err)
		}
		if err = receiver.Add(chunk); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := receiver.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateHandoff(raw, decision, 4<<20, cfg.Options()); err != nil {
		t.Fatal(err)
	}
	if _, err = AdoptGateway(context.Background(), cfg, bTarget, bID, decision, 4<<20, client, business); err == nil {
		t.Fatal("candidate took source writer lock")
	}
	if err = a.YieldHandoff(decision); err != nil {
		t.Fatal(err)
	}
	b, err := AdoptGateway(context.Background(), cfg, bTarget, bID, decision, 4<<20, client, business)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	d, err := handoff.ReadDecision(cfg.DataDir)
	if err != nil || d.Phase != handoff.Committed || d.ColdStart != bID.Release {
		t.Fatal(d, err)
	}
	if err = b.owner.Abort("h"); !errors.Is(err, handoff.ErrCommitted) {
		t.Fatal("rollback after commit", err)
	}
	var repeated shared.Response
	if err = client.Call("acquire", q, &repeated); err != nil || repeated.Lease != acquired.Lease || b.shared.Count("B") != 1 {
		t.Fatal("lease duplicated or lost", repeated, err)
	}
	release()
	release()
	if b.shared.Count("") != 0 || client.Active() != 0 {
		t.Fatal("imported lease not released exactly once")
	}
	if w := processRequest(b, "/agents/test/ok", "existing"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatal("upstream replay during state import", calls.Load())
	}
}

func TestYieldedSourceRestoresLatestImageAndCandidateLease(t *testing.T) {
	a, cfg, target, identity := completeGateway(t, t.TempDir(), "A")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"ok":true}`)) }))
	defer up.Close()
	configureGateway(t, a, up.URL)
	c := shared.NewLocalClient(a.shared, "B")
	defer c.Close()
	var config, lease shared.Response
	if err := c.Call("config", shared.Request{}, &config); err != nil {
		t.Fatal(err)
	}
	route := config.Config.Routes[0]
	if err := c.Call("acquire", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Session: "live", Entry: "proxy"}, &lease); err != nil {
		t.Fatal(err)
	}
	d, err := a.PrepareHandoff(context.Background(), "h", "r", handoff.Identity{Incarnation: "B", Release: strings.Repeat("b", 64)}, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreGateway(context.Background(), cfg, target, identity, d, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if decision := restored.owner.Decision(); decision.Phase != handoff.Aborted || decision.Epoch != d.Epoch || restored.shared.Count("B") != 1 {
		t.Fatal("source lost ownership or candidate accounting", decision)
	}
	// A remote candidate reconnects to the source endpoint with its original
	// epoch; the exact source image must accept its original lease identifier.
	reconnected := shared.NewLocalClient(restored.shared, "B")
	defer reconnected.Close()
	if err = reconnected.Call("release", shared.Request{Lease: lease.Lease}, nil); err != nil || restored.shared.Count("") != 0 {
		t.Fatal("candidate could not release after source recovery", err)
	}
	if w := processRequest(restored, "/agents/test/ok", "live"); w.Code != 200 {
		t.Fatal("latest route/session lost", w.Code, w.Body.String())
	}
}

func TestYieldedSourceCannotRestoreAfterCommit(t *testing.T) {
	a, cfg, target, identity := completeGateway(t, t.TempDir(), "A")
	bID := p.Identity{Incarnation: "B", Release: strings.Repeat("b", 64), Instance: "b"}
	bTarget := p.Target{Incarnation: "B", Release: bID.Release, Instance: "b", URL: "http://gateway", Socket: filepath.Join(cfg.DataDir, "b.sock")}
	d, err := a.PrepareHandoff(context.Background(), "h", "r", handoff.Identity{Incarnation: "B", Release: bID.Release}, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	b, err := AdoptGateway(context.Background(), cfg, bTarget, bID, d, 4<<20, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = RestoreGateway(context.Background(), cfg, target, identity, d, 4<<20); !errors.Is(err, handoff.ErrCommitted) {
		t.Fatal("committed candidate was rolled back", err)
	}
}

func TestGatewayFailedPreparationRestoresSource(t *testing.T) {
	a, cfg, _, _ := completeGateway(t, t.TempDir(), "A")
	_, err := a.PrepareHandoff(context.Background(), "h", "r", handoff.Identity{Incarnation: "B", Release: strings.Repeat("b", 64)}, 1)
	if err == nil {
		t.Fatal("oversized snapshot accepted")
	}
	d, err := handoff.ReadDecision(cfg.DataDir)
	if err != nil || d.Phase != handoff.Aborted || a.shared.BarrierStatus().Sealed || !a.Ready() {
		t.Fatal("precommit failure did not retain owner", d, err)
	}
	client := shared.NewLocalClient(a.shared, "probe")
	defer client.Close()
	if err = client.Call("config", shared.Request{}, nil); err != nil {
		t.Fatal("source state unavailable after abort", err)
	}
}

func TestSnapshotStorageFailureAbortsAndAllowsFreshHandoff(t *testing.T) {
	a, cfg, _, _ := completeGateway(t, t.TempDir(), "A")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer up.Close()
	configureGateway(t, a, up.URL)
	path := filepath.Join(cfg.DataDir, "handoff", "snapshots")
	if err := os.WriteFile(path, []byte("unavailable snapshot directory"), 0600); err != nil {
		t.Fatal(err)
	}
	target := handoff.Identity{Incarnation: "B", Release: strings.Repeat("b", 64)}
	if _, err := a.PrepareHandoff(context.Background(), "failed", "r", target, 4<<20); err == nil {
		t.Fatal("snapshot storage failure allowed sealing")
	}
	d, err := handoff.ReadDecision(cfg.DataDir)
	if err != nil || d.Phase != handoff.Aborted || d.Snapshot != (handoff.Snapshot{}) || a.shared.BarrierStatus().Sealed || !a.Ready() {
		t.Fatal("snapshot failure did not restore source admission", d, err)
	}
	if w := processRequest(a, "/agents/test/ok", "after-storage-failure"); w.Code != http.StatusOK {
		t.Fatal("source could not mutate state after abort", w.Code)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	d, err = a.PrepareHandoff(context.Background(), "retry", "r2", target, 4<<20)
	if err != nil || d.Phase != handoff.Sealed {
		t.Fatal("fresh handoff could not seal the latest state", d, err)
	}
	raw, err := a.owner.LoadSnapshot("retry", 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateHandoff(raw, d, 4<<20, cfg.Options()); err != nil {
		t.Fatal("fresh handoff published invalid state", err)
	}
	if err = a.AbortHandoff("retry"); err != nil {
		t.Fatal(err)
	}
}

func TestYieldedCommitAndRollbackHaveExactlyOneWinner(t *testing.T) {
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			a, cfg, target, identity := completeGateway(t, t.TempDir(), "A")
			bID := p.Identity{Incarnation: "B", Release: strings.Repeat("b", 64), Instance: "b"}
			bTarget := p.Target{Incarnation: bID.Incarnation, Release: bID.Release, Instance: bID.Instance, URL: "http://gateway", Socket: filepath.Join(cfg.DataDir, "b.sock")}
			d, err := a.PrepareHandoff(context.Background(), "race", "r", handoff.Identity{Incarnation: bID.Incarnation, Release: bID.Release}, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.YieldHandoff(d); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var restored, adopted *Gateway
			var restoreErr, adoptErr error
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				restored, restoreErr = RestoreGateway(context.Background(), cfg, target, identity, d, 4<<20)
			}()
			go func() {
				defer wg.Done()
				<-start
				adopted, adoptErr = AdoptGateway(context.Background(), cfg, bTarget, bID, d, 4<<20, nil, nil)
			}()
			close(start)
			wg.Wait()
			if restored != nil {
				defer restored.Close()
			}
			if adopted != nil {
				defer adopted.Close()
			}
			if (restoreErr == nil) == (adoptErr == nil) {
				t.Fatal("commit and rollback did not have exactly one winner", restoreErr, adoptErr)
			}
			actual, err := handoff.ReadDecision(cfg.DataDir)
			if err != nil {
				t.Fatal(err)
			}
			if restoreErr == nil && (actual.Phase != handoff.Aborted || actual.Owner != d.Source || !restored.Ready()) {
				t.Fatal("restored source differs from durable decision", actual)
			}
			if adoptErr == nil && (actual.Phase != handoff.Committed || actual.Owner != d.Target || !adopted.Ready()) {
				t.Fatal("adopted owner differs from durable decision", actual)
			}
		})
	}
}
