package resource

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

func directoryTest() (*Directory, *atomic.Int64) {
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	d := New(Limits{MaxResources: 100, MaxResourceAliases: 100, MaxDataIdempotency: 100, MaxSessions: 100, MaxTasks: 100}, nil)
	d.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return d, clock
}

func reserveTest(t *testing.T, d *Directory, owner string, kind protocol.ResourceKind, alias, parent string, parentKind protocol.ResourceKind, idem string) protocol.CreationReservation {
	t.Helper()
	r, err := d.Reserve(owner, "route", protocol.ResourceCreation{Kind: kind, Alias: alias, ParentKind: parentKind, ParentID: parent, Operation: string(kind) + "/create", IdempotencyKey: idem, Fingerprint: "fingerprint"}, func(parent *protocol.Resource) (string, error) {
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

func updateTest(t *testing.T, d *Directory, r protocol.Resource, state protocol.ResourceState) {
	t.Helper()
	if err := d.Update(r, state); err != nil {
		t.Fatal(err)
	}
}

func cleanTest(d *Directory) { d.Cleanup(func(protocol.Resource) bool { return false }) }

func TestResourceLineageAndAliasConflicts(t *testing.T) {
	d, _ := directoryTest()
	parent := reserveTest(t, d, "alice", protocol.HermesSession, "original", "", "", "").Resource
	updateTest(t, d, parent, protocol.ResourceLive)
	child := reserveTest(t, d, "alice", protocol.HermesSession, "fork", parent.ID, protocol.HermesSession, "").Resource
	if child.EndpointID != parent.EndpointID || child.ParentID != parent.ID || child.Upstream == parent.Upstream {
		t.Fatal("fork changed instance/identity", child)
	}
	if err := d.AttachAlias(parent, "compacted", true); err != nil {
		t.Fatal(err)
	}
	if err := d.AttachAlias(parent, "public-lineage", false); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"original", "public-lineage", parent.ID} {
		r, err := d.Lookup(protocol.ResourceKey{Owner: "alice", Route: "route", Kind: protocol.HermesSession, ID: alias})
		if err != nil || r.ID != parent.ID || r.Upstream != "compacted" {
			t.Fatal(alias, r, err)
		}
	}
	if err := d.AttachAlias(child, "compacted", true); !errors.Is(err, protocol.ErrResourceConflict) {
		t.Fatal("upstream alias collision accepted", err)
	}
	if err := d.AttachAlias(child, "original", false); !errors.Is(err, protocol.ErrResourceConflict) {
		t.Fatal("client alias collision accepted", err)
	}
	if err := d.AttachAlias(child, parent.ID, false); !errors.Is(err, protocol.ErrResourceConflict) {
		t.Fatal("canonical ID shadowed", err)
	}
	if err := d.Forget(parent); !errors.Is(err, protocol.ErrResourceInUse) {
		t.Fatal("parent lost with live fork", err)
	}
	if err := d.Abandon(child); err != nil {
		t.Fatal(err)
	}
	if err := d.Forget(parent); err != nil {
		t.Fatal(err)
	}
	if d.aliasCount != 0 || len(d.aliases) != 0 || len(d.reverse) != 0 || len(d.items) != 0 {
		t.Fatal("lineage indexes leaked")
	}
	stale := parent
	stale.Generation = "obsolete"
	if err := d.AttachAlias(stale, "new", true); !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal("stale generation accepted")
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
			first := reserveTest(t, d, "alice", protocol.HermesSession, "one", "", "", "idem-one")
			called := false
			_, err := d.Reserve("alice", "route", protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "two", Operation: "sessions/create", IdempotencyKey: "idem-two", Fingerprint: "two"}, func(*protocol.Resource) (string, error) { called = true; return "other", nil })
			if !errors.Is(err, protocol.ErrResourceCapacity) || called || len(d.items) != 1 || len(d.operations) != 1 || d.aliasCount != 1 {
				t.Fatal("capacity failure left partial reservation", err, called)
			}
			if err := d.Abandon(first.Resource); err != nil {
				t.Fatal(err)
			}
			if len(d.items) != 0 || len(d.operations) != 0 || len(d.reverse) != 0 || len(d.aliases) != 0 {
				t.Fatal("abandoned reservation leaked indexes")
			}
			second := reserveTest(t, d, "alice", protocol.HermesSession, "one", "", "", "idem-one")
			if first.Resource.Generation == second.Resource.Generation || first.Resource.ID == second.Resource.ID {
				t.Fatal("new generation reused abandoned identity")
			}
			if err := d.Update(first.Resource, protocol.ResourceUnknown); !errors.Is(err, protocol.ErrResourceNotFound) {
				t.Fatal("late old response affected new resource")
			}
			updateTest(t, d, second.Resource, protocol.ResourceUnknown)
			if err := d.Abandon(second.Resource); !errors.Is(err, protocol.ErrResourceInUse) {
				t.Fatal("uncertain request abandoned")
			}
			if err := d.Update(second.Resource, protocol.ResourceReserved); !errors.Is(err, protocol.ErrResourceConflict) {
				t.Fatal("uncertain resource returned to unsent state")
			}
		})
	}
}

func TestResourceLifecycleRetentionAndCleanup(t *testing.T) {
	d, clock := directoryTest()
	parent := reserveTest(t, d, "alice", protocol.HermesSession, "session", "", "", "").Resource
	updateTest(t, d, parent, protocol.ResourceLive)
	run := reserveTest(t, d, "alice", protocol.HermesRun, "run", parent.ID, protocol.HermesSession, "run-key").Resource
	if err := d.AttachAlias(run, "upstream-run", true); err != nil {
		t.Fatal(err)
	}
	updateTest(t, d, run, protocol.ResourceUnknown)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 || len(d.operations) != 1 {
		t.Fatal("unknown run lost ownership/parent/idempotency")
	}
	updateTest(t, d, run, protocol.ResourceWaiting)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 {
		t.Fatal("approval wait expired")
	}
	updateTest(t, d, run, protocol.ResourceLive)
	clock.Add(int64(48 * time.Hour))
	cleanTest(d)
	if len(d.items) != 2 {
		t.Fatal("active run expired")
	}
	updateTest(t, d, run, protocol.ResourceTerminal)
	pinned, release, err := d.Pin(run.Key())
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
	for _, key := range []protocol.ResourceKey{
		parent.Key(), run.Key(),
		{Owner: "alice", Route: "route", Kind: protocol.HermesSession, ID: "session"},
		{Owner: "alice", Route: "route", Kind: protocol.HermesRun, ID: "run"},
	} {
		if _, err := d.Lookup(key); !errors.Is(err, protocol.ErrResourceNotFound) {
			t.Fatal("expired alias still resolves", key, err)
		}
	}
}

func TestDataFingerprintAndIdempotencyNamespace(t *testing.T) {
	h := http.Header{"X-Hermes-Session-Key": []string{"session-a"}}
	a := protocol.DataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "X-Hermes-Session-Key")
	if a != protocol.DataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "x-hermes-session-key") {
		t.Fatal("header casing changes fingerprint")
	}
	h.Set("X-Hermes-Session-Key", "session-b")
	if a == protocol.DataFingerprint("POST", "/v1/runs", []byte(`{"input":"hello"}`), h, "X-Hermes-Session-Key") {
		t.Fatal("semantic header omitted from fingerprint")
	}
	if a == protocol.DataFingerprint("POST", "/v1/runs", []byte(`{"input":"different"}`), nil, "X-Hermes-Session-Key") {
		t.Fatal("body omitted from fingerprint")
	}
	keys := map[string]bool{}
	for _, parts := range [][4]string{{"a", "bc", "run", "key"}, {"ab", "c", "run", "key"}, {"a", "bc", "response", "key"}, {"a", "bc", "run", "other-key"}} {
		k := protocol.NamespacedIdempotency(parts[0], parts[1], parts[2], parts[3])
		if keys[k] || len(k) > 256 {
			t.Fatal("idempotency namespace collision")
		}
		keys[k] = true
	}
}

func TestResponseChainParentsAndFingerprintHeaderValues(t *testing.T) {
	d, _ := directoryTest()
	first := reserveTest(t, d, "alice", protocol.HermesResponse, "conversation", "", "", "").Resource
	if first.Upstream != "" {
		t.Fatal("server-generated response ID invented before response")
	}
	if err := d.AttachAlias(first, "resp-upstream-1", true); err != nil {
		t.Fatal(err)
	}
	updateTest(t, d, first, protocol.ResourceTerminal)
	next := reserveTest(t, d, "alice", protocol.HermesResponse, "", first.ID, protocol.HermesResponse, "").Resource
	if next.EndpointID != first.EndpointID || next.ParentID != first.ID {
		t.Fatal("response continuation lost origin")
	}
	if err := d.Forget(first); !errors.Is(err, protocol.ErrResourceInUse) {
		t.Fatal("response parent discarded")
	}
	_, err := d.Reserve("bob", "route", protocol.ResourceCreation{Kind: protocol.HermesResponse, ParentKind: protocol.HermesResponse, ParentID: first.ID, Operation: "responses/create", Fingerprint: "new"}, func(*protocol.Resource) (string, error) {
		t.Fatal("foreign parent triggered scheduling")
		return "", nil
	})
	if !errors.Is(err, protocol.ErrResourceNotFound) {
		t.Fatal(err)
	}
	h := http.Header{"X-Hermes-Session-Key": []string{"a"}}
	before := protocol.DataFingerprint("POST", "/v1/runs", nil, h, "X-Hermes-Session-Key")
	h.Add("X-Hermes-Session-Key", "b")
	if before == protocol.DataFingerprint("POST", "/v1/runs", nil, h, "X-Hermes-Session-Key") {
		t.Fatal("additional semantic header value omitted")
	}
}
