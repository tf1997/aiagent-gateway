package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
)

func TestDynamicConsumerCountersConcurrentRotation(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				l, err := g.acquire("alice", "test", "", "", "proxy", nil)
				if err == nil {
					l.release()
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		w := admin(t, g, "POST", "/admin/v1/routes", fmt.Sprintf(`{"key":"test","consumers":[{"id":"alice","token":"token-%d"}]}`, i), "", "")
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	wg.Wait()
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	if len(g.callerCounts) != 0 || g.inflight.Load() != 0 {
		t.Fatal("consumer counters leaked")
	}
}

func TestConsumerRotationDoesNotResetInflightLimit(t *testing.T) {
	g := setup(t)
	create(t, g, "proxy", "http://localhost:1")
	g.opts.MaxPerCaller = 1
	l, err := g.acquire("alice", "test", "", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.release()
	if w := admin(t, g, "POST", "/admin/v1/routes", `{"key":"test","consumers":[{"id":"alice","token":"rotated"}]}`, "", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if next, err := g.acquire("alice", "test", "", "", "proxy", nil); err == nil {
		next.release()
		t.Fatal("rotation reset inflight capacity")
	}
	l.release()
	next, err := g.acquire("alice", "test", "", "", "proxy", nil)
	if err != nil {
		t.Fatal("capacity not released", err)
	}
	next.release()
}

func TestStickyConcurrentDrain(t *testing.T) {
	g := setup(t)
	rt := create(t, g, "proxy", "http://localhost:1", "http://localhost:2")
	var wg sync.WaitGroup
	ids := make(chan string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := g.acquire("alice", "test", "same", "", "proxy", nil)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- l.ep.ID
			l.release()
		}()
	}
	wg.Wait()
	close(ids)
	chosen := ""
	for id := range ids {
		if chosen != "" && chosen != id {
			t.Fatal("split binding")
		}
		chosen = id
	}
	path := "/admin/v1/routes/" + rt.ID + "/endpoints/" + chosen
	w := admin(t, g, "PATCH", path, `{"state":"draining"}`, `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	l, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil || l.ep.ID != chosen {
		t.Fatal("drain broke affinity", err)
	}
	l.release()
	other, err := g.acquire("alice", "test", "new", "", "proxy", nil)
	if err != nil || other.ep.ID == chosen {
		t.Fatal("new session sent to draining")
	}
	other.release()
	if admin(t, g, "DELETE", path, "", `"2"`, "").Code != 409 {
		t.Fatal("deleted bound endpoint")
	}
	g.state(rt.ID, chosen).proxy.healthy.Store(false)
	if l, err := g.acquire("alice", "test", "same", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("failed endpoint silently reselected")
	}
}

func TestSessionCapacityAndTTL(t *testing.T) {
	g := setup(t)
	g.opts.MaxSessions = 1
	create(t, g, "proxy", "http://localhost:1")
	l, err := g.acquire("alice", "test", "a", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
	if l, err := g.acquire("alice", "test", "b", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("capacity ignored")
	}
	key := sessionKey{"alice", g.current.Load().Routes["test"].ID, "a", "proxy"}
	sh := g.shard(key)
	sh.mu.Lock()
	sh.items[key].Seen = time.Now().Add(-48 * time.Hour)
	sh.mu.Unlock()
	l, err = g.acquire("alice", "test", "a", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	l.release()
}

func BenchmarkBoundSession(b *testing.B) {
	g, err := Open(testOptions(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	r := protocol.Route{ID: "rt", Key: "test", AgentType: "generic", Expose: []string{"proxy"}, Enabled: true, Endpoints: []protocol.Endpoint{{ID: "ep", Interfaces: map[string]protocol.EndpointInterface{"proxy": {URL: "http://localhost:1"}}, State: "active", Weight: 100}}}
	configuration.Defaults(&r)
	g.current.Store(configuration.Build(protocol.Config{Schema: 2, Routes: []protocol.Route{r}}))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := g.acquire("alice", "test", "session", "", "proxy", nil)
			if err != nil {
				b.Error(err)
				return
			}
			l.release()
		}
	})
}

func TestCallerIsolationAndLimits(t *testing.T) {
	g := setup(t)
	g.opts.MaxPerCaller = 1
	create(t, g, "proxy", "http://localhost:1", "http://localhost:2")
	a, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := g.acquire("alice", "test", "other", "", "proxy", nil); err == nil {
		extra.release()
		t.Fatal("caller concurrency limit ignored")
	}
	b, err := g.acquire("bob", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.ep.ID == b.ep.ID {
		t.Fatal("owners shared session binding")
	}
	a.release()
	b.release()
	if g.inflight.Load() != 0 || len(g.callerCounts) != 0 {
		t.Fatal("concurrency slot leaked")
	}
}

func TestInterfaceHealthAndSharedCapacity(t *testing.T) {
	var failProxy atomic.Bool
	failProxy.Store(true)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer health-key" {
			t.Error("health request missing credential")
		}
		if r.URL.Path == "/proxy-health" && failProxy.Load() {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/ok")
			w.WriteHeader(302)
			return
		}
		w.WriteHeader(200)
	}))
	defer up.Close()
	g := setup(t)
	rt := createDual(t, g, map[string]protocol.EndpointInterface{"proxy": {URL: up.URL, HealthURL: up.URL + "/proxy-health", Token: "health-key"}, "a2a": {URL: up.URL + "/rpc", HealthURL: up.URL + "/a2a-health", Token: "health-key", ProtocolVersion: "1.0"}})
	g.checkHealth(context.Background())
	if l, err := g.acquire("alice", "test", "new", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("unhealthy proxy selected")
	}
	a, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		t.Fatal("proxy failure disabled A2A", err)
	}
	failProxy.Store(false)
	g.checkHealth(context.Background())
	g.opts.MaxPerEndpoint = 1
	if l, err := g.acquire("alice", "test", "same", "", "proxy", nil); err == nil {
		l.release()
		t.Fatal("endpoint concurrency not shared")
	}
	a.release()
	g.opts.MaxPerEndpoint = 2
	p, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err = g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		p.release()
		t.Fatal(err)
	}
	detail := admin(t, g, "GET", "/admin/v1/routes/"+rt.ID+"/endpoints/"+rt.Endpoints[0].ID, "", "", "")
	var result struct {
		Runtime struct {
			Inflight   int `json:"http_inflight"`
			Interfaces map[string]struct {
				Inflight int `json:"http_inflight"`
			} `json:"interfaces"`
		} `json:"runtime"`
	}
	if err := json.Unmarshal(detail.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Runtime.Inflight != 2 || result.Runtime.Interfaces["proxy"].Inflight != 1 || result.Runtime.Interfaces["a2a"].Inflight != 1 {
		t.Fatal(detail.Body.String())
	}
	p.release()
	a.release()
	if g.inflight.Load() != 0 || g.state(rt.ID, rt.Endpoints[0].ID).active.Load() != 0 {
		t.Fatal("counter leak")
	}
	epPath := "/admin/v1/routes/" + rt.ID + "/endpoints/" + rt.Endpoints[0].ID
	w := admin(t, g, "PATCH", epPath, fmt.Sprintf(`{"interfaces":{"proxy":{"health_url":%q}}}`, up.URL+"/redirect"), `"1"`, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	g.checkHealth(context.Background())
	if g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Load() {
		t.Fatal("redirect reported healthy")
	}
	w = admin(t, g, "PATCH", epPath, `{"interfaces":{"proxy":{"health_url":null}}}`, `"2"`, "")
	if w.Code != 200 || !g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Load() {
		t.Fatal("removed health URL left stale failure", w.Body.String())
	}
}

func TestInterfaceBindingsReferencesAndPatches(t *testing.T) {
	g := setup(t)
	rt := createDual(t, g, localInterfaces(), localInterfaces())
	g.state(rt.ID, rt.Endpoints[0].ID).proxy.healthy.Store(false)
	p, err := g.acquire("alice", "test", "same", "", "proxy", nil)
	if err != nil {
		t.Fatal(err)
	}
	p.release()
	a, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil {
		t.Fatal(err)
	}
	a.release()
	if p.ep.ID == a.ep.ID {
		t.Fatal("proxy and A2A shared a session namespace")
	}
	epPath := "/admin/v1/routes/" + rt.ID + "/endpoints/" + a.ep.ID
	routePath := "/admin/v1/routes/" + rt.ID
	for _, body := range []string{`{"interfaces":{"a2a":{"url":"http://localhost:99"}}}`, `{"interfaces":{"a2a":{"tenant":"changed"}}}`, `{"interfaces":{"a2a":null}}`} {
		if w := admin(t, g, "PATCH", epPath, body, `"1"`, ""); w.Code != 409 {
			t.Fatal("changed bound endpoint", w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{"expose":["proxy"]}`, `{"agent_type":"hermes"}`, `{"key":"other"}`} {
		if w := admin(t, g, "PATCH", routePath, body, `"1"`, ""); w.Code != 409 {
			t.Fatal("changed bound route", w.Code, w.Body.String())
		}
	}
	if w := admin(t, g, "PATCH", epPath, `{"url":"http://localhost:99"}`, `"1"`, ""); w.Code != 409 {
		t.Fatal("ambiguous legacy endpoint patch accepted")
	}
	if w := admin(t, g, "PATCH", routePath, `{"protocol":"proxy"}`, `"1"`, ""); w.Code != 409 {
		t.Fatal("ambiguous legacy route patch accepted")
	}
	if w := admin(t, g, "PATCH", epPath, `{"state":"draining","weight":50}`, `"1"`, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	bound, err := g.acquire("alice", "test", "same", "", "a2a", nil)
	if err != nil || bound.ep.ID != a.ep.ID {
		t.Fatal("drain broke binding", err)
	}
	bound.release()
	fresh, err := g.acquire("alice", "test", "fresh", "", "a2a", nil)
	if err != nil || fresh.ep.ID == a.ep.ID {
		t.Fatal("draining accepted new session", err)
	}
	fresh.release()
	old := g.current.Load().Routes["test"]
	w := admin(t, g, "PATCH", routePath, `{"allowed_methods":["GET"]}`, configuration.Tag(old.Version), "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if l, err := g.acquire("alice", "test", "stale", "", "a2a", old); err == nil {
		l.release()
		t.Fatal("stale route accepted")
	}
}
