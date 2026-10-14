package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/protocol"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

func TestHermesUpstreamErrorDetails(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"json", `{"error":{"message":"model unavailable","code":"model_not_found","api_key":"provider-secret"},"request_id":"req-123"}`, 422},
		{"text", "provider overloaded; retry later", 503},
		{"empty", "", 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer up.Close()
			g := setup(t)
			createHermesRoute(t, g, up.URL)
			w := hermesCall(g, "GET", "/v1/models", "", "alice")
			var response struct {
				Error    string `json:"error"`
				Upstream struct {
					Status int             `json:"status"`
					Body   json.RawMessage `json:"body"`
				} `json:"upstream"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != tc.status || response.Error != "HERMES_UPSTREAM_ERROR" || response.Upstream.Status != tc.status {
				t.Fatalf("missing upstream status: %d %s (%v)", w.Code, w.Body.String(), err)
			}
			if tc.name == "json" && (!strings.Contains(string(response.Upstream.Body), "model unavailable") || !strings.Contains(string(response.Upstream.Body), "req-123") || strings.Contains(w.Body.String(), "provider-secret")) {
				t.Fatal("missing details or exposed credentials", w.Body.String())
			}
			if tc.name == "text" && !strings.Contains(string(response.Upstream.Body), tc.body) {
				t.Fatal("lost upstream text", w.Body.String())
			}
		})
	}
}

func TestA2AUpstreamErrorDetails(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		streaming bool
	}{
		{"rpc", 200, false},
		{"http_rpc", 429, false},
		{"stream", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"provider quota exceeded","data":{"retry_after":30,"request_id":"req-456","token":"provider-secret"}}}`
				if tc.streaming {
					w.Header().Set("Content-Type", "text/event-stream")
					body = sse(body)
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, body)
			}))
			defer up.Close()
			g := setup(t)
			create(t, g, "a2a", up.URL)
			body := sendV1
			if tc.streaming {
				body = strings.Replace(body, "SendMessage", "SendStreamingMessage", 1)
			}
			w := dataRequest(g, "POST", "/agents/test/a2a", body, "alice", "")
			if w.Code != 200 || !strings.Contains(w.Body.String(), "provider quota exceeded") || !strings.Contains(w.Body.String(), `"retry_after":30`) || !strings.Contains(w.Body.String(), "req-456") || strings.Contains(w.Body.String(), "provider-secret") {
				t.Fatal("upstream RPC error details lost", w.Code, w.Body.String())
			}
		})
	}
}

func TestA2AUpstreamHTTPErrorDetails(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		fmt.Fprint(w, "model backend is overloaded")
	}))
	defer up.Close()
	g := setup(t)
	create(t, g, "a2a", up.URL)
	w := dataRequest(g, "POST", "/agents/test/a2a", sendV1, "alice", "")
	requireRPCCode(t, w, -32603)
	if !strings.Contains(w.Body.String(), `"status":503`) || !strings.Contains(w.Body.String(), "model backend is overloaded") {
		t.Fatal("lost upstream HTTP error", w.Body.String())
	}
}

func TestProxyTransportErrorDetails(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	up.Close()
	g := setup(t)
	create(t, g, "proxy", up.URL)
	w := dataRequest(g, "GET", "/agents/test/chat", "", "alice", "session")
	if w.Code != 502 || !strings.Contains(w.Body.String(), "UPSTREAM_UNAVAILABLE") || !strings.Contains(w.Body.String(), "connection refused") {
		t.Fatal("missing transport cause", w.Code, w.Body.String())
	}
}

func TestHermesImmediateUpstreamErrors(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.WriteHeader(404)
					return
				}
				body := `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"provider quota exceeded","data":{"retry_after":60}}}`
				if streaming {
					w.Header().Set("Content-Type", "text/event-stream")
					body = sse(body)
				} else {
					w.WriteHeader(429)
				}
				fmt.Fprint(w, body)
			}))
			defer up.Close()
			g := setup(t)
			rt := create(t, g, "a2a", up.URL)
			if w := admin(t, g, "PATCH", "/admin/v1/routes/test", `{"agent_type":"hermes"}`, etag(rt.Version), ""); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			body := strings.Replace(sendV1, `"message":`, `"configuration":{"returnImmediately":true},"message":`, 1)
			w := dataRequest(g, "POST", "/agents/test/a2a", body, "alice", "")
			requireRPCCode(t, w, -32603)
			if !strings.Contains(w.Body.String(), "provider quota exceeded") || !strings.Contains(w.Body.String(), `"retry_after":60`) {
				t.Fatal("immediate error details lost", w.Body.String())
			}
			waitFor(t, time.Second, "failed immediate request release", func() bool { return metric(t, g, "gateway_inflight") == 0 })
		})
	}
}

func TestListUpstreamErrorDetails(t *testing.T) {
	t.Run("hermes", func(t *testing.T) {
		fake := newHermesFake()
		up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/sessions/") {
				w.WriteHeader(503)
				fmt.Fprint(w, "session store unavailable")
				return
			}
			fake.ServeHTTP(w, r)
		}))
		defer up.Close()
		g := setup(t)
		createHermesRoute(t, g, up.URL)
		hermesCreated(t, g, "alice", "first")
		w := hermesCall(g, "GET", "/api/sessions", "", "alice")
		if w.Code != 502 || !strings.Contains(w.Body.String(), "session store unavailable") || !strings.Contains(w.Body.String(), `"status":503`) {
			t.Fatal("session list details lost", w.Code, w.Body.String())
		}
	})
	t.Run("a2a", func(t *testing.T) {
		up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var q rpcMessage
			json.NewDecoder(r.Body).Decode(&q)
			if q.Method == "SendMessage" {
				writeTestTask(w, q, map[string]any{"id": requestedTask(q), "contextId": "ctx", "status": map[string]any{"state": "TASK_STATE_WORKING"}})
				return
			}
			protocol.WriteJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": q.ID, "error": map[string]any{"code": -32603, "message": "task store unavailable", "data": map[string]any{"retry_after": 10}}})
		}))
		defer up.Close()
		g := setup(t)
		create(t, g, "a2a", up.URL)
		sendA2ATask(t, g, "alice", "first")
		w := dataRequest(g, "POST", "/agents/test/a2a", rpcBody("ListTasks", map[string]any{}), "alice", "")
		requireRPCCode(t, w, -32603)
		if !strings.Contains(w.Body.String(), "task store unavailable") || !strings.Contains(w.Body.String(), `"retry_after":10`) {
			t.Fatal("task list details lost", w.Body.String())
		}
	})
}

func TestHermesImmediateLaterErrorRetainsUnknownTask(t *testing.T) {
	finish := make(chan struct{}, 1)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sse(firstTask))
		w.(http.Flusher).Flush()
		select {
		case <-finish:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, sse(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"provider lost connection"}}`))
	}))
	defer up.Close()
	defer close(finish)
	g := setup(t)
	rt := create(t, g, "a2a", up.URL)
	if w := admin(t, g, "PATCH", "/admin/v1/routes/test", `{"agent_type":"hermes"}`, etag(rt.Version), ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	body := strings.Replace(sendV1, `"message":`, `"configuration":{"returnImmediately":true},"message":`, 1)
	w := dataRequest(g, "POST", "/agents/test/a2a", body, "alice", "")
	var response struct {
		Result struct {
			Task struct{ ID string } `json:"task"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Result.Task.ID == "" {
		t.Fatal("initial task missing", w.Body.String())
	}
	finish <- struct{}{}
	waitFor(t, time.Second, "failed background request release", func() bool { return metric(t, g, "gateway_inflight") == 0 })
	ref := readResource(t, g, protocol.ResourceKey{Owner: "alice", Route: rt.ID, Kind: protocol.A2ATask, ID: response.Result.Task.ID})
	if ref.State != protocol.ResourceUnknown {
		t.Fatal("failed stream lost uncertain task state", ref.State)
	}
}

func TestHermesErrorBodyIdleTimeout(t *testing.T) {
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(503)
		fmt.Fprint(w, "provider overloaded")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	g := setup(t)
	rt := createHermesRoute(t, g, up.URL)
	if w := admin(t, g, "PATCH", "/admin/v1/routes/test", `{"timeout":{"stream_idle_ms":50,"max_duration_ms":1000}}`, etag(rt.Version), ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w := hermesCall(g, "GET", "/v1/models", "", "alice")
	var response struct {
		Upstream struct {
			Body    string `json:"body"`
			Message string `json:"message"`
		} `json:"upstream"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 503 || response.Upstream.Body != "provider overloaded" || response.Upstream.Message == "" {
		t.Fatal("partial error response lost", w.Code, w.Body.String())
	}
}

func TestSharedWorkerUpstreamErrorDetails(t *testing.T) {
	f := sharedSetup(t)
	up := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		fmt.Fprint(w, `{"error":{"message":"model not found"}}`)
	}))
	defer up.Close()
	createHermesRoute(t, f.core, up.URL)
	worker := f.worker(t, "error-worker")
	server := newTestServer(worker.WorkerHandler(p.Identity{Incarnation: "error-worker", Release: "test"}))
	defer server.Close()
	j, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	front, err := entry.Open(j, "admin-secret", func(ctx context.Context, q p.Request) (p.Resolution, error) {
		return gateway.ResolveShared(f.socket, "admin-secret", q)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := front.Apply(0, p.Traffic{Revision: 1, Stable: p.Target{Instance: "a", Release: "test", Incarnation: "error-worker", URL: server.URL}}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/agents/test/v1/models", nil)
	req.Header.Set("Authorization", "Bearer alice-secret")
	w := httptest.NewRecorder()
	front.ServeHTTP(w, req)
	if w.Code != 422 || !strings.Contains(w.Body.String(), "model not found") || !strings.Contains(w.Body.String(), `"status":422`) {
		t.Fatal("entry/worker dropped error details", w.Code, w.Body.String())
	}
}
