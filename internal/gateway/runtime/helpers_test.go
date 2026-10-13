package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/protocol"
)

func rpcBody(method string, params map[string]any) string {
	b, _ := json.Marshal(a2a.RPC{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: method, Params: params})
	return string(b)
}

func requireRPCCode(t *testing.T, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	var env struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(w.Body.Bytes(), &env) != nil || env.Error.Code != code {
		t.Fatalf("want RPC %d: %s", code, w.Body.String())
	}
}

func consumerCall(g *Gateway, path, token, session string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Session-ID", session)
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}

func testOptions(dir string) Options {
	return Options{DataDir: dir, AdminToken: "admin-secret", dialContext: testDial, PublicURL: "http://localhost:8080"}
}

func testConsumers() []map[string]string {
	return []map[string]string{{"id": "alice", "token": "alice-secret"}, {"id": "bob", "token": "bob-secret"}}
}

func setup(t *testing.T) *Gateway {
	t.Helper()
	g, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func admin(t *testing.T, g *Gateway, method, path, body, etag, idem string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer admin-secret")
	r.Header.Set("If-Match", etag)
	r.Header.Set("Idempotency-Key", idem)
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, r)
	return w
}

func create(t *testing.T, g *Gateway, entry string, urls ...string) protocol.Route {
	t.Helper()
	eps := []map[string]any{}
	for _, u := range urls {
		eps = append(eps, map[string]any{"url": u, "weight": 100})
	}
	v := map[string]any{"consumers": testConsumers(), "key": "test", "protocol": entry, "endpoints": eps}
	if entry == "a2a" {
		v["agent_card"] = map[string]any{"name": "test", "description": "test", "capabilities": map[string]any{"streaming": true}, "version": "1", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}}
	}
	raw, _ := json.Marshal(v)
	w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", "")
	if w.Code != 201 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	var rt protocol.Route
	json.Unmarshal(w.Body.Bytes(), &rt)
	return rt
}

const sendV1 = `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`

func createDual(t *testing.T, g *Gateway, eps ...map[string]protocol.EndpointInterface) protocol.Route {
	t.Helper()
	endpoints := []map[string]any{}
	for _, ep := range eps {
		endpoints = append(endpoints, map[string]any{"interfaces": ep})
	}
	raw, _ := json.Marshal(map[string]any{"consumers": testConsumers(), "key": "test", "expose": []string{"proxy", "a2a"}, "endpoints": endpoints, "agent_card": map[string]any{"name": "test", "description": "test", "version": "1", "skills": []any{}, "defaultInputModes": []string{"text/plain"}, "defaultOutputModes": []string{"text/plain"}, "capabilities": map[string]any{"streaming": true}, "supportedInterfaces": []any{map[string]any{"url": "http://internal:9900"}}}})
	w := admin(t, g, "POST", "/admin/v1/routes", string(raw), "", "")
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var rt protocol.Route
	if err := json.Unmarshal(w.Body.Bytes(), &rt); err != nil {
		t.Fatal(err)
	}
	return rt
}

func dataRequest(g *Gateway, method, path, body, caller, sid string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if caller != "" {
		r.Header.Set("Authorization", "Bearer "+caller+"-secret")
	}
	if sid != "" {
		r.Header.Set("X-Session-ID", sid)
	}
	r.Header.Set("A2A-Version", "1.0")
	r.Header.Set("Cookie", "untrusted=1")
	w := httptest.NewRecorder()
	g.DataHandler().ServeHTTP(w, r)
	return w
}

func localInterfaces() map[string]protocol.EndpointInterface {
	return map[string]protocol.EndpointInterface{"proxy": {URL: "http://localhost:1"}, "a2a": {URL: "http://localhost:2/rpc", ProtocolVersion: "1.0"}}
}

func writeTestTask(w http.ResponseWriter, q a2a.RPC, task map[string]any) {
	var result any = task
	if q.Method == "SendMessage" {
		result = map[string]any{"task": task}
	}
	protocol.WriteJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": result})
}

var memRegistry sync.Map

var memSequence atomic.Int64

type testServer struct {
	URL   string
	close func()
}

func (s *testServer) Close() { s.close() }

type pipeListener struct {
	incoming chan net.Conn
	done     chan struct{}
	once     sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error { l.once.Do(func() { close(l.done) }); return nil }

func (l *pipeListener) Addr() net.Addr { return pipeAddr("memory") }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }

func (a pipeAddr) String() string { return string(a) }

func newTestServer(h http.Handler) *testServer {
	if os.Getenv("GATEWAY_TEST_IN_MEMORY") != "true" {
		s := httptest.NewServer(h)
		return &testServer{s.URL, s.Close}
	}
	host := fmt.Sprintf("test-%d.invalid:80", memSequence.Add(1))
	l := &pipeListener{incoming: make(chan net.Conn), done: make(chan struct{})}
	memRegistry.Store(host, l)
	srv := &http.Server{Handler: h}
	go srv.Serve(l)
	return &testServer{"http://" + host, func() { memRegistry.Delete(host); srv.Close() }}
}

func testDial(ctx context.Context, network, address string) (net.Conn, error) {
	if v, ok := memRegistry.Load(address); ok {
		l := v.(*pipeListener)
		a, b := net.Pipe()
		select {
		case l.incoming <- b:
			return a, nil
		case <-ctx.Done():
			a.Close()
			b.Close()
			return nil, ctx.Err()
		case <-l.done:
			a.Close()
			b.Close()
			return nil, net.ErrClosed
		}
	}
	return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
}

func testClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: testDial}, Timeout: 3 * time.Second}
}
