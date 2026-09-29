package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

func (g *Gateway) transport(ms int) *http.Transport {
	g.transportMu.Lock()
	defer g.transportMu.Unlock()
	t := g.transports[ms]
	if t == nil {
		t = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, MaxIdleConns: 512, MaxIdleConnsPerHost: 64, MaxConnsPerHost: 256, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: time.Duration(ms) * time.Millisecond}
		if g.opts.dialContext != nil {
			t.DialContext = g.opts.dialContext
			t.Proxy = nil
		}
		g.transports[ms] = t
	}
	return t
}
func (g *Gateway) DataHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			writeJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		if r.URL.Path == "/readyz" {
			code := 200
			if !g.healthy.Load() {
				code = 503
			}
			writeJSON(w, code, map[string]bool{"ready": code == 200})
			return
		}
		g.requests.Add(1)
		var caller *Caller
		for i := range g.opts.Callers {
			if tokenOK(r.Header.Get("Authorization"), g.opts.Callers[i].Token) {
				caller = &g.opts.Callers[i]
				break
			}
		}
		if caller == nil {
			problem(w, 401, "UNAUTHORIZED")
			return
		}
		p := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
		if len(p) < 2 {
			problem(w, 404, "NOT_FOUND")
			return
		}
		key := p[1]
		rt := g.current.Load().Routes[key]
		if rt == nil || !rt.Enabled {
			problem(w, 404, "ROUTE_NOT_FOUND")
			return
		}
		if !allowed(*caller, key) {
			problem(w, 403, "FORBIDDEN")
			return
		}
		if p[0] == "agents" && len(p) == 3 && p[2] == ".well-known/agent-card.json" && r.Method == "GET" {
			g.card(w, r, rt)
			return
		}
		if p[0] == "a2a" && len(p) == 2 && rt.Protocol == "a2a" {
			g.a2a(w, r, *caller, rt)
			return
		}
		if p[0] != "proxy" || rt.Protocol != "proxy" {
			problem(w, 404, "NOT_FOUND")
			return
		}
		if len(rt.AllowedMethods) > 0 {
			ok := false
			for _, m := range rt.AllowedMethods {
				if r.Method == m {
					ok = true
				}
			}
			if !ok {
				problem(w, 405, "METHOD_NOT_ALLOWED")
				return
			}
		}
		sid, err := sessionID(r, rt)
		if err != nil {
			problem(w, 400, "INVALID_SESSION_ID")
			return
		}
		if !rt.Session.Enabled {
			sid = ""
		} else if sid == "" {
			problem(w, 400, "SESSION_ID_REQUIRED")
			return
		}
		l, err := g.acquire(caller.ID, key, sid, "")
		if err != nil {
			problem(w, 503, err.Error())
			return
		}
		defer l.release()
		g.proxy(w, r, *caller, l)
	})
}
func cleanHeaders(h http.Header) {
	for k := range h {
		v := strings.ToLower(k)
		if v == "authorization" || v == "cookie" || v == "forwarded" || v == "x-real-ip" || v == "x-request-id" || strings.HasPrefix(v, "x-forwarded-") || strings.HasPrefix(v, "x-gateway-") || v == "x-user-id" || v == "x-tenant-id" {
			h.Del(k)
		}
	}
}
func upstreamHeaders(h http.Header, c Caller, e *Endpoint) {
	cleanHeaders(h)
	h.Set("X-Gateway-Caller-ID", c.ID)
	if e.CredentialRef != "" {
		h.Set("Authorization", "Bearer "+os.Getenv(e.CredentialRef))
	}
}

type buffers struct{ p sync.Pool }

func (b *buffers) Get() []byte {
	if v := b.p.Get(); v != nil {
		return v.([]byte)
	}
	return make([]byte, 32<<10)
}
func (b *buffers) Put(v []byte) {
	if cap(v) == 32<<10 {
		b.p.Put(v[:32<<10])
	}
}

var copyBuffers buffers

// Read timer cancels stalled upstream reads, including streams that never emit data.
type idleBody struct {
	io.ReadCloser
	timer  *time.Timer
	cancel context.CancelFunc
	idle   time.Duration
	mu     sync.Mutex
	closed bool
}

func newIdleBody(b io.ReadCloser, c context.CancelFunc, d time.Duration) *idleBody {
	x := &idleBody{ReadCloser: b, cancel: c, idle: d}
	x.timer = time.AfterFunc(d, c)
	return x
}
func (b *idleBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.mu.Lock()
		if !b.closed {
			b.timer.Reset(b.idle)
		}
		b.mu.Unlock()
	}
	return n, e
}
func (b *idleBody) Close() error {
	b.mu.Lock()
	b.closed = true
	b.timer.Stop()
	b.mu.Unlock()
	return b.ReadCloser.Close()
}

type deadlineWriter struct {
	http.ResponseWriter
	idle time.Duration
}

func (w *deadlineWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *deadlineWriter) Write(b []byte) (int, error) {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.idle))
	return w.ResponseWriter.Write(b)
}
func (w *deadlineWriter) Flush() { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (g *Gateway) proxy(w http.ResponseWriter, r *http.Request, c Caller, l *lease) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(l.route.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	target, _ := url.Parse(l.ep.URL)
	idle := time.Duration(l.route.Timeout.StreamIdleMS) * time.Millisecond
	dw := &deadlineWriter{w, idle}
	defer http.NewResponseController(w).SetWriteDeadline(time.Time{})
	rp := &httputil.ReverseProxy{Transport: g.transport(l.route.Timeout.ResponseHeaderMS), BufferPool: &copyBuffers, Rewrite: func(pr *httputil.ProxyRequest) {
		prefix := "/proxy/" + l.route.Key
		pr.Out.URL.Path = strings.TrimPrefix(pr.In.URL.Path, prefix)
		pr.Out.URL.RawPath = ""
		if pr.In.URL.RawPath != "" {
			pr.Out.URL.RawPath = strings.TrimPrefix(pr.In.URL.RawPath, prefix)
		}
		if pr.Out.URL.Path == "" {
			pr.Out.URL.Path = "/"
		}
		pr.SetURL(target)
		upstreamHeaders(pr.Out.Header, c, l.ep)
		pr.Out.Header.Del("Idempotency-Key")
		pr.Out.Header.Del("X-Idempotency-Key")
	}, ModifyResponse: func(res *http.Response) error {
		res.Body = newIdleBody(res.Body, cancel, idle)
		if strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
			res.Header.Set("X-Accel-Buffering", "no")
		}
		return nil
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		g.failures.Add(1)
		code := 502
		if ctx.Err() == context.DeadlineExceeded {
			code = 504
		}
		problem(w, code, "UPSTREAM_UNAVAILABLE")
	}}
	rp.ServeHTTP(dw, r)
}
