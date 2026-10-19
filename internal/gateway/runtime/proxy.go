package runtime

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/gateway/runtime/rollout"
)

func (g *Gateway) transport(ms int) http.RoundTripper {
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
	return rollout.ObserveTransport(t)
}

func (g *Gateway) DataHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			protocol.WriteJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		if r.URL.Path == "/readyz" {
			code := 200
			if !g.healthy.Load() {
				code = 503
			}
			protocol.WriteJSON(w, code, map[string]bool{"ready": code == 200})
			return
		}
		if g.remote == nil {
			leave, err := g.stateBarrier.TryEnter()
			if err != nil {
				protocol.Problem(w, 503, err.Error())
				return
			}
			defer leave()
		}
		g.requests.Add(1)
		p := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
		if len(p) < 2 {
			protocol.Problem(w, 404, "NOT_FOUND")
			return
		}
		key := p[1]
		rt := g.current.Load().Routes[key]
		if rt == nil || !rt.Enabled {
			protocol.Problem(w, 404, "ROUTE_NOT_FOUND")
			return
		}
		caller := configuration.Authenticate(rt, r.Header.Get("Authorization"))
		if caller == nil {
			protocol.Problem(w, 401, "UNAUTHORIZED")
			return
		}
		if !g.healthy.Load() {
			protocol.Problem(w, 503, "GATEWAY_NOT_READY")
			return
		}
		suffix := ""
		if len(p) == 3 {
			suffix = p[2]
		}
		switch p[0] {
		case "a2a":
			if len(p) != 2 {
				protocol.Problem(w, 404, "NOT_FOUND")
				return
			}
			suffix = "a2a"
		case "agents", "proxy":
		default:
			protocol.Problem(w, 404, "NOT_FOUND")
			return
		}
		if suffix == "a2a" || strings.HasPrefix(suffix, "a2a/") || suffix == ".well-known" || strings.HasPrefix(suffix, ".well-known/") {
			if !protocol.Exposes(rt, "a2a") {
				protocol.Problem(w, 404, "NOT_FOUND")
				return
			}
			if suffix == ".well-known/agent-card.json" {
				if r.Method != "GET" {
					protocol.Problem(w, 405, "METHOD_NOT_ALLOWED")
					return
				}
				g.a2aHandler.Card(w, r, rt)
			} else if suffix == "a2a" {
				g.a2aHandler.ServeHTTP(w, r, *caller, rt)
			} else {
				protocol.Problem(w, 404, "NOT_FOUND")
			}
			return
		}
		if !protocol.Exposes(rt, "proxy") {
			protocol.Problem(w, 404, "NOT_FOUND")
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
				protocol.Problem(w, 405, "METHOD_NOT_ALLOWED")
				return
			}
		}
		if rt.AgentType == "hermes" {
			g.hermesHandler.ServeHTTP(w, r, *caller, rt, "/"+suffix)
			return
		}
		sid, err := protocol.SessionID(r, rt)
		if err != nil {
			protocol.Problem(w, 400, "INVALID_SESSION_ID")
			return
		}
		if !rt.Session.Enabled {
			sid = ""
		} else if sid == "" {
			protocol.Problem(w, 400, "SESSION_ID_REQUIRED")
			return
		}
		l, err := g.acquire(caller.ID, key, sid, "", "proxy", rt)
		if err != nil {
			protocol.Problem(w, 503, err.Error())
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

func (g *Gateway) upstreamHeaders(h http.Header, c protocol.Caller, e protocol.EndpointInterface) {
	cleanHeaders(h)
	h.Set("X-Gateway-Caller-ID", c.ID)
	if e.Token != "" {
		h.Set("Authorization", "Bearer "+e.Token)
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

func (g *Gateway) proxy(w http.ResponseWriter, r *http.Request, c protocol.Caller, l *lease) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(l.route.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	r = r.WithContext(ctx)
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	target, _ := url.Parse(l.iface.URL)
	idle := time.Duration(l.route.Timeout.StreamIdleMS) * time.Millisecond
	dw := &deadlineWriter{w, idle}
	defer http.NewResponseController(w).SetWriteDeadline(time.Time{})
	rp := &httputil.ReverseProxy{Transport: g.transport(l.route.Timeout.ResponseHeaderMS), BufferPool: &copyBuffers, Rewrite: func(pr *httputil.ProxyRequest) {
		prefix := "/" + strings.SplitN(strings.TrimPrefix(pr.In.URL.Path, "/"), "/", 2)[0] + "/" + l.route.Key
		pr.Out.URL.Path = strings.TrimPrefix(pr.In.URL.Path, prefix)
		pr.Out.URL.RawPath = ""
		if pr.In.URL.RawPath != "" {
			parts := strings.SplitN(strings.TrimPrefix(pr.In.URL.EscapedPath(), "/"), "/", 3)
			if len(parts) == 3 {
				pr.Out.URL.RawPath = "/" + parts[2]
			}
		}
		if pr.Out.URL.Path == "" {
			pr.Out.URL.Path = "/"
		}
		pr.SetURL(target)
		g.upstreamHeaders(pr.Out.Header, c, l.iface)
		pr.Out.Header.Del("Idempotency-Key")
		pr.Out.Header.Del("X-Idempotency-Key")
	}, ModifyResponse: func(res *http.Response) error {
		res.Body = protocol.NewIdleBody(res.Body, cancel, idle)
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
		protocol.UpstreamProblem(w, code, "UPSTREAM_UNAVAILABLE", protocol.DescribeUpstreamError(0, nil, err, l.iface.Token))
	}}
	rp.ServeHTTP(dw, r)
}
