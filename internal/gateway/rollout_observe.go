package gateway

import (
	"aiagent-gateway/internal/releaseprotocol"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
)

type attemptKey struct{}
type proxyAttempt struct {
	sent   atomic.Bool
	failed atomic.Bool
}
type observedTransport struct{ base http.RoundTripper }

func (t observedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	a, _ := r.Context().Value(attemptKey{}).(*proxyAttempt)
	if a != nil && r.Header.Get("X-Gateway-Caller-ID") != "" {
		a.sent.Store(true)
	}
	res, err := t.base.RoundTrip(r)
	if a != nil && err != nil && r.Context().Err() == nil && a.sent.Load() {
		a.failed.Store(true)
	}
	return res, err
}

// WorkerHandler emits trusted HTTP trailers only after the handler finishes.
// The stable entry combines them with its own downstream transport observation.
func (g *Gateway) WorkerHandler(identity releaseprotocol.Identity) http.Handler {
	handler := g.DataHandler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tokenOK("Bearer "+r.Header.Get(releaseprotocol.AuthHeader), g.opts.AdminToken) {
			problem(w, 401, "UNAUTHORIZED_ENTRY")
			return
		}
		if r.URL.Path == "/readyz" || r.URL.Path == "/healthz" {
			handler.ServeHTTP(w, r)
			return
		}
		if g.remote == nil || g.refreshShared() != nil {
			problem(w, 503, "SHARED_STATE_UNAVAILABLE")
			return
		}
		a := &proxyAttempt{}
		r = r.WithContext(context.WithValue(r.Context(), attemptKey{}, a))
		o := &resultWriter{ResponseWriter: w, profile: "http", attempt: a}
		w.Header().Add("Trailer", releaseprotocol.OutcomeTrailer)
		w.Header().Add("Trailer", releaseprotocol.ProfileTrailer)
		w.Header().Add("Trailer", releaseprotocol.ReasonTrailer)
		w.Header().Set(releaseprotocol.WorkerHeader, identity.Incarnation)
		g.workerRequests.Add(1)
		defer g.workerRequests.Add(-1)
		defer func() {
			p := recover()
			if p != nil {
				o.failure = "handler_aborted"
				if _, ok := p.(sharedUnavailable); ok && o.status == 0 {
					problem(o, 503, "SHARED_STATE_UNAVAILABLE")
				} else {
					panic(p)
				}
			}
			o.finish(r.Context())
			_ = g.remote.call("request_result", sharedRequest{ID: r.Header.Get(releaseprotocol.RequestHeader), Flag: o.Header().Get(releaseprotocol.OutcomeTrailer) == "failure"}, nil)
		}()
		handler.ServeHTTP(o, r)
	})
}

type resultWriter struct {
	eventBytes int
	http.ResponseWriter
	status      int
	profile     string
	failure     string
	attempt     *proxyAttempt
	json        bytes.Buffer
	line        []byte
	event       []string
	eventName   string
	done        bool
	overflow    bool
	sse         bool
	writeFailed bool
}

func (w *resultWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *resultWriter) WriteHeader(code int) {
	if code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = code
	w.sse = strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream")
	w.Header().Del("Content-Length")
	if w.sse {
		w.profile = "sse"
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *resultWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if w.sse {
		w.scan(p, false)
	} else if w.json.Len()+len(p) <= 4<<20 {
		w.json.Write(p)
	} else {
		w.overflow = true
	}
	n, err := w.ResponseWriter.Write(p)
	if err != nil || n != len(p) {
		w.writeFailed = true
	}
	return n, err
}
func (w *resultWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		w.writeFailed = true
	}
}
func (w *resultWriter) scan(p []byte, eof bool) {
	w.line = append(w.line, p...)
	for len(w.line) > 0 {
		advance, line, _ := splitSSELine(w.line, eof)
		if advance == 0 {
			break
		}
		text := string(line)
		w.line = w.line[advance:]
		w.eventBytes += len(text) + 1
		if w.eventBytes > 2<<20 {
			w.failure = "event_too_large"
			w.event = nil
			continue
		}
		if text == "" {
			w.frame()
			w.event = nil
			w.eventName = ""
			w.eventBytes = 0
		} else if strings.HasPrefix(text, "data:") {
			w.event = append(w.event, strings.TrimPrefix(strings.TrimPrefix(text, "data:"), " "))
		} else if strings.HasPrefix(text, "event:") {
			w.eventName = strings.TrimSpace(strings.TrimPrefix(text, "event:"))
		}
	}
	if len(w.line) > 2<<20 {
		w.line = nil
		w.failure = "event_too_large"
	}
}

func (w *resultWriter) frame() {
	if len(w.event) == 0 {
		return
	}
	data := strings.Join(w.event, "\n")
	if w.eventName == "error" {
		w.failure = "protocol_error"
	}
	if data == "[DONE]" {
		w.done = true
		return
	}
	var v map[string]any
	if json.Unmarshal([]byte(data), &v) != nil {
		w.failure = "invalid_event"
		return
	}
	if v["error"] != nil {
		w.failure = "protocol_error"
	}
	if v["jsonrpc"] == "2.0" {
		w.profile = "a2a_sse"
		result, _ := v["result"].(map[string]any)
		for _, kind := range []string{"task", "statusUpdate", "message"} {
			node, _ := result[kind].(map[string]any)
			if node == nil {
				continue
			}
			if kind == "message" {
				w.done = true
			}
			status, _ := node["status"].(map[string]any)
			state := taskResourceState(stringField(status, "state"))
			if state == resourceTerminal || state == resourceWaiting {
				w.done = true
			}
		}
	}
	name := w.eventName
	if name == "" {
		name = stringField(v, "type")
	}
	switch name {
	case "done", "run.completed", "response.completed", "run.cancelled", "run.canceled":
		w.done = true
	case "run.failed", "response.failed", "error":
		w.failure = "protocol_error"
	}
}
func (w *resultWriter) finish(ctx context.Context) {
	if w.sse {
		w.scan(nil, true)
	}
	verdict := "excluded"
	reason := "local_response"
	if w.attempt.sent.Load() {
		verdict = "success"
		reason = "proxy_completed"
	}
	if !w.sse && !w.overflow && w.json.Len() > 0 {
		var v map[string]any
		if json.Unmarshal(w.json.Bytes(), &v) == nil {
			if v["jsonrpc"] == "2.0" {
				w.profile = "a2a"
			}
			if v["error"] != nil && w.attempt.sent.Load() {
				w.failure = "protocol_error"
			}
		}
	}
	if w.sse && (!w.done || len(w.line) > 0 || len(w.event) > 0) {
		if w.failure == "" && ctx.Err() == nil && !w.writeFailed {
			w.failure = "stream_not_completed"
		}
	}
	if w.attempt.failed.Load() {
		w.failure = "upstream_transport"
	}
	if w.status >= 500 || w.status == 429 {
		w.failure = "http_failure"
	}
	if w.failure != "" {
		verdict = "failure"
		reason = w.failure
	} else if ctx.Err() != nil || w.writeFailed {
		verdict = "excluded"
		reason = "client_cancelled"
	} else if w.status >= 400 {
		verdict = "excluded"
		reason = "client_error"
	} else if w.status >= 300 {
		verdict = "unknown"
		reason = "unexpected_status"
	}
	w.Header().Set(releaseprotocol.OutcomeTrailer, verdict)
	w.Header().Set(releaseprotocol.ProfileTrailer, w.profile)
	w.Header().Set(releaseprotocol.ReasonTrailer, reason)
}
func (g *Gateway) WorkerIdentity(base releaseprotocol.Identity) releaseprotocol.Identity {
	base.Protocol = 1
	base.StateProtocol = 1
	base.Ready = g.healthy.Load()
	base.ConfigRevision = g.current.Load().Config.Revision
	base.Inflight = g.workerRequests.Load()
	if g.remote != nil {
		base.Ready = base.Ready && !g.remote.failed.Load()
		base.Inflight += g.remote.active.Load()
	}
	return base
}
func (g *Gateway) WorkerControl(base releaseprotocol.Identity) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tokenOK(r.Header.Get("Authorization"), g.opts.AdminToken) {
			problem(w, 401, "UNAUTHORIZED")
			return
		}
		if r.URL.Path != "/control/v1/identity" {
			problem(w, 404, "NOT_FOUND")
			return
		}
		var err error
		if g.remote != nil {
			err = g.refreshShared()
		}
		identity := g.WorkerIdentity(base)
		if err != nil {
			identity.Ready = false
		}
		writeJSON(w, 200, identity)
	})
}
