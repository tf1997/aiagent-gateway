package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Hermes v2026.9.24 subscriptions start with updates rather than the Task
// required by A2A 1.0. Obtain that Task from the same bound endpoint. The gateway
// still emits a conforming 1.0 stream and does not accept the 0.3 wire format.
func (g *Gateway) hermesSubscriptionSeed(ctx context.Context, q rpc, c Caller, l *lease) ([]byte, error) {
	get := rpc{JSONRPC: "2.0", ID: q.ID, Method: "GetTask", Params: map[string]any{"id": q.Params["id"], "historyLength": 0}}
	req, err := g.a2aUpstreamRequest(ctx, get, c, l.iface)
	if err != nil {
		return nil, err
	}
	res, err := g.transport(l.route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 || res.StatusCode != 200 {
		return nil, fmt.Errorf("cannot read subscribed task")
	}
	env, err := a2aEnvelope(raw, q.ID)
	if err != nil {
		return nil, err
	}
	if env["error"] != nil {
		return nil, fmt.Errorf("cannot read subscribed task")
	}
	task, ok := env["result"].(map[string]any)
	if !ok {
		return nil, errResourceInvalid
	}
	if err = checkA2AIdentity(task, "task", get); err != nil {
		return nil, err
	}
	env["result"] = map[string]any{"task": task}
	return json.Marshal(env)
}

// Hermes ignores returnImmediately on SendMessage. Its streaming entry gives us
// the Task promptly; keep draining that same bounded stream after acknowledging
// the caller so closing the response does not cancel the execution.
func (g *Gateway) hermesImmediate(w http.ResponseWriter, r *http.Request, c Caller, l *lease, q rpc, sid string) {
	ctx, cancel := context.WithTimeout(g.background, time.Duration(l.route.Timeout.MaxDurationMS)*time.Millisecond)
	if attempt := r.Context().Value(attemptKey{}); attempt != nil {
		ctx = context.WithValue(ctx, attemptKey{}, attempt)
	}
	cancelOnDisconnect := context.AfterFunc(r.Context(), cancel)
	q.Method = "SendStreamingMessage"
	req, err := g.a2aUpstreamRequest(ctx, q, c, l.iface)
	if err != nil {
		cancelOnDisconnect()
		cancel()
		rpcError(w, q.ID, -32603, "Invalid upstream")
		return
	}
	res, err := g.transport(l.route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		cancelOnDisconnect()
		cancel()
		rpcError(w, q.ID, -32603, "Upstream unavailable")
		return
	}
	if res.StatusCode != 200 || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		res.Body.Close()
		cancelOnDisconnect()
		cancel()
		rpcError(w, q.ID, -32603, "Hermes streaming required for immediate send")
		return
	}
	g.lifecycle.RLock()
	if !g.healthy.Load() {
		g.lifecycle.RUnlock()
		res.Body.Close()
		cancelOnDisconnect()
		cancel()
		rpcError(w, q.ID, -32603, "Gateway shutting down")
		return
	}
	g.wg.Add(1)
	g.lifecycle.RUnlock()
	release := l.release
	l.release = func() {}
	first := make(chan []byte, 1)
	idle := time.Duration(l.route.Timeout.StreamIdleMS) * time.Millisecond
	go func() {
		defer g.wg.Done()
		defer release()
		defer cancel()
		defer cancelOnDisconnect()
		body := newIdleBody(res.Body, cancel, idle)
		defer body.Close()
		state := &a2aStreamState{}
		sent := false
		var observed resource
		transform := func(raw []byte) ([]byte, error) {
			env, err := a2aEnvelope(raw, q.ID)
			if err != nil {
				return nil, err
			}
			if env["error"] != nil {
				return nil, errResourceInvalid
			}
			result := env["result"].(map[string]any)
			node, kind, err := a2aResult(result, q.Method)
			if err != nil {
				return nil, err
			}
			if err = checkA2AIdentity(node, kind, q); err != nil {
				return nil, err
			}
			if err = state.accept(node, kind); err != nil {
				return nil, err
			}
			if err = g.mapObject(node, kind, c.ID, l.route.ID, l.ep.ID, sid); err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(env)
			if err != nil {
				return nil, err
			}
			if !sent {
				if kind == "task" {
					observed, _ = g.resources.lookup(resourceKey{c.ID, l.route.ID, a2aTask, stringField(node, "id")})
				}
				// Detach before sending the first response to prevent a fast caller from
				// canceling the accepted stream as soon as it receives the Task.
				if !cancelOnDisconnect() && r.Context().Err() != nil {
					return nil, r.Context().Err()
				}
				first <- encoded
				sent = true
			}
			return encoded, nil
		}
		g.forwardA2AStream(&discardStream{header: make(http.Header)}, body, q.ID, idle, state, transform)
		if !state.done && observed.ID != "" {
			g.resources.update(observed, resourceUnknown)
		}
		if !sent {
			first <- nil
		}
	}()
	select {
	case raw := <-first:
		if raw == nil {
			rpcError(w, q.ID, -32603, "Hermes did not return an initial Task")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	case <-r.Context().Done():
		cancel()
	case <-ctx.Done():
		rpcError(w, q.ID, -32603, "Upstream unavailable")
	}
}

type discardStream struct{ header http.Header }

func (d *discardStream) Header() http.Header       { return d.header }
func (*discardStream) Write(p []byte) (int, error) { return len(p), nil }
func (*discardStream) WriteHeader(int)             {}
func (*discardStream) Flush()                      {}
