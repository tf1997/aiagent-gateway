package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

// Hermes v2026.9.24 subscriptions start with updates rather than the Task
// required by A2A 1.0. Obtain that Task from the same bound endpoint. The gateway
// still emits a conforming 1.0 stream and does not accept the 0.3 wire format.
func (g *Handler) hermesSubscriptionSeed(ctx context.Context, q RPC, c protocol.Caller, l *protocol.Lease) ([]byte, error) {
	get := RPC{JSONRPC: "2.0", ID: q.ID, Method: "GetTask", Params: map[string]any{"id": q.Params["id"], "historyLength": 0}}
	req, err := g.UpstreamRequest(ctx, get, c, l.Interface)
	if err != nil {
		return nil, err
	}
	res, err := g.Transport(l.Route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 || res.StatusCode != 200 {
		return nil, fmt.Errorf("cannot read subscribed task")
	}
	env, err := decodeEnvelope(raw, q.ID)
	if err != nil {
		return nil, err
	}
	if env["error"] != nil {
		return nil, fmt.Errorf("cannot read subscribed task")
	}
	task, ok := env["result"].(map[string]any)
	if !ok {
		return nil, protocol.ErrResourceInvalid
	}
	if err = checkIdentity(task, "task", get); err != nil {
		return nil, err
	}
	env["result"] = map[string]any{"task": task}
	return json.Marshal(env)
}

// Hermes ignores returnImmediately on SendMessage. Its streaming entry gives us
// the Task promptly; keep draining that same bounded stream after acknowledging
// the caller so closing the response does not cancel the execution.
func (g *Handler) hermesImmediate(w http.ResponseWriter, r *http.Request, c protocol.Caller, l *protocol.Lease, q RPC, sid string) {
	ctx, cancel := context.WithTimeout(g.Background(), time.Duration(l.Route.Timeout.MaxDurationMS)*time.Millisecond)
	if attempt := r.Context().Value(protocol.AttemptKey{}); attempt != nil {
		ctx = context.WithValue(ctx, protocol.AttemptKey{}, attempt)
	}
	cancelOnDisconnect := context.AfterFunc(r.Context(), cancel)
	q.Method = "SendStreamingMessage"
	req, err := g.UpstreamRequest(ctx, q, c, l.Interface)
	if err != nil {
		cancelOnDisconnect()
		cancel()
		RPCError(w, q.ID, -32603, "Invalid upstream")
		return
	}
	res, err := g.Transport(l.Route.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		cancelOnDisconnect()
		cancel()
		RPCError(w, q.ID, -32603, "Upstream unavailable")
		return
	}
	if res.StatusCode != 200 || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		res.Body.Close()
		cancelOnDisconnect()
		cancel()
		RPCError(w, q.ID, -32603, "Hermes streaming required for immediate send")
		return
	}
	done, accepted := g.RegisterBackground()
	if !accepted {
		res.Body.Close()
		cancelOnDisconnect()
		cancel()
		RPCError(w, q.ID, -32603, "Gateway shutting down")
		return
	}
	release := l.Release
	l.Release = func() {}
	first := make(chan []byte, 1)
	idle := time.Duration(l.Route.Timeout.StreamIdleMS) * time.Millisecond
	go func() {
		defer done()
		defer release()
		defer cancel()
		defer cancelOnDisconnect()
		body := protocol.NewIdleBody(res.Body, cancel, idle)
		defer body.Close()
		state := &a2aStreamState{}
		sent := false
		var observed protocol.Resource
		transform := func(raw []byte) ([]byte, error) {
			env, err := decodeEnvelope(raw, q.ID)
			if err != nil {
				return nil, err
			}
			if env["error"] != nil {
				return nil, protocol.ErrResourceInvalid
			}
			result := env["result"].(map[string]any)
			node, kind, err := decodeResult(result, q.Method)
			if err != nil {
				return nil, err
			}
			if err = checkIdentity(node, kind, q); err != nil {
				return nil, err
			}
			if err = state.accept(node, kind); err != nil {
				return nil, err
			}
			if err = g.MapObject(node, kind, c.ID, l.Route.ID, l.Endpoint.ID, sid); err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(env)
			if err != nil {
				return nil, err
			}
			if !sent {
				if kind == "task" {
					observed, _ = g.Resources.Lookup(protocol.ResourceKey{Owner: c.ID, Route: l.Route.ID, Kind: protocol.A2ATask, ID: protocol.StringField(node, "id")})
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
			g.Resources.Update(observed, protocol.ResourceUnknown)
		}
		if !sent {
			first <- nil
		}
	}()
	select {
	case raw := <-first:
		if raw == nil {
			RPCError(w, q.ID, -32603, "Hermes did not return an initial Task")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	case <-r.Context().Done():
		cancel()
	case <-ctx.Done():
		RPCError(w, q.ID, -32603, "Upstream unavailable")
	}
}

type discardStream struct{ header http.Header }

func (d *discardStream) Header() http.Header { return d.header }

func (*discardStream) Write(p []byte) (int, error) { return len(p), nil }

func (*discardStream) WriteHeader(int) {}

func (*discardStream) Flush() {}
