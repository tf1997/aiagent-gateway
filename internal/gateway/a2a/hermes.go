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
		return nil, protocol.DescribeUpstreamError(0, nil, err, l.Interface.Token)
	}
	defer res.Body.Close()
	body := protocol.NewIdleBody(res.Body, func() { res.Body.Close() }, time.Duration(l.Route.Timeout.StreamIdleMS)*time.Millisecond)
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if res.StatusCode != 200 {
		return nil, upstreamHTTPError(res.StatusCode, raw, err, q.ID, l.Interface.Token)
	}
	if err != nil || len(raw) > 2<<20 {
		return nil, fmt.Errorf("cannot read subscribed task")
	}
	env, err := decodeEnvelope(raw, q.ID, l.Interface.Token)
	if err != nil {
		return nil, err
	}
	if detail, ok := env["error"].(map[string]any); ok {
		return nil, &upstreamRPCFailure{detail: detail}
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
		writeUpstreamRPCError(w, q.ID, "Upstream unavailable", protocol.DescribeUpstreamError(0, nil, err, l.Interface.Token))
		return
	}
	if res.StatusCode != 200 || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		body := protocol.NewIdleBody(res.Body, cancel, time.Duration(l.Route.Timeout.StreamIdleMS)*time.Millisecond)
		raw, err := io.ReadAll(io.LimitReader(body, (2<<20)+1))
		body.Close()
		cancelOnDisconnect()
		cancel()
		writeUpstreamRPCError(w, q.ID, "Hermes streaming required for immediate send", upstreamHTTPError(res.StatusCode, raw, err, q.ID, l.Interface.Token))
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
		upstreamFailed := false
		var observed protocol.Resource
		transform := func(raw []byte) ([]byte, error) {
			env, err := decodeEnvelope(raw, q.ID, l.Interface.Token)
			if err != nil {
				return nil, err
			}
			if env["error"] != nil {
				upstreamFailed = true
				state.done = true
				encoded, err := json.Marshal(env)
				if err == nil && !sent {
					first <- encoded
					sent = true
				}
				return encoded, err
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
		if (!state.done || upstreamFailed) && observed.ID != "" {
			g.Resources.Update(observed, protocol.ResourceUnknown)
		}
		if !sent {
			first <- nil
		}
	}()
	writeFirst := func(raw []byte) {
		if raw == nil {
			RPCError(w, q.ID, -32603, "Hermes did not return an initial Task")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	}
	select {
	case raw := <-first:
		writeFirst(raw)
	case <-r.Context().Done():
		cancel()
	case <-ctx.Done():
		// A short stream may finish and cancel its context after queuing the
		// first response. Prefer that response, including an upstream error.
		select {
		case raw := <-first:
			writeFirst(raw)
		default:
			writeUpstreamRPCError(w, q.ID, "Upstream unavailable", protocol.DescribeUpstreamError(0, nil, ctx.Err(), l.Interface.Token))
		}
	}
}

type discardStream struct{ header http.Header }

func (d *discardStream) Header() http.Header { return d.header }

func (*discardStream) Write(p []byte) (int, error) { return len(p), nil }

func (*discardStream) WriteHeader(int) {}

func (*discardStream) Flush() {}
