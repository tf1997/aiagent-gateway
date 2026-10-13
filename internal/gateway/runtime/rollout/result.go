package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

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
		advance, line, _ := protocol.SplitSSELine(w.line, eof)
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
			state := a2a.TaskResourceState(protocol.StringField(status, "state"))
			if state == protocol.ResourceTerminal || state == protocol.ResourceWaiting {
				w.done = true
			}
		}
	}
	name := w.eventName
	if name == "" {
		name = protocol.StringField(v, "type")
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
