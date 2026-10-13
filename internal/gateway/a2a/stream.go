package a2a

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

type a2aStreamState struct {
	seed                     []byte
	started, done, subscribe bool
	task, context            string
	state                    protocol.ResourceState
}

func (s *a2aStreamState) accept(node map[string]any, kind string) error {
	if s.done {
		return fmt.Errorf("event after stream completion")
	}
	tid := protocol.StringField(node, "taskId")
	if kind == "task" {
		tid = protocol.StringField(node, "id")
	}
	ctx := protocol.StringField(node, "contextId")
	if !s.started {
		if kind == "message" && !s.subscribe {
			s.started = true
			s.done = true
			return nil
		}
		if kind != "task" {
			return fmt.Errorf("stream must begin with Task")
		}
		s.started = true
		s.task = tid
		s.context = ctx
	} else {
		if kind != "statusUpdate" && kind != "artifactUpdate" {
			return fmt.Errorf("unexpected stream payload")
		}
		if s.task != tid || s.context != ctx {
			return fmt.Errorf("stream changed task identity")
		}
	}
	if status, ok := node["status"].(map[string]any); ok {
		s.state = TaskResourceState(protocol.StringField(status, "state"))
		s.done = s.state == protocol.ResourceTerminal
	}
	return nil
}

func (g *Handler) forwardA2AStream(w http.ResponseWriter, body io.Reader, id json.RawMessage, idle time.Duration, state *a2aStreamState, transform func([]byte) ([]byte, error)) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	defer controller.SetWriteDeadline(time.Time{})
	write := func(b []byte) bool {
		_ = controller.SetWriteDeadline(time.Now().Add(idle))
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	fail := func(message string) {
		g.RecordFailure()
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32006, "message": message}})
		write(b)
	}
	if len(state.seed) > 0 {
		out, err := transform(state.seed)
		if err != nil {
			fail("Invalid subscribed task")
			return
		}
		if !write(out) || state.done {
			return
		}
	}
	scanner := bufio.NewScanner(body)
	scanner.Split(protocol.SplitSSELine)
	scanner.Buffer(make([]byte, 4096), (2<<20)+1)
	var data []string
	eventSize := 0
	firstLine := true
	for scanner.Scan() {
		line := scanner.Text()
		if firstLine {
			line = strings.TrimPrefix(line, "\ufeff")
			firstLine = false
		}
		if line == "" {
			eventSize = 0
			if len(data) == 0 {
				continue
			}
			out, err := transform([]byte(strings.Join(data, "\n")))
			data = nil
			if err != nil {
				fail("Invalid upstream stream event")
				return
			}
			if !write(out) {
				return
			}
			if state.done {
				return
			}
			continue
		}
		eventSize += len(line) + 1
		if eventSize > 2<<20 {
			fail("Upstream event too large")
			return
		}
		if strings.HasPrefix(line, ":") {
			_ = controller.SetWriteDeadline(time.Now().Add(idle))
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			if controller.Flush() != nil {
				return
			}
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		}
		value = strings.TrimPrefix(value, " ")
		if field == "data" {
			data = append(data, value)
		}
	}
	// A trailing frame without its blank delimiter is incomplete and never applied.
	if scanner.Err() != nil || len(data) > 0 || !state.started || (!state.done && state.state != protocol.ResourceWaiting) {
		fail("Upstream stream interrupted before completion")
	}
}
