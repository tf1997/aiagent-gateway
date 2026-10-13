package hermes

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

func (h *hermesRequest) stream(w http.ResponseWriter, body io.Reader, idle time.Duration) {
	if h.op.action == "run.events" && h.execution.State == protocol.ResourceTerminal {
		h.terminal = true
	}
	defer func() {
		if !h.terminal && h.execution.ID != "" {
			h.g.Resources.Update(h.execution, protocol.ResourceUnknown)
		}
	}()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	defer controller.SetWriteDeadline(time.Time{})
	write := func(frame string) bool {
		_ = controller.SetWriteDeadline(time.Now().Add(idle))
		_, err := io.WriteString(w, frame)
		return err == nil && controller.Flush() == nil
	}
	fail := func() {
		h.g.RecordFailure()
		write("event: error\ndata: {\"error\":{\"code\":\"UPSTREAM_STREAM_INTERRUPTED\",\"message\":\"Hermes stream interrupted\"}}\n\n")
	}
	scanner := bufio.NewScanner(body)
	scanner.Split(protocol.SplitSSELine)
	scanner.Buffer(make([]byte, 4096), (2<<20)+1)
	var data, other []string
	name := ""
	size := 0
	first := true
	for scanner.Scan() {
		line := scanner.Text()
		if first {
			line = strings.TrimPrefix(line, "\ufeff")
			first = false
		}
		if line == "" {
			if len(data) > 0 {
				raw := strings.Join(data, "\n")
				if raw == "[DONE]" {
					if !h.terminal {
						fail()
						return
					}
				} else {
					value, err := protocol.JSONObject([]byte(raw))
					if err != nil {
						fail()
						return
					}
					// An empty SSE event name is still a stream event, not a sync completion.
					event := name
					if event == "" {
						event = "message"
					}
					if err = h.observe(value, event, nil); err != nil {
						fail()
						return
					}
					encoded, err := json.Marshal(value)
					if err != nil {
						fail()
						return
					}
					raw = string(encoded)
				}
				frame := ""
				if len(other) > 0 {
					frame = strings.Join(other, "\n") + "\n"
				}
				if !write(frame + "data: " + raw + "\n\n") {
					return
				}
			} else if len(other) > 0 {
				if !write(strings.Join(other, "\n") + "\n\n") {
					return
				}
			}
			data = nil
			other = nil
			name = ""
			size = 0
			continue
		}
		size += len(line) + 1
		if size > 2<<20 {
			fail()
			return
		}
		if strings.HasPrefix(line, ":") {
			if !write(line + "\n\n") {
				return
			}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		if field == "data" {
			data = append(data, value)
		} else {
			other = append(other, line)
			if field == "event" {
				name = value
			}
		}
	}
	if scanner.Err() != nil || len(data) > 0 || !h.terminal {
		fail()
	}
}
