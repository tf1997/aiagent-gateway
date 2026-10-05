package gateway

import (
	"net/http"
	"strings"
)

// Only protocol-owned ID positions are transformed. Text, metadata, tool inputs,
// outputs and arbitrary business objects are not traversed.
func (h *hermesRequest) observe(v map[string]any, event string, headers http.Header) error {
	if h.session.ID == "" {
		return nil
	}
	sessionIDs := []string{}
	active := headers.Get("X-Hermes-Session-Id")
	addSession := func(m map[string]any, key string, primary bool) error {
		if value, ok := m[key]; ok && value != nil && value != "" {
			id, ok := value.(string)
			if !ok || !validUpstreamID(id) {
				return errResourceInvalid
			}
			sessionIDs = append(sessionIDs, id)
			if primary {
				active = id
			}
		}
		return nil
	}
	if err := addSession(v, "session_id", true); err != nil {
		return err
	}
	session, _ := v["session"].(map[string]any)
	if session != nil {
		if err := addSession(session, "id", true); err != nil {
			return err
		}
	}
	messages, _ := v["data"].([]any)
	if h.op.action == "session.messages" {
		for _, item := range messages {
			if m, ok := item.(map[string]any); ok {
				if err := addSession(m, "session_id", false); err != nil {
					return err
				}
			}
		}
	}
	if (h.op.action == "session.create" || h.op.action == "session.fork") && event == "" && (session == nil || stringField(session, "id") != h.session.Upstream) {
		return errResourceConflict
	}
	executionID := stringField(v, "run_id")
	if field, ok := v["run_id"]; ok && (field == nil || executionID == "") {
		return errResourceInvalid
	}
	response, _ := v["response"].(map[string]any)
	if h.op.action == "response" {
		if response != nil {
			executionID = stringField(response, "id")
		} else if event == "" || v["object"] == "response" {
			executionID = stringField(v, "id")
		}
	} else if h.op.action == "completion" && strings.HasPrefix(stringField(v, "object"), "chat.completion") {
		executionID = stringField(v, "id")
	}

	if event == "" {
		switch h.op.action {
		case "session.create", "session.fork", "session.get", "session.patch", "session.model":
			if h.op.action != "session.model" && (session == nil || stringField(session, "id") == "") {
				return errResourceInvalid
			}
		case "session.messages":
			if _, ok := v["data"].([]any); !ok {
				return errResourceInvalid
			}
		case "run.create", "run.get", "run.stop", "run.steer", "run.approval":
			if executionID == "" {
				return errResourceInvalid
			}
		case "response":
			if executionID == "" || stringField(v, "status") == "" {
				return errResourceInvalid
			}
		case "completion":
			if executionID == "" {
				return errResourceInvalid
			}
			if _, ok := v["choices"].([]any); !ok {
				return errResourceInvalid
			}
		case "session.chat":
			if _, ok := v["message"].(map[string]any); !ok && v["object"] != "hermes.session.chat.queued" {
				return errResourceInvalid
			}
		}
	}
	state := resourceState("")
	name := event
	if name == "" || name == "message" {
		name = stringField(v, "event")
	}
	if strings.HasPrefix(name, "run.") {
		state = hermesState(strings.TrimPrefix(name, "run."))
	}
	if name == "approval.request" {
		state = resourceWaiting
	}
	if status, ok := v["status"].(string); ok {
		state = hermesState(status)
	}
	if response != nil {
		if status, ok := response["status"].(string); ok {
			state = hermesState(status)
		}
	}
	if name == "response.completed" || name == "response.failed" || name == "response.incomplete" {
		state = resourceTerminal
	}
	if h.op.action == "completion" {
		choices, _ := v["choices"].([]any)
		for _, item := range choices {
			if choice, ok := item.(map[string]any); ok && choice["finish_reason"] != nil {
				state = resourceTerminal
			}
		}
	}
	if event == "" && state == "" && (h.op.action == "session.chat" || h.op.action == "completion") && v["object"] != "hermes.session.chat.queued" {
		state = resourceTerminal
	}

	// Validate nested references before publishing any new alias or execution ID.
	for _, key := range []string{"approval", "run"} {
		if child, ok := v[key].(map[string]any); ok {
			if id := stringField(child, "run_id"); id != "" && id != executionID && id != h.execution.Upstream {
				return errResourceConflict
			}
			if id := stringField(child, "session_id"); id != "" && id != active && id != h.session.Upstream && h.g.resources.publicUpstream(h.caller.ID, h.rt.ID, hermesSession, h.session.EndpointID, id) != h.session.ID {
				return errResourceConflict
			}
		}
	}
	previousPublic := ""
	if h.op.action == "response" {
		target := v
		if response != nil {
			target = response
		}
		if previous := stringField(target, "previous_response_id"); previous != "" {
			previousPublic = h.g.resources.publicUpstream(h.caller.ID, h.rt.ID, hermesResponse, h.session.EndpointID, previous)
			ref, err := h.g.resources.lookup(resourceKey{h.caller.ID, h.rt.ID, hermesResponse, previousPublic})
			if err != nil || ref.ParentID != h.session.ID {
				return errResourceConflict
			}
		}
	}
	if err := h.g.resources.observeHermes(h.session, h.execution, sessionIDs, active, executionID, state); err != nil {
		return err
	}
	if state == resourceTerminal {
		h.terminal = true
	}
	if _, ok := v["session_id"]; ok {
		v["session_id"] = h.session.ID
	}
	if session != nil {
		session["id"] = h.session.ID
		for _, key := range []string{"parent_session_id", "root_session_id"} {
			if id := stringField(session, key); id != "" {
				mapped := h.g.resources.publicUpstream(h.caller.ID, h.rt.ID, hermesSession, h.session.EndpointID, id)
				if mapped == "" {
					delete(session, key)
				} else {
					session[key] = mapped
				}
			}
		}
	}
	if h.op.action == "session.delete" {
		if _, ok := v["id"]; ok {
			v["id"] = h.session.ID
		}
	}
	if h.op.action == "session.messages" {
		for _, item := range messages {
			if m, ok := item.(map[string]any); ok {
				if _, ok = m["session_id"]; ok {
					m["session_id"] = h.session.ID
				}
			}
		}
	}
	if _, ok := v["run_id"]; ok {
		v["run_id"] = h.execution.ID
	}
	if h.op.action == "completion" && executionID != "" {
		v["id"] = h.execution.ID
	}
	if h.op.action == "response" {
		target := v
		if response != nil {
			target = response
		}
		if _, ok := target["id"]; ok {
			target["id"] = h.execution.ID
		}
		if _, ok := target["conversation"]; ok {
			target["conversation"] = h.conversation
		}
		if previousPublic != "" {
			target["previous_response_id"] = previousPublic
		}
	}
	// Known nested control payloads may carry the same run/session references.
	for _, key := range []string{"approval", "run"} {
		if child, ok := v[key].(map[string]any); ok {
			if id := stringField(child, "run_id"); id != "" {
				child["run_id"] = h.execution.ID
			}
			if id := stringField(child, "session_id"); id != "" {
				child["session_id"] = h.session.ID
			}
		}
	}
	return nil
}
