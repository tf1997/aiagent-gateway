package gateway

// Wire contract: a2aproject/A2A v1.0.1 specification/a2a.proto (protocol 1.0).
import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func isSend(method string) bool { return method == "SendMessage" || method == "SendStreamingMessage" }
func isStreaming(method string) bool {
	return method == "SendStreamingMessage" || method == "SubscribeToTask"
}
func stringField(m map[string]any, k string) string { s, _ := m[k].(string); return s }
func fields(m map[string]any, allowed string) error {
	for k := range m {
		if !strings.Contains(" "+allowed+" ", " "+k+" ") {
			return fmt.Errorf("unsupported field %s", k)
		}
	}
	return nil
}
func optionalStrings(m map[string]any, names ...string) error {
	for _, k := range names {
		if v, ok := m[k]; ok {
			if _, ok = v.(string); !ok {
				return fmt.Errorf("%s must be a string", k)
			}
		}
	}
	return nil
}
func optionalObject(m map[string]any, key string) error {
	if v, ok := m[key]; ok {
		if _, ok = v.(map[string]any); !ok {
			return fmt.Errorf("%s must be an object", key)
		}
	}
	return nil
}
func optionalStringList(m map[string]any, key string) error {
	if v, ok := m[key]; ok {
		list, ok := v.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array", key)
		}
		for _, v := range list {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("%s must contain strings", key)
			}
		}
	}
	return nil
}
func intField(m map[string]any, key string, min, max int) (int, bool, error) {
	v, ok := m[key]
	if !ok {
		return 0, false, nil
	}
	// Requests decoded with UseNumber; int32 fields have integral values.
	n, ok := v.(json.Number)
	if !ok {
		return 0, true, fmt.Errorf("invalid %s", key)
	}
	value, err := strconv.ParseInt(string(n), 10, 32)
	if err != nil || value < int64(min) || value > int64(max) {
		return 0, true, fmt.Errorf("invalid %s", key)
	}
	return int(value), true, nil
}
func validateParts(v any, nonempty bool) error {
	parts, ok := v.([]any)
	if !ok || (nonempty && len(parts) == 0) {
		return fmt.Errorf("parts required")
	}
	for _, v := range parts {
		p, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid part")
		}
		if err := fields(p, "text raw url data metadata filename mediaType"); err != nil {
			return err
		}
		count := 0
		for _, k := range []string{"text", "raw", "url", "data"} {
			if v, ok := p[k]; ok {
				count++
				if k != "data" {
					s, ok := v.(string)
					if !ok {
						return fmt.Errorf("invalid part content")
					}
					if k == "raw" {
						if _, err := base64.StdEncoding.DecodeString(s); err != nil {
							return fmt.Errorf("invalid raw base64")
						}
					}
				}
			}
		}
		if count != 1 {
			return fmt.Errorf("part requires exactly one content field")
		}
		if err := optionalStrings(p, "filename", "mediaType"); err != nil {
			return err
		}
		if err := optionalObject(p, "metadata"); err != nil {
			return err
		}
	}
	return nil
}
func validateMessage(m map[string]any) error {
	if m == nil || stringField(m, "messageId") == "" {
		return fmt.Errorf("messageId required")
	}
	if err := fields(m, "messageId contextId taskId role parts metadata extensions referenceTaskIds"); err != nil {
		return err
	}
	if m["role"] != "ROLE_USER" && m["role"] != "ROLE_AGENT" {
		return fmt.Errorf("invalid message role")
	}
	if err := optionalStrings(m, "contextId", "taskId"); err != nil {
		return err
	}
	if err := optionalObject(m, "metadata"); err != nil {
		return err
	}
	for _, k := range []string{"extensions", "referenceTaskIds"} {
		if err := optionalStringList(m, k); err != nil {
			return err
		}
	}
	return validateParts(m["parts"], true)
}
func validateA2AParams(q rpc) error {
	p := q.Params
	if v, ok := p["tenant"]; ok && v != "" {
		return fmt.Errorf("public interface has no tenant")
	}
	if err := optionalObject(p, "metadata"); err != nil {
		return err
	}
	switch q.Method {
	case "SendMessage", "SendStreamingMessage":
		if err := fields(p, "tenant message configuration metadata"); err != nil {
			return err
		}
		m, _ := p["message"].(map[string]any)
		if err := validateMessage(m); err != nil {
			return err
		}
		if err := optionalObject(p, "configuration"); err != nil {
			return err
		}
		if cfg, ok := p["configuration"].(map[string]any); ok {
			if err := fields(cfg, "acceptedOutputModes taskPushNotificationConfig historyLength returnImmediately"); err != nil {
				return err
			}
			if err := optionalStringList(cfg, "acceptedOutputModes"); err != nil {
				return err
			}
			if _, _, err := intField(cfg, "historyLength", 0, 1<<31-1); err != nil {
				return err
			}
			if v, ok := cfg["returnImmediately"]; ok {
				if _, ok := v.(bool); !ok {
					return fmt.Errorf("invalid returnImmediately")
				}
			}
			if err := optionalObject(cfg, "taskPushNotificationConfig"); err != nil {
				return err
			}
		}
	case "GetTask", "CancelTask", "SubscribeToTask":
		allowed := "tenant id"
		if q.Method == "GetTask" {
			allowed += " historyLength"
		}
		if q.Method == "CancelTask" {
			allowed += " metadata"
		}
		if err := fields(p, allowed); err != nil {
			return err
		}
		if stringField(p, "id") == "" {
			return fmt.Errorf("task ID required")
		}
		if _, _, err := intField(p, "historyLength", 0, 1<<31-1); err != nil {
			return err
		}
	case "ListTasks":
		if err := fields(p, "tenant contextId status pageSize pageToken historyLength statusTimestampAfter includeArtifacts"); err != nil {
			return err
		}
		if err := optionalStrings(p, "contextId", "status", "pageToken", "statusTimestampAfter"); err != nil {
			return err
		}
		if _, _, err := intField(p, "pageSize", 1, 100); err != nil {
			return err
		}
		if _, _, err := intField(p, "historyLength", 0, 1<<31-1); err != nil {
			return err
		}
		if s := stringField(p, "status"); s != "" && !knownTaskState(s) {
			return fmt.Errorf("invalid status")
		}
		if s := stringField(p, "statusTimestampAfter"); s != "" {
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				return fmt.Errorf("invalid statusTimestampAfter")
			}
		}
		if v, ok := p["includeArtifacts"]; ok {
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("invalid includeArtifacts")
			}
		}
	}
	return nil
}
func knownTaskState(s string) bool {
	switch s {
	case "TASK_STATE_UNSPECIFIED", "TASK_STATE_SUBMITTED", "TASK_STATE_WORKING", "TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED", "TASK_STATE_INPUT_REQUIRED", "TASK_STATE_AUTH_REQUIRED":
		return true
	}
	return false
}
func taskResourceState(s string) resourceState {
	switch s {
	case "TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED":
		return resourceTerminal
	case "TASK_STATE_SUBMITTED", "TASK_STATE_WORKING":
		return resourceLive
	case "TASK_STATE_INPUT_REQUIRED", "TASK_STATE_AUTH_REQUIRED":
		return resourceWaiting
	}
	return resourceUnknown
}
func a2aEnvelope(b []byte, id json.RawMessage) (map[string]any, error) {
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if !json.Valid(b) || d.Decode(&m) != nil || m["jsonrpc"] != "2.0" {
		return nil, fmt.Errorf("invalid envelope")
	}
	upstreamID, _ := json.Marshal(m["id"])
	var expected any
	d = json.NewDecoder(bytes.NewReader(id))
	d.UseNumber()
	_ = d.Decode(&expected)
	expectedID, _ := json.Marshal(expected)
	if !bytes.Equal(upstreamID, expectedID) {
		return nil, fmt.Errorf("mismatched response ID")
	}
	result, hasResult := m["result"]
	errValue, hasError := m["error"]
	if hasResult == hasError {
		return nil, fmt.Errorf("response requires exactly one result or error")
	}
	if hasResult {
		if _, ok := result.(map[string]any); !ok {
			return nil, fmt.Errorf("invalid result")
		}
	}
	if hasError {
		e, ok := errValue.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid error")
		}
		n, ok := e["code"].(json.Number)
		if !ok {
			return nil, fmt.Errorf("invalid error code")
		}
		code, err := strconv.Atoi(string(n))
		if err != nil {
			return nil, err
		}
		if _, ok := e["message"].(string); !ok {
			return nil, fmt.Errorf("invalid error message")
		}
		// Upstream error details can contain private IDs, URLs and credentials.
		m["error"] = map[string]any{"code": code, "message": a2aErrorText(code)}
	}
	m["id"] = id
	return m, nil
}
func a2aErrorText(code int) string {
	switch code {
	case -32700:
		return "Invalid JSON payload"
	case -32600:
		return "Invalid request"
	case -32601:
		return "Method not found"
	case -32602:
		return "Invalid parameters"
	case -32603:
		return "Upstream internal error"
	case -32001:
		return "Task not found"
	case -32002:
		return "Task not cancelable"
	case -32003:
		return "Push notifications not supported"
	case -32004:
		return "Unsupported operation"
	case -32005:
		return "Content type not supported"
	case -32006:
		return "Invalid agent response"
	case -32007:
		return "Extended agent card not configured"
	case -32008:
		return "Extension support required"
	case -32009:
		return "Version not supported"
	}
	return "Upstream error"
}
func a2aResult(result map[string]any, method string) (map[string]any, string, error) {
	if method == "GetTask" || method == "CancelTask" {
		return result, "task", nil
	}
	allowed := "task message"
	if isStreaming(method) {
		allowed += " statusUpdate artifactUpdate"
	}
	if err := fields(result, allowed); err != nil {
		return nil, "", err
	}
	if len(result) != 1 {
		return nil, "", fmt.Errorf("response requires one payload")
	}
	for k, v := range result {
		node, ok := v.(map[string]any)
		if !ok {
			return nil, "", fmt.Errorf("invalid payload")
		}
		return node, k, nil
	}
	return nil, "", fmt.Errorf("missing payload")
}
func checkA2AIdentity(node map[string]any, kind string, q rpc) error {
	task, contextID := stringField(q.Params, "id"), ""
	if msg, ok := q.Params["message"].(map[string]any); ok {
		task = stringField(msg, "taskId")
		contextID = stringField(msg, "contextId")
	}
	taskField := "taskId"
	if kind == "task" {
		taskField = "id"
	}
	if task != "" && stringField(node, taskField) != task {
		return fmt.Errorf("upstream changed task identity")
	}
	if contextID != "" && stringField(node, "contextId") != contextID {
		return fmt.Errorf("upstream changed context identity")
	}
	return nil
}
func trimA2AHistory(node map[string]any, q rpc) {
	p := q.Params
	if isSend(q.Method) {
		p, _ = p["configuration"].(map[string]any)
	}
	if n, ok, err := intField(p, "historyLength", 0, 1<<31-1); err == nil && ok {
		if history, ok := node["history"].([]any); ok && len(history) > n {
			node["history"] = history[len(history)-n:]
		}
	}
}

// Only protocol-owned fields are rewritten; parts and metadata remain opaque.
// Stage a complete frame before publishing any resource identity or lifecycle state.
func (g *Gateway) mapResult(m map[string]any, owner, route, ep, sid string) error {
	return g.mapObject(m, "task", owner, route, ep, sid)
}
func (g *Gateway) mapObject(m map[string]any, kind, owner, route, ep, sid string) error {
	if kind == "message" && stringField(m, "contextId") == "" {
		return fmt.Errorf("server message contextId required")
	}
	obs := []resourceObservation{}
	type rewrite struct {
		key   resourceReverseKey
		apply func(string)
	}
	changes := []rewrite{}
	add := func(kind resourceKind, up, parent string, state resourceState, reference bool, set func(string)) {
		obs = append(obs, resourceObservation{Kind: kind, Upstream: up, ParentUpstream: parent, SessionID: sid, State: state, ReferenceOnly: reference})
		changes = append(changes, rewrite{resourceReverseKey{owner, route, kind, ep, up}, set})
	}
	var walk func(map[string]any, string) error
	walk = func(node map[string]any, kind string) error {
		if node == nil {
			return fmt.Errorf("missing %s", kind)
		}
		allowed := ""
		switch kind {
		case "task":
			allowed = "id contextId status artifacts history metadata"
		case "message":
			allowed = "messageId contextId taskId role parts metadata extensions referenceTaskIds"
		case "statusUpdate":
			allowed = "taskId contextId status metadata"
		case "artifactUpdate":
			allowed = "taskId contextId artifact append lastChunk metadata"
		default:
			return fmt.Errorf("invalid payload")
		}
		if err := fields(node, allowed); err != nil {
			return err
		}
		if err := optionalStrings(node, "id", "taskId", "contextId"); err != nil {
			return err
		}
		if err := optionalObject(node, "metadata"); err != nil {
			return err
		}
		if kind == "message" {
			if err := validateMessage(node); err != nil {
				return err
			}
		}
		ctxID := stringField(node, "contextId")
		if ((kind == "message" && node["role"] == "ROLE_AGENT") || kind == "statusUpdate" || kind == "artifactUpdate") && ctxID == "" {
			return fmt.Errorf("contextId required")
		}
		if ctxID != "" {
			add(a2aContext, ctxID, "", resourceLive, false, func(id string) { node["contextId"] = id })
		}
		taskField := "taskId"
		if kind == "task" {
			taskField = "id"
		}
		tid := stringField(node, taskField)
		state := resourceState("")
		if kind == "task" || kind == "statusUpdate" {
			status, ok := node["status"].(map[string]any)
			if !ok {
				return fmt.Errorf("task status required")
			}
			if err := fields(status, "state message timestamp"); err != nil {
				return err
			}
			s := stringField(status, "state")
			// Future named enums remain conservative/unknown; old 0.3 spellings fail.
			if !strings.HasPrefix(s, "TASK_STATE_") {
				return fmt.Errorf("invalid task state")
			}
			state = taskResourceState(s)
			if err := optionalStrings(status, "timestamp"); err != nil {
				return err
			}
			if ts := stringField(status, "timestamp"); ts != "" {
				if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
					return fmt.Errorf("invalid status timestamp")
				}
			}
			if v, ok := status["message"]; ok {
				msg, _ := v.(map[string]any)
				if err := walk(msg, "message"); err != nil {
					return err
				}
			}
		}
		if tid != "" {
			add(a2aTask, tid, ctxID, state, false, func(id string) { node[taskField] = id })
		} else if kind != "message" {
			return fmt.Errorf("task ID required")
		}
		if value, ok := node["history"]; ok {
			history, ok := value.([]any)
			if !ok {
				return fmt.Errorf("invalid history")
			}
			for _, v := range history {
				msg, _ := v.(map[string]any)
				if err := walk(msg, "message"); err != nil {
					return err
				}
			}
		}
		if value, ok := node["artifacts"]; ok {
			artifacts, ok := value.([]any)
			if !ok {
				return fmt.Errorf("invalid artifacts")
			}
			for _, v := range artifacts {
				if err := validateArtifact(v); err != nil {
					return err
				}
			}
		}
		if kind == "artifactUpdate" {
			if err := validateArtifact(node["artifact"]); err != nil {
				return err
			}
			for _, k := range []string{"append", "lastChunk"} {
				if v, ok := node[k]; ok {
					if _, ok := v.(bool); !ok {
						return fmt.Errorf("invalid %s", k)
					}
				}
			}
		}
		if refs, ok := node["referenceTaskIds"].([]any); ok {
			for i, v := range refs {
				up := v.(string)
				add(a2aTask, up, "", resourceUnknown, true, func(id string) { refs[i] = id })
			}
		}
		return nil
	}
	if err := walk(m, kind); err != nil {
		return err
	}
	mapped, err := g.resources.observeBatch(owner, route, ep, obs)
	if err != nil {
		return err
	}
	for _, change := range changes {
		change.apply(mapped[change.key].ID)
	}
	return nil
}
func validateArtifact(v any) error {
	m, ok := v.(map[string]any)
	if !ok || stringField(m, "artifactId") == "" {
		return fmt.Errorf("artifactId required")
	}
	if err := fields(m, "artifactId name description parts metadata extensions"); err != nil {
		return err
	}
	if err := optionalStrings(m, "name", "description"); err != nil {
		return err
	}
	if err := optionalObject(m, "metadata"); err != nil {
		return err
	}
	if err := optionalStringList(m, "extensions"); err != nil {
		return err
	}
	return validateParts(m["parts"], true)
}
