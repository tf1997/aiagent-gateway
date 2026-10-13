package rollout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	"aiagent-gateway/internal/releaseprotocol"
)

// Resolver derives stable affinity from resource lineage, sessions and request
// idempotency. Callbacks inspect ownership without changing it.
type Resolver struct {
	Route         func(key string) *protocol.Route
	ResourceOwner func(protocol.ResourceKey) (key, worker string)
	SessionOwner  func(owner, route, session, entry string) string
}

func (s Resolver) Resolve(q releaseprotocol.Request) releaseprotocol.Resolution {
	h := http.Header(q.Header)
	parts := strings.SplitN(strings.TrimPrefix(q.Path, "/"), "/", 3)
	if len(parts) < 2 || (parts[0] != "agents" && parts[0] != "proxy" && parts[0] != "a2a") {
		return releaseprotocol.Resolution{Status: 404, Error: "NOT_FOUND"}
	}
	rt := s.Route(parts[1])
	if rt == nil || !rt.Enabled {
		return releaseprotocol.Resolution{Status: 404, Error: "ROUTE_NOT_FOUND"}
	}
	caller := configuration.Authenticate(rt, h.Get("Authorization"))
	if caller == nil {
		return releaseprotocol.Resolution{Status: 401, Error: "UNAUTHORIZED"}
	}
	suffix := ""
	if len(parts) == 3 {
		suffix = parts[2]
	}
	entry := "proxy"
	if parts[0] == "a2a" || suffix == "a2a" {
		entry = "a2a"
	}
	if !q.BodyRead && (entry == "a2a" || rt.AgentType == "hermes") && (q.Method == "POST" || q.Method == "PATCH") {
		return releaseprotocol.Resolution{NeedBody: true}
	}
	key := ""
	worker := ""
	var body map[string]any
	_ = json.Unmarshal(q.Body, &body)
	bind := func(kind protocol.ResourceKind, id string) {
		if id == "" || key != "" {
			return
		}
		key, worker = s.ResourceOwner(protocol.ResourceKey{Owner: caller.ID, Route: rt.ID, Kind: kind, ID: id})
	}
	if entry == "a2a" {
		p, _ := body["params"].(map[string]any)
		m, _ := p["message"].(map[string]any)
		bind(protocol.A2AContext, protocol.StringField(m, "contextId"))
		bind(protocol.A2ATask, protocol.StringField(m, "taskId"))
		bind(protocol.A2ATask, protocol.StringField(p, "id"))
		bind(protocol.A2AContext, protocol.StringField(p, "contextId"))
	} else if rt.AgentType == "hermes" {
		fields := strings.Split(suffix, "/")
		if len(fields) >= 3 && fields[0] == "api" {
			switch fields[1] {
			case "sessions":
				bind(protocol.HermesSession, fields[2])
			case "runs":
				bind(protocol.HermesRun, fields[2])
			}
		}
		if len(fields) >= 3 && fields[0] == "v1" && fields[1] == "runs" {
			bind(protocol.HermesRun, fields[2])
		}
		if len(fields) >= 3 && fields[0] == "v1" && fields[1] == "responses" {
			bind(protocol.HermesResponse, fields[2])
		}
		if suffix == "api/sessions" {
			bind(protocol.HermesSession, protocol.StringField(body, "id"))
		}
		bind(protocol.HermesResponse, protocol.StringField(body, "previous_response_id"))
		bind(protocol.HermesSession, h.Get("X-Hermes-Session-Id"))
		bind(protocol.HermesSession, protocol.StringField(body, "session_id"))
		if key == "" {
			key = protocol.StringField(body, "conversation")
		}
		if key == "" {
			key = h.Get("X-Hermes-Session-Key")
		}
	}
	sid := h.Get(rt.Session.Header)
	if key == "" && sid != "" {
		key = "session/" + sid
		worker = s.SessionOwner(caller.ID, rt.ID, sid, entry)
	}
	if key == "" && h.Get("Idempotency-Key") != "" {
		key = "operation/" + q.Method + "/" + q.Path + "/" + h.Get("Idempotency-Key")
	}
	if key != "" {
		raw, _ := json.Marshal([]string{caller.ID, rt.ID, entry, key})
		sum := sha256.Sum256(raw)
		key = hex.EncodeToString(sum[:])
	}
	return releaseprotocol.Resolution{Key: key, OwnerWorker: worker}
}
