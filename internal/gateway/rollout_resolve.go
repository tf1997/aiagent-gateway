package gateway

import (
	"aiagent-gateway/internal/releaseprotocol"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

func (g *Gateway) resolveRollout(q releaseprotocol.Request) releaseprotocol.Resolution {
	h := http.Header(q.Header)
	var caller *Caller
	for i := range g.opts.Callers {
		c := &g.opts.Callers[i]
		if tokenOK(h.Get("Authorization"), c.Token) {
			caller = c
			break
		}
	}
	if caller == nil {
		return releaseprotocol.Resolution{Status: 401, Error: "UNAUTHORIZED"}
	}
	parts := strings.SplitN(strings.TrimPrefix(q.Path, "/"), "/", 3)
	if len(parts) < 2 || (parts[0] != "agents" && parts[0] != "proxy" && parts[0] != "a2a") {
		return releaseprotocol.Resolution{Status: 404, Error: "NOT_FOUND"}
	}
	rt := g.current.Load().Routes[parts[1]]
	if rt == nil || !rt.Enabled {
		return releaseprotocol.Resolution{Status: 404, Error: "ROUTE_NOT_FOUND"}
	}
	if !allowed(*caller, rt.Key) {
		return releaseprotocol.Resolution{Status: 403, Error: "FORBIDDEN"}
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
	bind := func(kind resourceKind, id string) {
		if id == "" || key != "" {
			return
		}
		d := g.resources
		d.mu.Lock()
		defer d.mu.Unlock()
		r := d.lookupLocked(resourceKey{caller.ID, rt.ID, kind, id})
		if r == nil {
			key = string(kind) + "/" + id
			return
		}
		for r.ParentID != "" {
			p := d.items[r.parent()]
			if p == nil {
				break
			}
			r = p
		}
		key = string(r.Kind) + "/" + r.ID
		worker = r.WorkerOwner
	}
	if entry == "a2a" {
		p, _ := body["params"].(map[string]any)
		m, _ := p["message"].(map[string]any)
		bind(a2aContext, stringField(m, "contextId"))
		bind(a2aTask, stringField(m, "taskId"))
		bind(a2aTask, stringField(p, "id"))
		bind(a2aContext, stringField(p, "contextId"))
	} else if rt.AgentType == "hermes" {
		fields := strings.Split(suffix, "/")
		if len(fields) >= 3 && fields[0] == "api" {
			switch fields[1] {
			case "sessions":
				bind(hermesSession, fields[2])
			case "runs":
				bind(hermesRun, fields[2])
			}
		}
		if len(fields) >= 3 && fields[0] == "v1" && fields[1] == "runs" {
			bind(hermesRun, fields[2])
		}
		if len(fields) >= 3 && fields[0] == "v1" && fields[1] == "responses" {
			bind(hermesResponse, fields[2])
		}
		if suffix == "api/sessions" {
			bind(hermesSession, stringField(body, "id"))
		}
		bind(hermesResponse, stringField(body, "previous_response_id"))
		bind(hermesSession, h.Get("X-Hermes-Session-Id"))
		bind(hermesSession, stringField(body, "session_id"))
		if key == "" {
			key = stringField(body, "conversation")
		}
		if key == "" {
			key = h.Get("X-Hermes-Session-Key")
		}
	}
	sid := h.Get(rt.Session.Header)
	if key == "" && sid != "" {
		key = "session/" + sid
		sh := g.shard(sessionKey{caller.ID, rt.ID, sid, entry})
		sh.mu.Lock()
		if b := sh.items[sessionKey{caller.ID, rt.ID, sid, entry}]; b != nil {
			worker = b.WorkerOwner
		}
		sh.mu.Unlock()
	}
	if key == "" && h.Get("Idempotency-Key") != "" {
		key = "operation/" + q.Method + "/" + q.Path + "/" + h.Get("Idempotency-Key")
	}
	if key != "" {
		raw := rawValue([]string{caller.ID, rt.ID, entry, key})
		sum := sha256.Sum256(raw)
		key = hex.EncodeToString(sum[:])
	}
	return releaseprotocol.Resolution{Key: key, OwnerWorker: worker}
}
func (s *SharedServer) assignWorker(resources []resource, worker string) error {
	d := s.g.resources
	d.mu.Lock()
	defer d.mu.Unlock()
	changed := false
	for _, res := range resources {
		r := d.items[res.key()]
		if r == nil {
			continue
		}
		for r.ParentID != "" {
			p := d.items[r.parent()]
			if p == nil {
				break
			}
			r = p
		}
		if r.WorkerOwner == "" {
			r.WorkerOwner = worker
			changed = true
		}
	}
	if changed && d.persist != nil {
		return d.persist()
	}
	return nil
}
func ResolveShared(socket, token string, q releaseprotocol.Request) (releaseprotocol.Resolution, error) {
	c := newSharedClient(socket, token, "entry")
	defer c.client.CloseIdleConnections()
	var out sharedResponse
	err := c.call("resolve", sharedRequest{Request: q}, &out)
	return out.Resolution, err
}
