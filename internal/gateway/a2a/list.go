package a2a

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

// Bounded, short-lived snapshots give repeatable pages without ever issuing an
// upstream ListTasks (which could expose another gateway caller's tasks).
const a2aListTTL = 2 * time.Minute

const a2aListMaxTasks = 1000

const a2aListMaxBytes = 8 << 20

func (g *Handler) listA2ATasks(w http.ResponseWriter, r *http.Request, c protocol.Caller, rt *protocol.Route, q RPC) {
	release, err := g.AcquireList("tasks")
	if err != nil {
		RPCError(w, q.ID, -32603, "Task list capacity reached")
		return
	}
	defer release()
	params := map[string]any{}
	for k, v := range q.Params {
		if k != "pageToken" && k != "tenant" {
			params[k] = v
		}
	}
	size, present, _ := intField(params, "pageSize", 1, 100)
	if !present {
		size = 50
	}
	params["pageSize"] = json.Number(strconv.Itoa(size))
	filterRaw, _ := json.Marshal(params)
	filter := string(filterRaw)
	token := protocol.StringField(q.Params, "pageToken")
	if token != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		key, pos, ok := strings.Cut(string(decoded), ":")
		offset, offsetErr := strconv.Atoi(pos)
		record, found := g.CacheGet("tasks", key)
		s := record
		if err != nil || !ok || offsetErr != nil || !found || s.Owner != c.ID || s.Route != rt.ID || s.Revision != rt.Version || s.Filter != filter || offset < 0 || offset >= len(s.Tasks) || offset%s.Size != 0 {
			RPCError(w, q.ID, -32602, "Invalid or expired page token")
			return
		}
		writeA2APage(w, q.ID, key, s, offset)
		return
	}
	refs := g.Resources.List(c.ID, rt.ID, protocol.A2ATask, "")
	ctxFilter := protocol.StringField(q.Params, "contextId")
	if ctxFilter != "" {
		filtered := refs[:0]
		for _, res := range refs {
			if res.ParentID == ctxFilter {
				filtered = append(filtered, res)
			}
		}
		refs = filtered
	}
	if len(refs) > a2aListMaxTasks {
		RPCError(w, q.ID, -32603, "Too many owned tasks; narrow contextId filter")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(rt.Timeout.MaxDurationMS)*time.Millisecond)
	defer cancel()
	type taskView struct {
		raw       json.RawMessage
		timestamp time.Time
		id        string
	}
	views := []taskView{}
	bytesUsed := 0
	statusFilter := protocol.StringField(q.Params, "status")
	after, _ := time.Parse(time.RFC3339Nano, protocol.StringField(q.Params, "statusTimestampAfter"))
	for _, ref := range refs {
		task, err := g.FetchTask(ctx, c, rt, ref, q.Params)
		if err != nil {
			RPCError(w, q.ID, -32603, "Cannot retrieve owned task list")
			return
		}
		if task == nil {
			continue
		} // An upstream TaskNotFound is an absent task, not a new binding.
		status := task["status"].(map[string]any)
		ts, _ := time.Parse(time.RFC3339Nano, protocol.StringField(status, "timestamp"))
		if statusFilter != "" && status["state"] != statusFilter {
			continue
		}
		if !after.IsZero() && ts.Before(after) {
			continue
		}
		if q.Params["includeArtifacts"] != true {
			delete(task, "artifacts")
		}
		raw, _ := json.Marshal(task)
		bytesUsed += len(raw)
		if bytesUsed > a2aListMaxBytes {
			RPCError(w, q.ID, -32603, "Task list snapshot too large; narrow contextId filter")
			return
		}
		views = append(views, taskView{raw, ts, protocol.StringField(task, "id")})
	}
	sort.Slice(views, func(i, j int) bool {
		if views[i].timestamp.Equal(views[j].timestamp) {
			return views[i].id < views[j].id
		}
		return views[i].timestamp.After(views[j].timestamp)
	})
	s := protocol.ListSnapshot{Owner: c.ID, Route: rt.ID, Revision: rt.Version, Filter: filter, Expires: time.Now().Add(a2aListTTL), Size: size, Bytes: bytesUsed, Tasks: make([]json.RawMessage, 0, len(views))}
	for _, v := range views {
		s.Tasks = append(s.Tasks, v.raw)
	}
	// Keep every page within the same JSON response budget as individual RPCs.
	pageBytes := 0
	for i, task := range s.Tasks {
		if i%size == 0 {
			pageBytes = 1024
		}
		pageBytes += len(task) + 1
		if pageBytes > 2<<20 {
			RPCError(w, q.ID, -32603, "Task page too large; reduce pageSize/historyLength or omit artifacts")
			return
		}
	}
	key := ""
	if len(s.Tasks) > size {
		var err error
		key, err = g.CachePut("tasks", s)
		if err != nil {
			RPCError(w, q.ID, -32603, "Task list snapshot capacity reached")
			return
		}
	}
	writeA2APage(w, q.ID, key, s, 0)
}

func writeA2APage(w http.ResponseWriter, id json.RawMessage, key string, s protocol.ListSnapshot, offset int) {
	end := offset + s.Size
	if end > len(s.Tasks) {
		end = len(s.Tasks)
	}
	next := ""
	if end < len(s.Tasks) {
		next = base64.RawURLEncoding.EncodeToString([]byte(key + ":" + strconv.Itoa(end)))
	}
	protocol.WriteJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"tasks": s.Tasks[offset:end], "nextPageToken": next, "pageSize": s.Size, "totalSize": len(s.Tasks)}})
}

func (g *Handler) FetchTask(ctx context.Context, c protocol.Caller, rt *protocol.Route, ref protocol.Resource, p map[string]any) (map[string]any, error) {
	l, res, err := g.AcquireOwnedResource(c.ID, rt, protocol.A2ATask, ref.ID, c.Reconcile)
	if err != nil {
		return nil, err
	}
	defer l.Release()
	params := map[string]any{"id": res.Upstream, "historyLength": json.Number("0")}
	if v, ok := p["historyLength"]; ok {
		params["historyLength"] = v
	}
	q := RPC{JSONRPC: "2.0", ID: json.RawMessage(`"list"`), Method: "GetTask", Params: params}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := g.UpstreamRequest(ctx, q, c, l.Interface)
	if err != nil {
		return nil, err
	}
	response, err := g.Transport(rt.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body := protocol.NewIdleBody(response.Body, cancel, time.Duration(rt.Timeout.StreamIdleMS)*time.Millisecond)
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		return nil, fmt.Errorf("invalid task response")
	}
	env, err := decodeEnvelope(raw, q.ID)
	if err != nil {
		return nil, err
	}
	if e, ok := env["error"].(map[string]any); ok {
		if e["code"] == -32001 {
			return nil, nil
		}
		return nil, fmt.Errorf("upstream RPC failure")
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("upstream HTTP failure")
	}
	task := env["result"].(map[string]any)
	if err := checkIdentity(task, "task", q); err != nil {
		return nil, err
	}
	if err := g.MapResult(task, c.ID, rt.ID, l.Endpoint.ID, res.SessionID); err != nil {
		return nil, err
	}
	trimHistory(task, q)
	return task, nil
}
