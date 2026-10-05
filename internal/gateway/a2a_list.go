package gateway

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
	"sync"
	"sync/atomic"
	"time"
)

// Bounded, short-lived snapshots give repeatable pages without ever issuing an
// upstream ListTasks (which could expose another gateway caller's tasks).
const a2aListTTL = 2 * time.Minute
const a2aListMaxTasks = 1000
const a2aListMaxBytes = 8 << 20
const a2aListCacheBytes = 32 << 20

type a2aListSnapshot struct {
	owner, route, filter string
	revision             uint64
	expires              time.Time
	tasks                []json.RawMessage
	size, bytes          int
}
type a2aListCache struct {
	mu        sync.Mutex
	active    atomic.Int64
	snapshots map[string]a2aListSnapshot
}

func (g *Gateway) listA2ATasks(w http.ResponseWriter, r *http.Request, c Caller, rt *Route, q rpc) {
	if g.taskLists.active.Add(1) > 4 {
		g.taskLists.active.Add(-1)
		rpcError(w, q.ID, -32603, "Task list capacity reached")
		return
	}
	defer g.taskLists.active.Add(-1)
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
	token := stringField(q.Params, "pageToken")
	now := time.Now()
	g.taskLists.mu.Lock()
	for k, s := range g.taskLists.snapshots {
		if !now.Before(s.expires) {
			delete(g.taskLists.snapshots, k)
		}
	}
	if token != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		key, pos, ok := strings.Cut(string(decoded), ":")
		offset, offsetErr := strconv.Atoi(pos)
		s, found := g.taskLists.snapshots[key]
		g.taskLists.mu.Unlock()
		if err != nil || !ok || offsetErr != nil || !found || s.owner != c.ID || s.route != rt.ID || s.revision != rt.Version || s.filter != filter || offset < 0 || offset >= len(s.tasks) || offset%s.size != 0 {
			rpcError(w, q.ID, -32602, "Invalid or expired page token")
			return
		}
		writeA2APage(w, q.ID, key, s, offset)
		return
	}
	g.taskLists.mu.Unlock()
	refs := g.resources.list(c.ID, rt.ID, a2aTask, "")
	ctxFilter := stringField(q.Params, "contextId")
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
		rpcError(w, q.ID, -32603, "Too many owned tasks; narrow contextId filter")
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
	statusFilter := stringField(q.Params, "status")
	after, _ := time.Parse(time.RFC3339Nano, stringField(q.Params, "statusTimestampAfter"))
	for _, ref := range refs {
		task, err := g.fetchA2ATask(ctx, c, rt, ref, q.Params)
		if err != nil {
			rpcError(w, q.ID, -32603, "Cannot retrieve owned task list")
			return
		}
		if task == nil {
			continue
		} // An upstream TaskNotFound is an absent task, not a new binding.
		status := task["status"].(map[string]any)
		ts, _ := time.Parse(time.RFC3339Nano, stringField(status, "timestamp"))
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
			rpcError(w, q.ID, -32603, "Task list snapshot too large; narrow contextId filter")
			return
		}
		views = append(views, taskView{raw, ts, stringField(task, "id")})
	}
	sort.Slice(views, func(i, j int) bool {
		if views[i].timestamp.Equal(views[j].timestamp) {
			return views[i].id < views[j].id
		}
		return views[i].timestamp.After(views[j].timestamp)
	})
	s := a2aListSnapshot{owner: c.ID, route: rt.ID, revision: rt.Version, filter: filter, expires: time.Now().Add(a2aListTTL), size: size, bytes: bytesUsed, tasks: make([]json.RawMessage, 0, len(views))}
	for _, v := range views {
		s.tasks = append(s.tasks, v.raw)
	}
	// Keep every page within the same JSON response budget as individual RPCs.
	pageBytes := 0
	for i, task := range s.tasks {
		if i%size == 0 {
			pageBytes = 1024
		}
		pageBytes += len(task) + 1
		if pageBytes > 2<<20 {
			rpcError(w, q.ID, -32603, "Task page too large; reduce pageSize/historyLength or omit artifacts")
			return
		}
	}
	key := ""
	if len(s.tasks) > size {
		g.taskLists.mu.Lock()
		used := 0
		for _, snapshot := range g.taskLists.snapshots {
			used += snapshot.bytes
		}
		if len(g.taskLists.snapshots) >= 64 || used+bytesUsed > a2aListCacheBytes {
			g.taskLists.mu.Unlock()
			rpcError(w, q.ID, -32603, "Task list snapshot capacity reached")
			return
		}
		if g.taskLists.snapshots == nil {
			g.taskLists.snapshots = map[string]a2aListSnapshot{}
		}
		key = newID("list_")
		g.taskLists.snapshots[key] = s
		g.taskLists.mu.Unlock()
	}
	writeA2APage(w, q.ID, key, s, 0)
}
func writeA2APage(w http.ResponseWriter, id json.RawMessage, key string, s a2aListSnapshot, offset int) {
	end := offset + s.size
	if end > len(s.tasks) {
		end = len(s.tasks)
	}
	next := ""
	if end < len(s.tasks) {
		next = base64.RawURLEncoding.EncodeToString([]byte(key + ":" + strconv.Itoa(end)))
	}
	writeJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"tasks": s.tasks[offset:end], "nextPageToken": next, "pageSize": s.size, "totalSize": len(s.tasks)}})
}
func (g *Gateway) fetchA2ATask(ctx context.Context, c Caller, rt *Route, ref resource, p map[string]any) (map[string]any, error) {
	l, res, err := g.acquireResource(c.ID, rt, a2aTask, ref.ID)
	if err != nil {
		return nil, err
	}
	defer l.release()
	params := map[string]any{"id": res.Upstream, "historyLength": json.Number("0")}
	if v, ok := p["historyLength"]; ok {
		params["historyLength"] = v
	}
	q := rpc{JSONRPC: "2.0", ID: json.RawMessage(`"list"`), Method: "GetTask", Params: params}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := a2aUpstreamRequest(ctx, q, c, l.iface)
	if err != nil {
		return nil, err
	}
	response, err := g.transport(rt.Timeout.ResponseHeaderMS).RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body := newIdleBody(response.Body, cancel, time.Duration(rt.Timeout.StreamIdleMS)*time.Millisecond)
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		return nil, fmt.Errorf("invalid task response")
	}
	env, err := a2aEnvelope(raw, q.ID)
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
	if err := checkA2AIdentity(task, "task", q); err != nil {
		return nil, err
	}
	if err := g.mapResult(task, c.ID, rt.ID, l.ep.ID, res.SessionID); err != nil {
		return nil, err
	}
	trimA2AHistory(task, q)
	return task, nil
}
