package gateway

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (g *Gateway) listHermesSessions(w http.ResponseWriter, r *http.Request, c Caller, rt *Route) {
	release, err := g.acquireList("sessions")
	if err != nil {
		problem(w, 503, "SESSION_LIST_CAPACITY")
		return
	}
	defer release()
	query := r.URL.Query()
	for key, values := range query {
		if len(values) != 1 || (key != "limit" && key != "offset" && key != "page_token" && key != "source" && key != "include_children") {
			problem(w, 400, "INVALID_PAGINATION")
			return
		}
	}
	limit := 50
	if query.Get("limit") != "" {
		n, err := strconv.Atoi(query.Get("limit"))
		if err != nil || n < 1 || n > 200 {
			problem(w, 400, "INVALID_PAGINATION")
			return
		}
		limit = n
	}
	offset := 0
	if query.Get("offset") != "" {
		n, err := strconv.Atoi(query.Get("offset"))
		if err != nil || n < 0 {
			problem(w, 400, "INVALID_PAGINATION")
			return
		}
		offset = n
	}
	if v := query.Get("include_children"); v != "" && v != "true" && v != "false" {
		problem(w, 400, "INVALID_PAGINATION")
		return
	}
	token := query.Get("page_token")
	query.Del("page_token")
	query.Del("offset")
	query.Set("limit", strconv.Itoa(limit))
	filter := query.Encode()
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		key, pos, ok := strings.Cut(string(raw), ":")
		n, e := strconv.Atoi(pos)
		record, found := g.cacheGet("sessions", key)
		s := record.snapshot()
		if err != nil || e != nil || !ok || !found || s.owner != c.ID || s.route != rt.ID || s.revision != rt.Version || s.filter != filter || n < 0 || n >= len(s.tasks) || (offset != 0 && offset != n) {
			problem(w, 400, "INVALID_PAGE_TOKEN")
			return
		}
		writeHermesPage(w, key, s, n)
		return
	}
	refs := g.resources.list(c.ID, rt.ID, hermesSession, "")
	if len(refs) > 1000 {
		problem(w, 503, "SESSION_LIST_CAPACITY")
		return
	}
	s := a2aListSnapshot{owner: c.ID, route: rt.ID, revision: rt.Version, filter: filter, expires: time.Now().Add(2 * time.Minute), size: limit, tasks: []json.RawMessage{}}
	for _, ref := range refs {
		if ref.State == resourceTerminal || (ref.ParentID != "" && query.Get("include_children") != "true") {
			continue
		}
		l, current, err := g.acquireResource(c.ID, rt, hermesSession, ref.ID)
		if err != nil {
			hermesError(w, err)
			return
		}
		value, status, err := g.readHermesSession(r, l, c, current)
		l.release()
		if status == 404 {
			continue
		}
		if err != nil {
			problem(w, 502, "SESSION_LIST_UPSTREAM_UNAVAILABLE")
			return
		}
		session, ok := value["session"].(map[string]any)
		if !ok {
			problem(w, 502, "INVALID_HERMES_RESPONSE")
			return
		}
		if source := query.Get("source"); source != "" && session["source"] != source {
			continue
		}
		raw, _ := json.Marshal(session)
		s.bytes += len(raw)
		if s.bytes > 8<<20 {
			problem(w, 503, "SESSION_LIST_CAPACITY")
			return
		}
		s.tasks = append(s.tasks, raw)
	}
	if offset > len(s.tasks) {
		offset = len(s.tasks)
	}
	key := ""
	if offset+limit < len(s.tasks) {
		var err error
		key, err = g.cachePut("sessions", cacheWire(s))
		if err != nil {
			problem(w, 503, "SESSION_LIST_CAPACITY")
			return
		}
	}
	writeHermesPage(w, key, s, offset)
}
func writeHermesPage(w http.ResponseWriter, key string, s a2aListSnapshot, offset int) {
	end := min(offset+s.size, len(s.tasks))
	bytes := 0
	for _, raw := range s.tasks[offset:end] {
		bytes += len(raw) + 1
	}
	if bytes > 4<<20 {
		problem(w, 503, "SESSION_PAGE_TOO_LARGE")
		return
	}
	next := ""
	if end < len(s.tasks) {
		next = base64.RawURLEncoding.EncodeToString([]byte(key + ":" + strconv.Itoa(end)))
	}
	writeJSON(w, 200, map[string]any{"object": "list", "data": s.tasks[offset:end], "limit": s.size, "offset": offset, "total": len(s.tasks), "has_more": next != "", "next_page_token": next})
}
func (g *Gateway) readHermesSession(r *http.Request, l *lease, c Caller, ref resource) (map[string]any, int, error) {
	target, _ := hermesURL(l.iface.URL, "/api/sessions/"+url.PathEscape(ref.Upstream), "")
	req, err := http.NewRequestWithContext(r.Context(), "GET", target, nil)
	if err != nil {
		return nil, 0, err
	}
	upstreamHeaders(req.Header, c, l.iface)
	client := &http.Client{Transport: g.transport(l.route.Timeout.ResponseHeaderMS), Timeout: 5 * time.Second, CheckRedirect: noRedirect}
	res, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, res.StatusCode, errResourceNotFound
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		return nil, 200, errResourceInvalid
	}
	value, err := hermesJSON(raw)
	if err != nil {
		return nil, 200, err
	}
	h := hermesRequest{g: g, caller: c, rt: l.route, session: ref, op: hermesOperation{action: "session.get"}}
	err = h.observe(value, "", res.Header)
	return value, 200, err
}
