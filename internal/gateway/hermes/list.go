package hermes

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

func (g *Handler) listHermesSessions(w http.ResponseWriter, r *http.Request, c protocol.Caller, rt *protocol.Route) {
	release, err := g.AcquireList("sessions")
	if err != nil {
		protocol.Problem(w, 503, "SESSION_LIST_CAPACITY")
		return
	}
	defer release()
	query := r.URL.Query()
	for key, values := range query {
		if len(values) != 1 || (key != "limit" && key != "offset" && key != "page_token" && key != "source" && key != "include_children") {
			protocol.Problem(w, 400, "INVALID_PAGINATION")
			return
		}
	}
	limit := 50
	if query.Get("limit") != "" {
		n, err := strconv.Atoi(query.Get("limit"))
		if err != nil || n < 1 || n > 200 {
			protocol.Problem(w, 400, "INVALID_PAGINATION")
			return
		}
		limit = n
	}
	offset := 0
	if query.Get("offset") != "" {
		n, err := strconv.Atoi(query.Get("offset"))
		if err != nil || n < 0 {
			protocol.Problem(w, 400, "INVALID_PAGINATION")
			return
		}
		offset = n
	}
	if v := query.Get("include_children"); v != "" && v != "true" && v != "false" {
		protocol.Problem(w, 400, "INVALID_PAGINATION")
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
		record, found := g.CacheGet("sessions", key)
		s := record
		if err != nil || e != nil || !ok || !found || s.Owner != c.ID || s.Route != rt.ID || s.Revision != rt.Version || s.Filter != filter || n < 0 || n >= len(s.Tasks) || (offset != 0 && offset != n) {
			protocol.Problem(w, 400, "INVALID_PAGE_TOKEN")
			return
		}
		writeHermesPage(w, key, s, n)
		return
	}
	refs := g.Resources.List(c.ID, rt.ID, protocol.HermesSession, "")
	if len(refs) > 1000 {
		protocol.Problem(w, 503, "SESSION_LIST_CAPACITY")
		return
	}
	s := protocol.ListSnapshot{Owner: c.ID, Route: rt.ID, Revision: rt.Version, Filter: filter, Expires: time.Now().Add(2 * time.Minute), Size: limit, Tasks: []json.RawMessage{}}
	for _, ref := range refs {
		if ref.State == protocol.ResourceTerminal || (ref.ParentID != "" && query.Get("include_children") != "true") {
			continue
		}
		l, current, err := g.AcquireResource(c.ID, rt, protocol.HermesSession, ref.ID)
		if err != nil {
			hermesError(w, err)
			return
		}
		value, status, err := g.readHermesSession(r, l, c, current)
		l.Release()
		if status == 404 {
			continue
		}
		if err != nil {
			protocol.Problem(w, 502, "SESSION_LIST_UPSTREAM_UNAVAILABLE")
			return
		}
		session, ok := value["session"].(map[string]any)
		if !ok {
			protocol.Problem(w, 502, "INVALID_HERMES_RESPONSE")
			return
		}
		if source := query.Get("source"); source != "" && session["source"] != source {
			continue
		}
		raw, _ := json.Marshal(session)
		s.Bytes += len(raw)
		if s.Bytes > 8<<20 {
			protocol.Problem(w, 503, "SESSION_LIST_CAPACITY")
			return
		}
		s.Tasks = append(s.Tasks, raw)
	}
	if offset > len(s.Tasks) {
		offset = len(s.Tasks)
	}
	key := ""
	if offset+limit < len(s.Tasks) {
		var err error
		key, err = g.CachePut("sessions", s)
		if err != nil {
			protocol.Problem(w, 503, "SESSION_LIST_CAPACITY")
			return
		}
	}
	writeHermesPage(w, key, s, offset)
}

func writeHermesPage(w http.ResponseWriter, key string, s protocol.ListSnapshot, offset int) {
	end := min(offset+s.Size, len(s.Tasks))
	bytes := 0
	for _, raw := range s.Tasks[offset:end] {
		bytes += len(raw) + 1
	}
	if bytes > 4<<20 {
		protocol.Problem(w, 503, "SESSION_PAGE_TOO_LARGE")
		return
	}
	next := ""
	if end < len(s.Tasks) {
		next = base64.RawURLEncoding.EncodeToString([]byte(key + ":" + strconv.Itoa(end)))
	}
	protocol.WriteJSON(w, 200, map[string]any{"object": "list", "data": s.Tasks[offset:end], "limit": s.Size, "offset": offset, "total": len(s.Tasks), "has_more": next != "", "next_page_token": next})
}

func (g *Handler) readHermesSession(r *http.Request, l *protocol.Lease, c protocol.Caller, ref protocol.Resource) (map[string]any, int, error) {
	target, _ := protocol.UpstreamURL(l.Interface.URL, "/api/sessions/"+url.PathEscape(ref.Upstream), "")
	req, err := http.NewRequestWithContext(r.Context(), "GET", target, nil)
	if err != nil {
		return nil, 0, err
	}
	g.UpstreamHeaders(req.Header, c, l.Interface)
	client := &http.Client{Transport: g.Transport(l.Route.Timeout.ResponseHeaderMS), Timeout: 5 * time.Second, CheckRedirect: protocol.NoRedirect}
	res, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, res.StatusCode, protocol.ErrResourceNotFound
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 {
		return nil, 200, protocol.ErrResourceInvalid
	}
	value, err := protocol.JSONObject(raw)
	if err != nil {
		return nil, 200, err
	}
	h := hermesRequest{g: g, caller: c, rt: l.Route, session: ref, op: hermesOperation{action: "session.get"}}
	err = h.observe(value, "", res.Header)
	return value, 200, err
}
