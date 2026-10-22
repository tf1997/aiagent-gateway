// Package entry is the stable HTTP listener used during business releases.
package entry

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Resolver func(context.Context, p.Request) (p.Resolution, error)
type Entry struct {
	mu          sync.Mutex
	j           *state.Journal
	traffic     p.Traffic
	bindings    map[string]string
	results     map[string]p.Result
	active      map[string]p.Result
	lost        bool
	counter     uint64
	resolve     Resolver
	transport   *http.Transport
	token       string
	localTarget p.Target
	local       func(http.ResponseWriter, *http.Request, func(p.Result))
	private     map[string]*http.Transport
	peerTokens  map[p.Target]string
	sealed      bool
	changed     chan struct{}
	admission   func() (func(), error)
}

func Open(j *state.Journal, token string, resolve Resolver) (*Entry, error) {
	e := &Entry{j: j, token: token, resolve: resolve, bindings: map[string]string{}, results: map[string]p.Result{}, active: map[string]p.Result{}, transport: &http.Transport{Proxy: nil, MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, ResponseHeaderTimeout: 24 * time.Hour}}
	for k, v := range j.Snapshot() {
		switch {
		case k == "entry/traffic":
			if err := json.Unmarshal(v, &e.traffic); err != nil {
				return nil, err
			}
		case k == "entry/lost":
			_ = json.Unmarshal(v, &e.lost)
		case strings.HasPrefix(k, "entry/binding/"):
			var worker string
			if json.Unmarshal(v, &worker) != nil {
				return nil, fmt.Errorf("invalid affinity")
			}
			e.bindings[strings.TrimPrefix(k, "entry/binding/")] = worker
		case strings.HasPrefix(k, "entry/result/"):
			var r p.Result
			if json.Unmarshal(v, &r) != nil {
				return nil, fmt.Errorf("invalid result")
			}
			e.results[r.ID] = r
		case strings.HasPrefix(k, "entry/active/"):
			var r p.Result
			if json.Unmarshal(v, &r) != nil {
				return nil, fmt.Errorf("invalid active request")
			}
			r.Verdict = "failure"
			r.Reason = "entry_restarted"
			r.Completed = time.Now()
			e.results[r.ID] = r
		}
	}
	if err := e.save(); err != nil {
		return nil, err
	}
	return e, nil
}
func jsonValue(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func (e *Entry) save() error {
	v := map[string]json.RawMessage{"entry/traffic": jsonValue(e.traffic), "entry/lost": jsonValue(e.lost)}
	for k, b := range e.bindings {
		v["entry/binding/"+k] = jsonValue(b)
	}
	for k, r := range e.results {
		v["entry/result/"+k] = jsonValue(r)
	}
	for k, r := range e.active {
		v["entry/active/"+k] = jsonValue(r)
	}
	return e.j.Replace("entry/", v)
}
func (e *Entry) Traffic() p.Traffic { e.mu.Lock(); defer e.mu.Unlock(); return e.traffic }
func validTarget(t p.Target) bool {
	u, err := url.Parse(t.URL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || t.Release == "" || t.Incarnation == "" {
		return false
	}
	if t.Socket != "" {
		return t.URL == "http://gateway" && filepath.IsAbs(t.Socket) && filepath.Clean(t.Socket) == t.Socket && !strings.ContainsRune(t.Socket, 0)
	}
	return u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
}

// SetLocal binds an exact process identity. Persisted traffic cannot substitute
// a different release or incarnation while reusing the local executor.
func (e *Entry) SetLocal(target p.Target, execute func(http.ResponseWriter, *http.Request, func(p.Result))) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !validTarget(target) || execute == nil || e.local != nil {
		return fmt.Errorf("invalid or duplicate local executor")
	}
	e.localTarget, e.local = target, execute
	return nil
}

// SetPeer installs an ephemeral credential for an exact candidate identity.
// It is never persisted with routing metadata or exposed in status.
func (e *Entry) SetPeer(target p.Target, token string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !validTarget(target) || token == "" {
		return fmt.Errorf("invalid private executor")
	}
	if e.peerTokens == nil {
		e.peerTokens = map[p.Target]string{}
	}
	e.peerTokens[target] = token
	return nil
}

func (e *Entry) transportFor(target p.Target) http.RoundTripper {
	if target.Socket == "" {
		return e.transport
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.private == nil {
		e.private = map[string]*http.Transport{}
	}
	if t := e.private[target.Socket]; t != nil {
		return t
	}
	t := e.transport.Clone()
	t.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", target.Socket)
	}
	e.private[target.Socket] = t
	return t
}

func (e *Entry) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.transport.CloseIdleConnections()
	for _, t := range e.private {
		t.CloseIdleConnections()
	}
}
func (e *Entry) Apply(expected uint64, next p.Traffic) error {
	leave, err := e.admit()
	if err != nil {
		return err
	}
	defer leave()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sealed {
		return fmt.Errorf("ENTRY_HANDOFF_SEALED")
	}
	if e.j.Healthy() != nil {
		return fmt.Errorf("entry persistence unavailable")
	}
	if e.traffic.Revision != expected {
		return fmt.Errorf("traffic revision conflict")
	}
	if next.Revision != expected+1 || next.Percent < 0 || next.Percent > 100 || !validTarget(next.Stable) || (next.Candidate == nil && next.Percent != 0) || (next.Candidate != nil && (!validTarget(*next.Candidate) || next.Candidate.Incarnation == next.Stable.Incarnation)) {
		return fmt.Errorf("invalid traffic configuration")
	}
	if next.Rollout != e.traffic.Rollout {
		e.results = map[string]p.Result{}
		e.lost = false
		e.bindings = map[string]string{}
	}
	if next.Candidate != nil {
		candidate := *next.Candidate
		next.Candidate = &candidate
	}
	e.traffic = next
	if next.Percent == 100 || next.Candidate == nil {
		e.bindings = map[string]string{}
	}
	return e.save()
}
func (e *Entry) Stats(since time.Time, worker string) p.Statistics {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := p.Statistics{Counts: map[string]p.Counts{}, Revision: e.traffic.Revision, Lost: e.lost || e.j.Healthy() != nil}
	for _, r := range e.active {
		if r.Incarnation == worker {
			out.Active++
		}
	}
	for _, r := range e.results {
		if r.Incarnation != worker {
			continue
		}
		if r.Verdict == "failure" {
			out.Failures++
		}
		if r.Started.Before(since) {
			continue
		}
		c := out.Counts[r.Profile]
		switch r.Verdict {
		case "success":
			c.Success++
		case "failure":
			c.Failure++
		case "excluded":
			c.Excluded++
		default:
			c.Unknown++
		}
		out.Counts[r.Profile] = c
		out.Total++
	}
	return out
}
func (e *Entry) start(key, preferred string) (p.Target, p.Result, error) {
	leave, err := e.admit()
	if err != nil {
		return p.Target{}, p.Result{}, err
	}
	defer leave()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sealed {
		return p.Target{}, p.Result{}, fmt.Errorf("ENTRY_HANDOFF_SEALED")
	}
	if e.j.Healthy() != nil {
		return p.Target{}, p.Result{}, fmt.Errorf("entry storage unavailable")
	}
	t := e.traffic.Stable
	if !validTarget(t) {
		return t, p.Result{}, fmt.Errorf("no active release")
	}
	worker := e.bindings[key]
	if worker == "" {
		worker = preferred
	}
	if c := e.traffic.Candidate; c != nil {
		if e.traffic.Percent == 100 {
			t = *c
		} else if e.traffic.Percent > 0 {
			if worker == c.Incarnation {
				t = *c
			} else if worker != t.Incarnation {
				e.counter++
				if int(e.counter%100) < e.traffic.Percent {
					t = *c
				}
			}
		}
	}
	if key != "" && e.traffic.Percent > 0 && e.traffic.Percent < 100 {
		if len(e.bindings) >= 100000 {
			return t, p.Result{}, fmt.Errorf("affinity capacity reached")
		}
		e.bindings[key] = t.Incarnation
	}
	r := p.Result{ID: p.ID("req_"), Instance: t.Instance, Incarnation: t.Incarnation, Release: t.Release, RoutingRevision: e.traffic.Revision, Started: time.Now()}
	e.active[r.ID] = r
	if e.traffic.Rollout != "" {
		if len(e.results)+len(e.active) > 8192 {
			e.lost = true
		}
		if err := e.save(); err != nil {
			delete(e.active, r.ID)
			return t, r, err
		}
	}
	return t, r, nil
}
func (e *Entry) finish(r p.Result) {
	leave, err := e.admit()
	if err != nil {
		return
	} // Seal requires the entry to have drained first.
	defer leave()
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, r.ID)
	if e.changed != nil {
		close(e.changed)
		e.changed = nil
	}
	if e.traffic.Rollout != "" {
		if len(e.results) < 8192 {
			e.results[r.ID] = r
		} else {
			e.lost = true
		}
		if e.save() != nil {
			e.lost = true
		}
	}
}
func (e *Entry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		write(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.URL.Path == "/readyz" {
		t := e.Traffic()
		if !validTarget(t.Stable) || e.j.Healthy() != nil {
			write(w, 503, map[string]bool{"ready": false})
		} else {
			write(w, 200, map[string]bool{"ready": true})
		}
		return
	}
	for k := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-gateway-rollout-") {
			r.Header.Del(k)
		}
	}
	q := p.Request{Method: r.Method, Path: r.URL.Path, Header: r.Header}
	resolved, err := e.resolve(r.Context(), q)
	if err != nil {
		write(w, 503, map[string]string{"error": "STATE_UNAVAILABLE"})
		return
	}
	if resolved.NeedBody {
		raw, er := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if er != nil {
			write(w, 413, map[string]string{"error": "REQUEST_TOO_LARGE"})
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		q.Body = raw
		q.BodyRead = true
		resolved, err = e.resolve(r.Context(), q)
		if err != nil {
			write(w, 503, map[string]string{"error": "STATE_UNAVAILABLE"})
			return
		}
	}
	if resolved.Status != 0 {
		write(w, resolved.Status, map[string]string{"error": resolved.Error})
		return
	}
	target, result, err := e.start(resolved.Key, resolved.OwnerWorker)
	if err != nil {
		write(w, 503, map[string]string{"error": "RELEASE_UNAVAILABLE"})
		return
	}
	rw := &responseWriter{ResponseWriter: w}
	var metadata http.Header
	var localResult *p.Result
	failed := ""
	panicked := false
	defer func() {
		if v := recover(); v != nil {
			panicked = true
			failed = "proxy_interrupted"
			defer func() { panic(v) }()
		}
		result.Completed = time.Now()
		result.Status = rw.status
		result.Profile = "http"
		result.Verdict = "unknown"
		result.Reason = "missing_worker_result"
		if localResult != nil {
			result.Profile, result.Verdict, result.Reason = localResult.Profile, localResult.Verdict, localResult.Reason
		}
		if metadata != nil {
			result.Profile = metadata.Get(p.ProfileTrailer)
			result.Verdict = metadata.Get(p.OutcomeTrailer)
			result.Reason = metadata.Get(p.ReasonTrailer)
			if result.Verdict != "success" && result.Verdict != "failure" && result.Verdict != "excluded" {
				result.Verdict = "unknown"
			}
		}
		if r.Context().Err() != nil && result.Verdict != "failure" {
			result.Verdict = "excluded"
			result.Reason = "client_cancelled"
		} else if failed != "" || panicked {
			result.Verdict = "failure"
			result.Reason = failed
		} else if rw.err != nil {
			result.Verdict = "excluded"
			result.Reason = "client_cancelled"
		}
		e.finish(result)
	}()
	e.mu.Lock()
	local, localTarget := e.local, e.localTarget
	peerToken := e.peerTokens[target]
	e.mu.Unlock()
	if local != nil && target == localTarget {
		r.Header.Set(p.RequestHeader, result.ID)
		local(rw, r, func(v p.Result) { localResult = &v })
		return
	}
	u, _ := url.Parse(target.URL)
	proxy := httputil.ReverseProxy{Transport: e.transportFor(target), FlushInterval: -1, Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(u)
		pr.Out.Host = pr.In.Host
		pr.Out.Header.Set(p.RequestHeader, result.ID)
		pr.Out.Header.Set(p.AuthHeader, e.token)
		if peerToken != "" {
			pr.Out.Header.Set(p.AuthHeader, peerToken)
			pr.Out.Header.Set(p.WorkerHeader, target.Incarnation)
		}
	}, ModifyResponse: func(res *http.Response) error {
		if res.Header.Get(p.WorkerHeader) != target.Incarnation {
			return fmt.Errorf("unexpected worker incarnation")
		}
		res.Header.Del(p.WorkerHeader)
		for _, key := range []string{p.OutcomeTrailer, p.ProfileTrailer, p.ReasonTrailer} {
			res.Trailer.Del(key)
		}
		res.Body = &trailerBody{ReadCloser: res.Body, finish: func() {
			metadata = res.Trailer.Clone()
			for _, key := range []string{p.OutcomeTrailer, p.ProfileTrailer, p.ReasonTrailer} {
				res.Trailer.Del(key)
			}
		}}
		return nil
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		failed = "worker_transport"
		write(w, 502, map[string]string{"error": "GATEWAY_VERSION_UNAVAILABLE"})
	}}
	proxy.ServeHTTP(rw, r)
}

type responseWriter struct {
	http.ResponseWriter
	status int
	err    error
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *responseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, err := w.ResponseWriter.Write(b)
	if err != nil {
		w.err = err
	}
	return n, err
}
func (w *responseWriter) Flush() {
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		w.err = err
	}
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (e *Entry) Control() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+e.token {
			write(w, 401, map[string]string{"error": "UNAUTHORIZED"})
			return
		}
		switch r.URL.Path {
		case "/control/v1/traffic":
			if r.Method == "GET" {
				write(w, 200, e.Traffic())
				return
			}
			if r.Method != "PUT" {
				write(w, 405, nil)
				return
			}
			var q struct {
				Expected uint64
				Traffic  p.Traffic
			}
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&q) != nil {
				write(w, 400, nil)
				return
			}
			if err := e.Apply(q.Expected, q.Traffic); err != nil {
				write(w, 409, map[string]string{"error": err.Error()})
				return
			}
			write(w, 200, e.Traffic())
		case "/control/v1/stats":
			since, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("since"))
			if err != nil {
				write(w, 400, nil)
				return
			}
			write(w, 200, e.Stats(since, r.URL.Query().Get("worker")))
		default:
			write(w, 404, nil)
		}
	})
}

type trailerBody struct {
	io.ReadCloser
	finish func()
	once   sync.Once
}

func (b *trailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.finish)
	}
	return n, err
}
