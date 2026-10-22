package process

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/entry"
	p "aiagent-gateway/internal/releaseprotocol"
)

type allocationRequest struct {
	Request p.Request `json:"request"`
	ID      string    `json:"id"`
}

func (c *CandidateProcess) sourceHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(p.WorkerHeader) != "" {
			n := c.runtimeOwner.Load()
			if n == nil || !n.StorageHealthy() || subtle.ConstantTimeCompare([]byte(r.Header.Get(p.AuthHeader)), []byte(c.token)) != 1 ||
				r.Header.Get(p.WorkerHeader) != n.target.Incarnation || !n.entry.AuthorizeExecution(c.target.Incarnation, r.Header.Get(p.RequestHeader), n.target.Incarnation) {
				http.Error(w, "INVALID_EXECUTION_ALLOCATION", http.StatusForbidden)
				return
			}
			r.Header.Set(p.AuthHeader, n.config.AdminToken)
			n.business.WorkerHandler(n.identity).ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/_process/") {
			c.stateOwner.Load().HandlerFor(c.token, c.target.Incarnation).ServeHTTP(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get(p.AuthHeader)), []byte(c.token)) != 1 {
			http.Error(w, "INVALID_ENTRY_PEER", 403)
			return
		}
		n := c.runtimeOwner.Load()
		if n == nil || !n.StorageHealthy() {
			http.Error(w, "STATE_UNAVAILABLE", 503)
			return
		}
		if r.URL.Path == "/_process/admin" || strings.HasPrefix(r.URL.Path, "/_process/admin/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/_process")
			n.AdminHandler().ServeHTTP(w, r)
			return
		}
		var out any
		var err error
		decode := func(v any) error {
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
			d.DisallowUnknownFields()
			if e := d.Decode(v); e != nil {
				return e
			}
			if d.Decode(new(any)) != io.EOF {
				return fmt.Errorf("INVALID_ENTRY_REQUEST")
			}
			return nil
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/_process/entry/ready":
			out = map[string]bool{"ready": n.StorageHealthy()}
		case r.Method == "POST" && r.URL.Path == "/_process/entry/allocate":
			var q allocationRequest
			if err = decode(&q); err == nil {
				out, err = n.entry.Allocate(r.Context(), q.Request, c.target.Incarnation, q.ID)
			}
		case r.Method == "POST" && r.URL.Path == "/_process/entry/complete":
			var q p.Result
			if err = decode(&q); err == nil {
				if q.Ingress != c.target.Incarnation {
					err = fmt.Errorf("INVALID_INGRESS_IDENTITY")
				} else {
					err = n.entry.Complete(r.Context(), q)
				}
			}
		case r.Method == "POST" && r.URL.Path == "/_process/entry/abandon":
			var q struct {
				ID string `json:"id"`
			}
			if err = decode(&q); err == nil {
				err = n.entry.AbandonAllocation(r.Context(), c.target.Incarnation, q.ID)
			}
		default:
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(503)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	})
}

type candidateAllocator struct {
	owner          atomic.Pointer[Gateway]
	client         *http.Client
	token, ingress string
	mu             sync.Mutex
	pending        map[string]p.Result
	unknown        map[string]bool
	source         p.Target
	stateSocket    string
	lost           bool
}

func newCandidateAllocator(boot candidateBoot) *candidateAllocator {
	return &candidateAllocator{token: boot.Token, ingress: boot.Target.Incarnation, source: boot.Source, stateSocket: boot.StateSocket, unknown: map[string]bool{}, pending: map[string]p.Result{}, client: &http.Client{Timeout: time.Duration(boot.Policy.StateWaitMillis) * time.Millisecond, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", boot.StateSocket)
	}}}}
}

func (a *candidateAllocator) healthy() bool { a.mu.Lock(); defer a.mu.Unlock(); return !a.lost }

func (a *candidateAllocator) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://source"+path, body)
	if err != nil {
		return err
	}
	req.Header.Set(p.AuthHeader, a.token)
	res, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("SOURCE_ENTRY_HTTP_%d", res.StatusCode)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 8<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(out)
}

func (a *candidateAllocator) Ready(ctx context.Context) bool {
	if !a.healthy() {
		return false
	}
	if n := a.owner.Load(); n != nil {
		return n.Ready()
	}
	var ready struct {
		Ready bool `json:"ready"`
	}
	return a.call(ctx, "GET", "/_process/entry/ready", nil, &ready) == nil && ready.Ready
}

func (a *candidateAllocator) Allocate(ctx context.Context, q p.Request, ingress, id string) (entry.Allocation, error) {
	if ingress != a.ingress {
		return entry.Allocation{}, fmt.Errorf("INVALID_LOCAL_INGRESS")
	}
	if !a.healthy() {
		return entry.Allocation{}, fmt.Errorf("ENTRY_RECEIPTS_LOST")
	}
	if n := a.owner.Load(); n != nil {
		return n.entry.Allocate(ctx, q, ingress, id)
	}
	var out entry.Allocation
	err := a.call(ctx, "POST", "/_process/entry/allocate", allocationRequest{q, id}, &out)
	if err != nil {
		a.mu.Lock()
		if len(a.pending)+len(a.unknown) >= 8192 {
			a.lost = true
		} else {
			a.unknown[id] = true
		}
		a.mu.Unlock()
	} else if out.Target == a.source {
		out.Target.Socket = a.stateSocket
	}
	return out, err
}

func (a *candidateAllocator) complete(ctx context.Context, r p.Result) error {
	if n := a.owner.Load(); n != nil {
		return n.entry.Complete(ctx, r)
	}
	return a.call(ctx, "POST", "/_process/entry/complete", r, nil)
}

func (a *candidateAllocator) Complete(ctx context.Context, r p.Result) error {
	a.mu.Lock()
	if len(a.pending)+len(a.unknown) >= 8192 {
		a.lost = true
		a.mu.Unlock()
		return fmt.Errorf("ENTRY_RECEIPT_CAPACITY")
	}
	a.pending[r.ID] = r
	a.mu.Unlock()
	if err := a.complete(ctx, r); err != nil {
		return err
	}
	a.mu.Lock()
	delete(a.pending, r.ID)
	a.mu.Unlock()
	return nil
}

func (a *candidateAllocator) retry(ctx context.Context) {
	a.mu.Lock()
	pending := make([]p.Result, 0, len(a.pending))
	for _, r := range a.pending {
		pending = append(pending, r)
	}
	unknown := make([]string, 0, len(a.unknown))
	for id := range a.unknown {
		unknown = append(unknown, id)
	}
	a.mu.Unlock()
	for _, r := range pending {
		if a.complete(ctx, r) == nil {
			a.mu.Lock()
			delete(a.pending, r.ID)
			a.mu.Unlock()
		}
	}
	for _, id := range unknown {
		var err error
		if n := a.owner.Load(); n != nil {
			err = n.entry.AbandonAllocation(ctx, a.ingress, id)
		} else {
			err = a.call(ctx, "POST", "/_process/entry/abandon", map[string]string{"id": id}, nil)
		}
		if err == nil {
			a.mu.Lock()
			delete(a.unknown, id)
			a.mu.Unlock()
		}
	}
}

func (c *Candidate) publicAdmin() http.Handler {
	u, _ := url.Parse("http://source")
	proxy := &httputil.ReverseProxy{Transport: c.allocator.client.Transport, Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(u)
		pr.Out.URL.Path = "/_process" + pr.In.URL.Path
		pr.Out.Header.Set(p.AuthHeader, c.boot.Token)
	}}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := c.allocator.owner.Load(); n != nil {
			n.AdminHandler().ServeHTTP(w, r)
			return
		}
		proxy.ServeHTTP(w, r)
	})
}
