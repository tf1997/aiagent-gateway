package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

type Unavailable struct{ error }

type Client struct {
	gate                  Barrier
	accessMu              sync.RWMutex
	local                 *Server
	epochMu               sync.Mutex
	epoch                 string
	socket, token, worker string
	client                *http.Client
	failed                atomic.Bool
	active                atomic.Int64
	sequence              atomic.Uint64
	stream                string
	pendingMu             sync.Mutex
	pending               map[string]*pendingRelease
}

type pendingRelease struct {
	mu      sync.Mutex
	request Request
	done    bool
}

func NewClient(socket, token, worker string) *Client {
	return &Client{socket: socket, token: token, worker: worker, stream: releaseprotocol.ID(""), client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}}
}

// NewLocalClient uses the same dispatcher and value-copy boundary as a UDS
// client. It owns no network connections and never closes the shared server.
func NewLocalClient(server *Server, worker string) *Client {
	return &Client{local: server, worker: worker, stream: releaseprotocol.ID("")}
}

func (c *Client) Call(action string, q Request, out *Response) error {
	return c.CallContext(context.Background(), action, q, out)
}

func (c *Client) CallContext(ctx context.Context, action string, q Request, out *Response) error {
	size, err := json.Marshal(q)
	if err != nil {
		return err
	}
	leave, err := c.gate.Wait(ctx, len(size))
	if err != nil {
		return err
	}
	defer leave()
	c.accessMu.RLock()
	defer c.accessMu.RUnlock()
	if c.failed.Load() {
		return fmt.Errorf("SHARED_STATE_UNAVAILABLE")
	}
	c.epochMu.Lock()
	q.Epoch = c.epoch
	c.epochMu.Unlock()
	q.Action = action
	q.Worker = c.worker
	if q.ID == "" {
		q.ID = c.operationID()
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return err
	}
	if c.local != nil {
		var copy Request
		if err = json.Unmarshal(raw, &copy); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return c.accept(c.local.dispatch(ctx, copy), out)
	}
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://state/control/v1/state", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+c.token)
		res, err := c.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		var v Response
		err = json.NewDecoder(io.LimitReader(res.Body, 64<<20)).Decode(&v)
		res.Body.Close()
		if err != nil {
			last = err
			continue
		}
		if res.StatusCode != 200 {
			last = fmt.Errorf("shared state HTTP %d", res.StatusCode)
			break
		}
		return c.accept(v, out)
	}
	c.failed.Store(true)
	return fmt.Errorf("SHARED_STATE_UNAVAILABLE: %w", last)
}

func (c *Client) ConfigureHandoffQueue(limits QueueLimits) error { return c.gate.Reconfigure(limits) }
func (c *Client) Quiesce(ctx context.Context) error              { return c.gate.Seal(ctx) }
func (c *Client) ResumeCalls() error                             { return c.gate.Resume() }
func (c *Client) QueueStatus() BarrierStatus                     { return c.gate.Status() }

func (c *Client) accept(v Response, out *Response) error {
	if v.Epoch != "" {
		c.epochMu.Lock()
		if c.epoch == "" {
			c.epoch = v.Epoch
		}
		changed := c.epoch != v.Epoch
		c.epochMu.Unlock()
		if changed {
			c.failed.Store(true)
			return fmt.Errorf("STATE_GENERATION_CHANGED")
		}
	}
	if v.Error != "" {
		return resourceError(v.Error)
	}
	if out != nil {
		*out = v
	}
	return nil
}

func resourceError(s string) error {
	for _, e := range []error{protocol.ErrResourceNotFound, protocol.ErrResourceConflict, protocol.ErrResourceCapacity, protocol.ErrResourceInUse, protocol.ErrResourceInvalid} {
		if e.Error() == s {
			return e
		}
	}
	return errors.New(s)
}

func (c *Client) Must(action string, q Request, out *Response) {
	if err := c.Call(action, q, out); err != nil {
		panic(Unavailable{err})
	}
}

func (c *Client) Release(id string) func() {
	c.active.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			op := &pendingRelease{request: Request{ID: c.operationID(), Lease: id}}
			c.pendingMu.Lock()
			if c.pending == nil {
				c.pending = map[string]*pendingRelease{}
			}
			c.pending[op.request.ID] = op
			c.pendingMu.Unlock()
			_ = c.retryRelease(context.Background(), op)
		})
	}
}

// Sequence is part of operation identity and therefore survives every retry.
// Arbitrary caller-supplied IDs remain valid while their receipt is retained;
// after eviction they fail closed instead of silently executing again.
func (c *Client) operationID() string {
	return fmt.Sprintf("op_%s_%020d", c.stream, c.sequence.Add(1))
}

func operationSequence(id string) (string, uint64) {
	if len(id) != 56 || !strings.HasPrefix(id, "op_") || id[35] != '_' {
		return "", 0
	}
	for _, ch := range id[3:35] {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return "", 0
		}
	}
	n, err := strconv.ParseUint(id[36:], 10, 64)
	if err != nil || n == 0 {
		return "", 0
	}
	return id[3:35], n
}

func (c *Client) retryRelease(ctx context.Context, op *pendingRelease) error {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.done {
		return nil
	}
	if err := c.CallContext(ctx, "release", op.request, nil); err != nil {
		return err
	}
	op.done = true
	c.active.Add(-1)
	c.pendingMu.Lock()
	delete(c.pending, op.request.ID)
	c.pendingMu.Unlock()
	return nil
}

// RetryReleases retries only state receipts, with the original operation ID.
// A timeout during sealing must not turn a live lease into a forgotten release.
// The coordinator calls this after adopting and resuming the new dispatcher.
func (c *Client) RetryReleases(ctx context.Context) error {
	c.pendingMu.Lock()
	var pending []*pendingRelease
	for _, op := range c.pending {
		pending = append(pending, op)
	}
	c.pendingMu.Unlock()
	for _, op := range pending {
		if err := c.retryRelease(ctx, op); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Close() {
	c.accessMu.Lock()
	defer c.accessMu.Unlock()
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
}

// AdoptLocal is an explicit transition after the coordinator has verified a
// committed owner. Unexpected epoch changes still fail closed. No operation
// may straddle the transport switch; waiting respects the handoff deadline.
func (c *Client) AdoptLocal(ctx context.Context, previousEpoch, nextEpoch string, server *Server) error {
	if server == nil {
		return fmt.Errorf("missing state owner")
	}
	for !c.accessMu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer c.accessMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	server.mu.Lock()
	defer server.mu.Unlock()
	if previousEpoch == "" || c.epoch != previousEpoch || server.previousEpoch != previousEpoch || server.epoch != nextEpoch || nextEpoch == previousEpoch {
		return fmt.Errorf("STATE_ADOPTION_MISMATCH")
	}
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
	c.local, c.epoch = server, nextEpoch
	c.failed.Store(false)
	return nil
}

// RestoreRemote is only called after the coordinator has durably aborted a
// yielded handoff and restored its original epoch. It cannot accept an
// unexpected generation or change the authenticated remote endpoint.
func (c *Client) RestoreRemote(ctx context.Context, epoch string) error {
	for !c.accessMu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer c.accessMu.Unlock()
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	if epoch == "" || c.epoch != epoch || c.local != nil || c.client == nil {
		return fmt.Errorf("STATE_RESTORATION_MISMATCH")
	}
	probe := NewClient(c.socket, c.token, c.worker)
	defer probe.Close()
	var response Response
	if err := probe.CallContext(ctx, "config", Request{}, &response); err != nil {
		return err
	}
	if response.Epoch != epoch {
		return fmt.Errorf("STATE_RESTORATION_MISMATCH")
	}
	c.client.CloseIdleConnections()
	c.failed.Store(false)
	return nil
}
func (c *Client) Failed() bool  { return c.failed.Load() }
func (c *Client) Active() int64 { return c.active.Load() }

func Control(socket, token, worker, action string, flag bool) (int64, error) {
	c := NewClient(socket, token, worker)
	defer c.Close()
	var out Response
	err := c.Call(action, Request{Flag: flag}, &out)
	return out.Count, err
}

func Resolve(socket, token string, q releaseprotocol.Request) (releaseprotocol.Resolution, error) {
	c := NewClient(socket, token, "entry")
	defer c.Close()
	var out Response
	err := c.Call("resolve", Request{Request: q}, &out)
	return out.Resolution, err
}
