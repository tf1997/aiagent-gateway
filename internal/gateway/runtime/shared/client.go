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
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

type Unavailable struct{ error }

type Client struct {
	epochMu               sync.Mutex
	epoch                 string
	socket, token, worker string
	client                *http.Client
	failed                atomic.Bool
	active                atomic.Int64
}

func NewClient(socket, token, worker string) *Client {
	return &Client{socket: socket, token: token, worker: worker, client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}}
}

func (c *Client) Call(action string, q Request, out *Response) error {
	if c.failed.Load() {
		return fmt.Errorf("SHARED_STATE_UNAVAILABLE")
	}
	c.epochMu.Lock()
	q.Epoch = c.epoch
	c.epochMu.Unlock()
	q.Action = action
	q.Worker = c.worker
	if q.ID == "" {
		q.ID = releaseprotocol.ID("op_")
	}
	raw, _ := json.Marshal(q)
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		req, _ := http.NewRequest("POST", "http://state/control/v1/state", bytes.NewReader(raw))
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
	c.failed.Store(true)
	return fmt.Errorf("SHARED_STATE_UNAVAILABLE: %w", last)
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
	return func() { once.Do(func() { _ = c.Call("release", Request{Lease: id}, nil); c.active.Add(-1) }) }
}

func (c *Client) Close()        { c.client.CloseIdleConnections() }
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
