package rollout

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	HTTP  *http.Client
	Token string
}

func NewClient(socket, token string) *Client {
	return &Client{Token: token, HTTP: &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}}
}
func (c *Client) Call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://control"+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("control HTTP %d: %s", res.StatusCode, b)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(out)
}
func (c *Client) Traffic(ctx context.Context) (p.Traffic, error) {
	var t p.Traffic
	err := c.Call(ctx, "GET", "/control/v1/traffic", nil, &t)
	return t, err
}
func (c *Client) Apply(ctx context.Context, expected uint64, t p.Traffic) error {
	return c.Call(ctx, "PUT", "/control/v1/traffic", struct {
		Expected uint64
		Traffic  p.Traffic
	}{expected, t}, nil)
}
func (c *Client) Stats(ctx context.Context, since time.Time, worker string) (p.Statistics, error) {
	var s p.Statistics
	err := c.Call(ctx, "GET", "/control/v1/stats?since="+url.QueryEscape(since.Format(time.RFC3339Nano))+"&worker="+url.QueryEscape(worker), nil, &s)
	return s, err
}
func (c *Controller) Handler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(401)
			return
		}
		var err error
		switch {
		case r.URL.Path == "/control/v1/rollout" && r.Method == "GET":
		case r.URL.Path == "/control/v1/rollout" && r.Method == "POST":
			var q struct {
				Artifact string
				Policy   p.Policy
			}
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&q) != nil {
				w.WriteHeader(400)
				return
			}
			err = c.Begin(q.Artifact, q.Policy)
		case r.URL.Path == "/control/v1/rollout/resume" && r.Method == "POST":
			err = c.Resume()
		case r.URL.Path == "/control/v1/rollout/rollback" && r.Method == "POST":
			err = c.Rollback()
		default:
			w.WriteHeader(404)
			return
		}
		if err != nil {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(c.Report())
	})
}
