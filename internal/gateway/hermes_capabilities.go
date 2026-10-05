package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type capabilitySnapshot struct {
	Interface EndpointInterface `json:"-"`
	Checked   time.Time         `json:"checked_at"`
	Ready     bool              `json:"ready"`
	Features  map[string]any    `json:"features,omitempty"`
}

var hermesFeatures = []string{"session_resources", "session_model_lock", "session_chat", "session_chat_streaming", "session_fork", "chat_completions", "chat_completions_streaming", "responses_api", "responses_streaming", "run_submission", "run_status", "run_events_sse", "run_stop", "run_steer", "run_approval_response"}

func (g *Gateway) interfaceReady(rt *Route, ep *Endpoint, entry string) bool {
	st := g.state(rt.ID, ep.ID).iface(entry)
	if !st.healthy.Load() {
		return false
	}
	if rt.AgentType == "hermes" {
		c := st.discovery.Load()
		return c != nil && c.Interface == ep.Interfaces[entry] && c.Ready && time.Since(c.Checked) < 2*time.Minute && (entry != "a2a" || !supportsStreaming(rt) || c.Features["streaming"] == true)
	}
	return true
}

func (g *Gateway) discoverHermes(ctx context.Context, rt *Route) {
	for i := range rt.Endpoints {
		ep := &rt.Endpoints[i]
		if ep.State == "disabled" {
			continue
		}
		iface, ok := ep.Interfaces["proxy"]
		if !ok {
			continue
		}
		st := &g.state(rt.ID, ep.ID).proxy
		st.discoveryMu.Lock()
		cached := st.discovery.Load()
		if cached != nil && cached.Interface == iface && time.Since(cached.Checked) < 30*time.Second {
			st.discoveryMu.Unlock()
			continue
		}
		c := &capabilitySnapshot{Interface: iface, Checked: time.Now()}
		func() {
			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			u, err := hermesURL(iface.URL, "/v1/capabilities", "")
			if err != nil {
				return
			}
			req, err := http.NewRequestWithContext(probeCtx, "GET", u, nil)
			if err != nil {
				return
			}
			upstreamHeaders(req.Header, Caller{}, iface)
			client := &http.Client{Transport: g.transport(3000), CheckRedirect: noRedirect}
			res, err := client.Do(req)
			if err != nil {
				return
			}
			defer res.Body.Close()
			if res.StatusCode != 200 {
				return
			}
			raw, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
			if err != nil || len(raw) > 1<<20 {
				return
			}
			var body struct {
				Features map[string]any `json:"features"`
			}
			if json.Unmarshal(raw, &body) != nil {
				return
			}
			c.Features = map[string]any{}
			c.Ready = true
			for _, feature := range hermesFeatures {
				v := body.Features[feature] == true
				c.Features[feature] = v
				c.Ready = c.Ready && v
			}
			idem, _ := body.Features["runs_idempotency"].(map[string]any)
			c.Features["run_events_replay"] = body.Features["run_events_replay"] == true
			c.Features["runs_idempotency"] = map[string]any{"supported": idem["supported"] == true, "durable": false}
			c.Ready = c.Ready && idem["supported"] == true
		}()
		st.discovery.Store(c)
		st.discoveryMu.Unlock()
		if ctx.Err() != nil {
			return
		}
	}
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func (g *Gateway) discoverA2A(ctx context.Context, rt *Route) {
	if rt.AgentType != "hermes" {
		return
	}
	for i := range rt.Endpoints {
		ep := &rt.Endpoints[i]
		iface, ok := ep.Interfaces["a2a"]
		if !ok || ep.State == "disabled" {
			continue
		}
		st := &g.state(rt.ID, ep.ID).a2a
		st.discoveryMu.Lock()
		cached := st.discovery.Load()
		if cached != nil && cached.Interface == iface && time.Since(cached.Checked) < 30*time.Second {
			st.discoveryMu.Unlock()
			continue
		}
		c := &capabilitySnapshot{Interface: iface, Checked: time.Now()}
		func() {
			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			target, err := hermesURL(iface.URL, "/.well-known/agent-card.json", "")
			if err != nil {
				return
			}
			req, err := http.NewRequestWithContext(probeCtx, "GET", target, nil)
			if err != nil {
				return
			}
			upstreamHeaders(req.Header, Caller{}, iface)
			client := &http.Client{Transport: g.transport(3000), CheckRedirect: noRedirect}
			res, err := client.Do(req)
			if err != nil {
				return
			}
			defer res.Body.Close()
			if res.StatusCode != 200 {
				return
			}
			raw, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
			if err != nil || len(raw) > 1<<20 {
				return
			}
			var card struct {
				SupportedInterfaces []struct {
					ProtocolVersion string `json:"protocolVersion"`
					ProtocolBinding string `json:"protocolBinding"`
				} `json:"supportedInterfaces"`
				Capabilities struct {
					Streaming bool `json:"streaming"`
				} `json:"capabilities"`
			}
			if json.Unmarshal(raw, &card) != nil {
				return
			}
			for _, entry := range card.SupportedInterfaces {
				if entry.ProtocolVersion == "1.0" && entry.ProtocolBinding == "JSONRPC" {
					c.Ready = true
				}
			}

			c.Features = map[string]any{"streaming": card.Capabilities.Streaming, "protocol_version": "1.0"}
		}()
		st.discovery.Store(c)
		st.discoveryMu.Unlock()
		if ctx.Err() != nil {
			return
		}
	}
}
