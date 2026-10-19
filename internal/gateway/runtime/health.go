package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
)

type capabilitySnapshot = protocol.Capabilities

var hermesFeatures = []string{"session_resources", "session_model_lock", "session_chat", "session_chat_streaming", "session_fork", "chat_completions", "chat_completions_streaming", "responses_api", "responses_streaming", "run_submission", "run_status", "run_events_sse", "run_stop", "run_steer", "run_approval_response"}

func (g *Gateway) interfaceReady(rt *protocol.Route, ep *protocol.Endpoint, entry string) bool {
	// Capability discovery is diagnostic: older Hermes versions can serve
	// requests without meeting the full capability baseline.
	return g.state(rt.ID, ep.ID).iface(entry).healthy.Load()
}

func (g *Gateway) discoverHermes(ctx context.Context, rt *protocol.Route) {
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
			u, err := protocol.UpstreamURL(iface.URL, "/v1/capabilities", "")
			if err != nil {
				return
			}
			req, err := http.NewRequestWithContext(probeCtx, "GET", u, nil)
			if err != nil {
				return
			}
			g.upstreamHeaders(req.Header, protocol.Caller{}, iface)
			client := &http.Client{Transport: g.transport(3000), CheckRedirect: protocol.NoRedirect}
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

func (g *Gateway) discoverA2A(ctx context.Context, rt *protocol.Route) {
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
			target, err := protocol.UpstreamURL(iface.URL, "/.well-known/agent-card.json", "")
			if err != nil {
				return
			}
			req, err := http.NewRequestWithContext(probeCtx, "GET", target, nil)
			if err != nil {
				return
			}
			g.upstreamHeaders(req.Header, protocol.Caller{}, iface)
			client := &http.Client{Transport: g.transport(3000), CheckRedirect: protocol.NoRedirect}
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

type interfaceState struct {
	healthy     atomic.Bool
	active      atomic.Int64
	discovery   atomic.Pointer[capabilitySnapshot]
	discoveryMu sync.Mutex
}

type endpointState struct {
	active atomic.Int64
	proxy  interfaceState
	a2a    interfaceState
}

func (s *endpointState) iface(entry string) *interfaceState {
	if entry == "a2a" {
		return &s.a2a
	}
	return &s.proxy
}

func (g *Gateway) state(route, id string) *endpointState {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	k := route + "/" + id
	s := g.states[k]
	if s == nil {
		s = &endpointState{}
		s.proxy.healthy.Store(true)
		s.a2a.healthy.Store(true)
		g.states[k] = s
	}
	return s
}

func (g *Gateway) checkHealth(ctx context.Context) {
	leave, err := g.stateBarrier.TryEnter()
	if err != nil {
		return
	}
	defer leave()
	if ctx.Err() != nil {
		return
	}
	client := &http.Client{Transport: g.transport(3000), Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, r := range g.current.Load().Routes {
		if r.AgentType == "hermes" {
			g.discoverHermes(ctx, r)
			g.discoverA2A(ctx, r)
		}
		for _, ep := range r.Endpoints {
			for entry, iface := range ep.Interfaces {
				if iface.HealthURL == "" || ep.State == "disabled" || !protocol.Exposes(r, entry) {
					continue
				}
				req, err := http.NewRequestWithContext(ctx, "GET", iface.HealthURL, nil)
				if err != nil {
					continue
				}
				if iface.Token != "" {
					req.Header.Set("Authorization", "Bearer "+iface.Token)
				}
				res, err := client.Do(req)
				ok := err == nil
				if res != nil {
					ok = res.StatusCode >= 200 && res.StatusCode < 300
					res.Body.Close()
				}
				g.lifecycle.RLock()
				latest := g.current.Load().Routes[r.Key]
				if latest != nil && latest.ID == r.ID {
					currentEP := latest.Endpoint(ep.ID)
					if currentEP != nil && currentEP.Interfaces[entry] == iface {
						g.state(r.ID, ep.ID).iface(entry).healthy.Store(ok)
					}
				}
				g.lifecycle.RUnlock()
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

// Called under the lifecycle write lock before publishing a new configuration.
func (g *Gateway) resetHealth(old *configuration.Snapshot, next *protocol.Config) {
	for _, rt := range next.Routes {
		previous := old.Routes[rt.Key]
		if previous == nil || previous.ID != rt.ID {
			continue
		}
		for _, ep := range rt.Endpoints {
			before := previous.Endpoint(ep.ID)
			if before == nil {
				continue
			}
			for entry, iface := range ep.Interfaces {
				if before.Interfaces[entry] != iface {
					g.state(rt.ID, ep.ID).iface(entry).healthy.Store(true)
				}
			}
		}
	}
}
