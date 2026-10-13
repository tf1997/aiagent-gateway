package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
)

type Timeout struct {
	ResponseHeaderMS int `json:"response_header_ms"`
	StreamIdleMS     int `json:"stream_idle_ms"`
	MaxDurationMS    int `json:"max_duration_ms"`
}
type SessionPolicy struct {
	Enabled          bool   `json:"enabled"`
	Header           string `json:"header"`
	IdleTTLSeconds   int    `json:"idle_ttl_seconds"`
	OnUnavailable    string `json:"on_endpoint_unavailable"`
	OnExpired        string `json:"on_expired"`
	AcceptA2AContext bool   `json:"accept_a2a_context"`
}
type Endpoint struct {
	ID         string                       `json:"id"`
	Name       string                       `json:"name"`
	Weight     int                          `json:"weight"`
	State      string                       `json:"state"`
	Interfaces map[string]EndpointInterface `json:"interfaces"`
	Version    uint64                       `json:"version"`
}
type EndpointInterface struct {
	URL             string `json:"url"`
	Token           string `json:"token,omitempty"`
	HealthURL       string `json:"health_url,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`
	Tenant          string `json:"tenant,omitempty"`
}
type Route struct {
	endpointIndex  map[string]*Endpoint
	ID             string          `json:"id"`
	Key            string          `json:"key"`
	AgentType      string          `json:"agent_type"`
	Expose         []string        `json:"expose"`
	Enabled        bool            `json:"enabled"`
	BalancePolicy  string          `json:"balance_policy"`
	Session        SessionPolicy   `json:"session_policy"`
	Timeout        Timeout         `json:"timeout"`
	Endpoints      []Endpoint      `json:"endpoints"`
	Consumers      []Consumer      `json:"consumers,omitempty"`
	AllowedMethods []string        `json:"allowed_methods,omitempty"`
	Card           json.RawMessage `json:"agent_card,omitempty"`
	Version        uint64          `json:"version"`
}
type Audit struct {
	At       time.Time `json:"at"`
	Actor    string    `json:"actor"`
	Action   string    `json:"action"`
	Path     string    `json:"path"`
	Revision uint64    `json:"revision"`
}
type Receipt struct {
	Hash string          `json:"hash"`
	Body json.RawMessage `json:"body"`
	ETag string          `json:"etag"`
}
type Config struct {
	Schema       int                `json:"schema_version"`
	Revision     uint64             `json:"config_revision"`
	Routes       []Route            `json:"routes"`
	Receipts     map[string]Receipt `json:"idempotency"`
	ReceiptOrder []string           `json:"idempotency_order"`
	Audit        []Audit            `json:"audit"`
}
type snapshot struct {
	Config Config
	Routes map[string]*Route
}
type Caller struct {
	ID        string
	reconcile bool
}

// Consumer is the persisted form. Plaintext tokens exist only in management
// input, never in snapshots, receipts, or management responses.
type Consumer struct {
	ID        string `json:"id"`
	TokenHash string `json:"token_hash"`
}
type Options struct {
	dialContext                                          func(context.Context, string, string) (net.Conn, error)
	StateSocket, WorkerID                                string
	DataDir, AdminToken, PublicURL                       string
	Initialize                                           bool
	DeferBackground                                      bool
	MaxSessions, MaxTasks, MaxInflight                   int
	MaxPerCaller, MaxPerEndpoint                         int
	MaxResources, MaxResourceAliases, MaxDataIdempotency int
	HealthInterval                                       time.Duration
}

var validKey = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}
func tag(v uint64) string { return fmt.Sprintf(`"%d"`, v) }
func defaults(r *Route) {
	if r.AgentType == "" {
		r.AgentType = "generic"
	}
	if r.BalancePolicy == "" {
		r.BalancePolicy = "weighted_round_robin"
	}
	if r.Session.Header == "" {
		r.Session.Header = "X-Session-ID"
	}
	if r.Session.IdleTTLSeconds == 0 {
		r.Session.IdleTTLSeconds = 86400
	}
	if r.Session.OnUnavailable == "" {
		r.Session.OnUnavailable = "fail"
	}
	if r.Session.OnExpired == "" {
		r.Session.OnExpired = "reselect"
	}
	if r.Timeout.ResponseHeaderMS == 0 {
		r.Timeout.ResponseHeaderMS = 10000
	}
	if r.Timeout.StreamIdleMS == 0 {
		r.Timeout.StreamIdleMS = 120000
	}
	if r.Timeout.MaxDurationMS == 0 {
		r.Timeout.MaxDurationMS = 1800000
	}
}
func validURL(s string) bool {
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.Fragment == ""
}
func validate(c *Config) error {
	if c.Schema != 2 {
		return fmt.Errorf("unsupported schema_version")
	}
	ids := map[string]bool{}
	keys := map[string]bool{}
	for i := range c.Routes {
		r := &c.Routes[i]
		if !validKey.MatchString(r.Key) || r.ID == "" || ids[r.ID] || keys[r.Key] {
			return fmt.Errorf("invalid or duplicate route")
		}
		ids[r.ID] = true
		keys[r.Key] = true
		if err := validateConsumers(r.Consumers); err != nil {
			return err
		}
		if r.AgentType != "generic" && r.AgentType != "hermes" {
			return fmt.Errorf("agent_type must be generic or hermes")
		}
		if len(r.Expose) == 0 {
			return fmt.Errorf("expose must not be empty")
		}
		entries := map[string]bool{}
		for _, entry := range r.Expose {
			if (entry != "proxy" && entry != "a2a") || entries[entry] {
				return fmt.Errorf("invalid or duplicate expose entry")
			}
			entries[entry] = true
		}
		if r.BalancePolicy != "weighted_round_robin" {
			return fmt.Errorf("unsupported balance policy")
		}
		if r.Session.IdleTTLSeconds < 1 || r.Session.IdleTTLSeconds > 31536000 || !validKey.MatchString(r.Session.Header) || r.Session.OnUnavailable != "fail" || r.Session.OnExpired != "reselect" {
			return fmt.Errorf("invalid session policy")
		}
		if r.Timeout.ResponseHeaderMS < 1 || r.Timeout.ResponseHeaderMS > 86400000 || r.Timeout.StreamIdleMS < 1 || r.Timeout.StreamIdleMS > 86400000 || r.Timeout.MaxDurationMS < 1 || r.Timeout.MaxDurationMS > 86400000 {
			return fmt.Errorf("invalid timeout")
		}
		for _, e := range r.Endpoints {
			if e.ID == "" || ids[e.ID] || e.Weight < 1 || e.Weight > 10000 || (e.State != "active" && e.State != "draining" && e.State != "disabled") {
				return fmt.Errorf("invalid endpoint")
			}
			ids[e.ID] = true
			for entry := range entries {
				if _, ok := e.Interfaces[entry]; !ok {
					return fmt.Errorf("endpoint requires interfaces.%s", entry)
				}
			}
			for entry, iface := range e.Interfaces {
				if (entry != "proxy" && entry != "a2a") || !validURL(iface.URL) {
					return fmt.Errorf("invalid endpoint interface")
				}
				if iface.HealthURL != "" && !validURL(iface.HealthURL) {
					return fmt.Errorf("invalid health_url")
				}
				if iface.Token != "" && (len(iface.Token) > 4096 || strings.ContainsAny(iface.Token, "\r\n\x00")) {
					return fmt.Errorf("invalid upstream token")
				}
				if entry == "proxy" && (iface.ProtocolVersion != "" || iface.Tenant != "") {
					return fmt.Errorf("proxy interface must not specify protocol_version or tenant")
				}
				if entry == "a2a" {
					if iface.ProtocolVersion != "1.0" {
						return fmt.Errorf("a2a protocol_version must be 1.0; upgrade upstream and configuration")
					}

				}
			}
		}
		if exposes(r, "a2a") && len(r.Card) != 0 {
			var card map[string]any
			if len(r.Card) == 0 || json.Unmarshal(r.Card, &card) != nil {
				return fmt.Errorf("a2a route requires agent_card")
			}
			for _, k := range []string{"name", "description", "version"} {
				if v, ok := card[k].(string); !ok || v == "" {
					return fmt.Errorf("agent_card requires %s", k)
				}
			}
			for _, k := range []string{"skills", "defaultInputModes", "defaultOutputModes"} {
				if _, ok := card[k].([]any); !ok {
					return fmt.Errorf("agent_card requires %s array", k)
				}
			}
		}
	}
	return nil
}
func exposes(r *Route, entry string) bool {
	for _, v := range r.Expose {
		if v == entry {
			return true
		}
	}
	return false
}
func build(c Config) *snapshot {
	s := &snapshot{Config: c, Routes: make(map[string]*Route, len(c.Routes))}
	for i := range s.Config.Routes {
		r := &s.Config.Routes[i]
		r.endpointIndex = make(map[string]*Endpoint, len(r.Endpoints))
		for j := range r.Endpoints {
			r.endpointIndex[r.Endpoints[j].ID] = &r.Endpoints[j]
		}
		s.Routes[r.Key] = r
	}
	return s
}
func endpoint(r *Route, id string) *Endpoint {
	if r.endpointIndex != nil {
		return r.endpointIndex[id]
	}
	for i := range r.Endpoints {
		if r.Endpoints[i].ID == id {
			return &r.Endpoints[i]
		}
	}
	return nil
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
func sessionID(r *http.Request, route *Route) (string, error) {
	s := r.Header.Get(route.Session.Header)
	if len(s) > 256 || strings.IndexFunc(s, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("invalid session ID")
	}
	return s, nil
}
