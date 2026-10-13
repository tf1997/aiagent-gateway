package protocol

import (
	"encoding/json"
	"time"
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

type Caller struct {
	ID        string
	Reconcile bool `json:"-"`
}

// Consumer is the persisted form. Plaintext tokens exist only in management
// input, never in snapshots, receipts, or management responses.
type Consumer struct {
	ID        string `json:"id"`
	TokenHash string `json:"token_hash"`
}

func (r *Route) BuildEndpointIndex() {
	r.endpointIndex = make(map[string]*Endpoint, len(r.Endpoints))
	for i := range r.Endpoints {
		r.endpointIndex[r.Endpoints[i].ID] = &r.Endpoints[i]
	}
}

func (r *Route) Endpoint(id string) *Endpoint {
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
