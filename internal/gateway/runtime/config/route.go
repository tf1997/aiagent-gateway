package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"aiagent-gateway/internal/gateway/protocol"
)

type Timeout = protocol.Timeout

type SessionPolicy = protocol.SessionPolicy

type Endpoint = protocol.Endpoint

type EndpointInterface = protocol.EndpointInterface

type Route = protocol.Route

type Audit = protocol.Audit

type Receipt = protocol.Receipt

type Config = protocol.Config

type Caller = protocol.Caller

type Consumer = protocol.Consumer

type Snapshot struct {
	Config Config
	Routes map[string]*Route
}

var validKey = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

func Tag(v uint64) string { return fmt.Sprintf(`"%d"`, v) }

func Defaults(r *Route) {
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

func ValidURL(s string) bool {
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.Fragment == ""
}

func Validate(c *Config) error {
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
				if (entry != "proxy" && entry != "a2a") || !ValidURL(iface.URL) {
					return fmt.Errorf("invalid endpoint interface")
				}
				if iface.HealthURL != "" && !ValidURL(iface.HealthURL) {
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
		if protocol.Exposes(r, "a2a") && len(r.Card) != 0 {
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

func Build(c Config) *Snapshot {
	s := &Snapshot{Config: c, Routes: make(map[string]*Route, len(c.Routes))}
	for i := range s.Config.Routes {
		r := &s.Config.Routes[i]
		r.BuildEndpointIndex()
		s.Routes[r.Key] = r
	}
	return s
}

func endpoint(r *Route, id string) *Endpoint { return r.Endpoint(id) }

func (s *Snapshot) RoutesByID(id string) *Route {
	for _, r := range s.Routes {
		if r.ID == id {
			return r
		}
	}
	return nil
}
