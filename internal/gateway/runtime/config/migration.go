package config

import (
	"encoding/json"
	"errors"
	"fmt"
)

var errConfigConflict = errors.New("CONFIG_SHAPE_CONFLICT")

// Versioned DTOs keep legacy fields out of schema 2 and all new responses.
type legacyEndpoint struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	URL           string `json:"url"`
	Weight        int    `json:"weight"`
	State         string `json:"state"`
	Token         string `json:"token,omitempty"`
	CredentialRef string `json:"credential_ref,omitempty"`
	HealthURL     string `json:"health_url,omitempty"`
	Version       uint64 `json:"version"`
}

type legacyRoute struct {
	ID             string           `json:"id"`
	Key            string           `json:"key"`
	Protocol       string           `json:"protocol"`
	Enabled        bool             `json:"enabled"`
	BalancePolicy  string           `json:"balance_policy"`
	Session        SessionPolicy    `json:"session_policy"`
	Timeout        Timeout          `json:"timeout"`
	Endpoints      []legacyEndpoint `json:"endpoints"`
	AllowedMethods []string         `json:"allowed_methods,omitempty"`
	Card           json.RawMessage  `json:"agent_card,omitempty"`
	Version        uint64           `json:"version"`
}

type legacyConfig struct {
	Schema       int                `json:"schema_version"`
	Revision     uint64             `json:"config_revision"`
	Routes       []legacyRoute      `json:"routes"`
	Receipts     map[string]Receipt `json:"idempotency"`
	ReceiptOrder []string           `json:"idempotency_order"`
	Audit        []Audit            `json:"audit"`
}

func (e legacyEndpoint) current(entry string) Endpoint {
	i := EndpointInterface{URL: e.URL, Token: e.Token, HealthURL: e.HealthURL}
	if entry == "a2a" {
		i.ProtocolVersion = "1.0"
	}
	return Endpoint{ID: e.ID, Name: e.Name, Weight: e.Weight, State: e.State, Version: e.Version, Interfaces: map[string]EndpointInterface{entry: i}}
}

func (r legacyRoute) current() (Route, error) {
	if r.Protocol != "proxy" && r.Protocol != "a2a" {
		return Route{}, fmt.Errorf("protocol must be proxy or a2a")
	}
	if r.Protocol == "a2a" && len(r.Card) == 0 {
		return Route{}, fmt.Errorf("legacy a2a route requires agent_card")
	}
	out := Route{ID: r.ID, Key: r.Key, AgentType: "generic", Expose: []string{r.Protocol}, Enabled: r.Enabled, BalancePolicy: r.BalancePolicy, Session: r.Session, Timeout: r.Timeout, Endpoints: []Endpoint{}, AllowedMethods: r.AllowedMethods, Card: r.Card, Version: r.Version}
	for _, e := range r.Endpoints {
		if e.CredentialRef != "" {
			return Route{}, fmt.Errorf("legacy credential_ref requires migration to interface token")
		}
		out.Endpoints = append(out.Endpoints, e.current(r.Protocol))
	}
	return out, nil
}

func Load(raw []byte) (Config, bool, error) {
	var header struct {
		Schema int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return Config{}, false, err
	}
	if header.Schema == 2 {
		var c Config
		err := decode(raw, &c)
		for i := range c.Routes {
			if c.Routes[i].AgentType == "" {
				c.Routes[i].AgentType = "generic"
			}
		}
		return c, false, err
	}
	if header.Schema != 1 {
		return Config{}, false, fmt.Errorf("unsupported schema_version")
	}
	var old legacyConfig
	if err := decode(raw, &old); err != nil {
		return Config{}, false, err
	}
	c := Config{Schema: 2, Revision: old.Revision, Routes: []Route{}, Receipts: old.Receipts, ReceiptOrder: old.ReceiptOrder, Audit: old.Audit}
	for _, r := range old.Routes {
		rt, err := r.current()
		if err != nil {
			return Config{}, false, err
		}
		c.Routes = append(c.Routes, rt)
	}
	return c, true, nil
}

func object(raw []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("INVALID_JSON_OBJECT")
	}
	return fields, nil
}

func has(fields map[string]json.RawMessage, keys ...string) bool {
	for _, k := range keys {
		if _, ok := fields[k]; ok {
			return true
		}
	}
	return false
}

func decodeRouteInput(raw []byte, rt *Route) error {
	fields, err := object(raw)
	if err != nil {
		return err
	}
	legacy := has(fields, "protocol")
	if legacy && has(fields, "agent_type", "expose", "interfaces") {
		return errConfigConflict
	}
	var eps []map[string]json.RawMessage
	if json.Unmarshal(fields["endpoints"], &eps) == nil {
		for _, ep := range eps {
			if (legacy && has(ep, "interfaces")) || (!legacy && has(ep, "url", "token", "credential_ref", "health_url")) {
				return errConfigConflict
			}
		}
	}
	if !legacy {
		return decode(raw, rt)
	}
	old := legacyRoute{Enabled: rt.Enabled, Session: rt.Session}
	if err := decode(raw, &old); err != nil {
		return err
	}
	*rt, err = old.current()
	return err
}

// Legacy endpoint input is unambiguous only for a generic single-entry route.
func legacyEntry(rt *Route) (string, error) {
	if rt.AgentType != "generic" || len(rt.Expose) != 1 {
		return "", errConfigConflict
	}
	return rt.Expose[0], nil
}

func endpointInput(raw []byte, rt *Route, patch bool) ([]byte, error) {
	fields, err := object(raw)
	if err != nil {
		return nil, err
	}
	if has(fields, "credential_ref") {
		return nil, fmt.Errorf("credential_ref is unsupported; use token")
	}
	if !has(fields, "url", "health_url", "token") {
		return raw, nil
	}
	if has(fields, "interfaces") {
		return nil, errConfigConflict
	}
	entry, err := legacyEntry(rt)
	if err != nil {
		return nil, err
	}
	i := map[string]json.RawMessage{}
	for _, k := range []string{"url", "health_url", "token"} {
		if v, ok := fields[k]; ok {
			i[k] = v
			delete(fields, k)
		}
	}
	if entry == "a2a" && !patch {
		i["protocol_version"] = json.RawMessage(`"1.0"`)
	}
	fields["interfaces"], _ = json.Marshal(map[string]any{entry: i})
	return json.Marshal(fields)
}

func routePatchInput(raw []byte, rt *Route) ([]byte, error) {
	fields, err := object(raw)
	if err != nil {
		return nil, err
	}
	if !has(fields, "protocol") {
		return raw, nil
	}
	if has(fields, "agent_type", "expose", "interfaces") {
		return nil, errConfigConflict
	}
	entry, err := legacyEntry(rt)
	if err != nil {
		return nil, err
	}
	var protocol string
	if json.Unmarshal(fields["protocol"], &protocol) != nil || (protocol != "proxy" && protocol != "a2a") {
		return nil, fmt.Errorf("protocol must be proxy or a2a")
	}
	if protocol != entry {
		for n := range rt.Endpoints {
			e := &rt.Endpoints[n]
			if len(e.Interfaces) != 1 {
				return nil, errConfigConflict
			}
			i := e.Interfaces[entry]
			i.ProtocolVersion = ""
			if protocol == "a2a" {
				i.ProtocolVersion = "1.0"
			}
			e.Interfaces = map[string]EndpointInterface{protocol: i}
			e.Version++
		}
	}
	delete(fields, "protocol")
	fields["expose"], _ = json.Marshal([]string{protocol})
	return json.Marshal(fields)
}
