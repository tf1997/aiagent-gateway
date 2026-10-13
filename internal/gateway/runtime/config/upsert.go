package config

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	"aiagent-gateway/internal/gateway/protocol"
)

// Keys are the public management identity. Internal IDs remain accepted for
// existing clients, but a key always wins if it resembles another route's ID.
func adminRouteIndex(routes []Route, keyOrID string) int {
	for i := range routes {
		if routes[i].Key == keyOrID {
			return i
		}
	}
	for i := range routes {
		if routes[i].ID == keyOrID {
			return i
		}
	}
	return -1
}

// upsertRoute runs under the configuration and lifecycle locks. It prepares a
// candidate without changing the old route; persistence publishes it atomically.
func (h *handler) upsertRoute(old Route, fields map[string]json.RawMessage) (Route, error) {
	next := old
	next.Endpoints = append([]Endpoint(nil), old.Endpoints...)
	consumers, supplied, err := h.consumerInput(fields, old.Consumers)
	if err != nil {
		return Route{}, err
	}
	endpoints, replaceEndpoints := fields["endpoints"]
	delete(fields, "endpoints")
	patch, _ := json.Marshal(fields)
	patch, err = routePatchInput(patch, &next)
	if err != nil {
		return Route{}, err
	}
	if err = merge(patch, &next); err != nil {
		return Route{}, err
	}
	if supplied {
		next.Consumers = consumers
	}
	if replaceEndpoints {
		var inputs []json.RawMessage
		if json.Unmarshal(endpoints, &inputs) != nil || inputs == nil {
			return Route{}, fmt.Errorf("endpoints must be an array")
		}
		next.Endpoints = make([]Endpoint, 0, len(inputs))
		used := map[string]bool{}
		names := map[string]bool{}
		for _, raw := range inputs {
			input, err := endpointInput(raw, &next, false)
			if err != nil {
				return Route{}, err
			}
			var ep Endpoint
			if err = decode(input, &ep); err != nil {
				return Route{}, err
			}
			if err = initEndpoint(&ep); err != nil {
				return Route{}, err
			}
			if ep.Name != "" && names[ep.Name] {
				return Route{}, fmt.Errorf("%w: duplicate endpoint name", errConfigConflict)
			}
			names[ep.Name] = true
			match := -1
			for i, previous := range old.Endpoints {
				// Legacy unnamed endpoints can be reconciled only by identical
				// interfaces. Never infer identity from array position.
				if previous.Name != ep.Name {
					continue
				}
				if ep.Name == "" {
					candidate := ep
					candidate.Interfaces = maps.Clone(ep.Interfaces)
					inheritEndpointTokens(&candidate, previous, input)
					if !reflect.DeepEqual(previous.Interfaces, candidate.Interfaces) {
						continue
					}
				}
				if match >= 0 {
					return Route{}, fmt.Errorf("%w: ambiguous endpoint identity", errConfigConflict)
				}
				match = i
			}
			if match >= 0 {
				previous := old.Endpoints[match]
				inheritEndpointTokens(&ep, previous, input)
				if used[previous.ID] {
					return Route{}, fmt.Errorf("%w: duplicate endpoint identity", errConfigConflict)
				}
				used[previous.ID] = true
				ep.ID, ep.Version = previous.ID, previous.Version
				if !reflect.DeepEqual(ep, previous) {
					ep.Version++
				}
			}
			next.Endpoints = append(next.Endpoints, ep)
		}
	}
	changed := old.AgentType != next.AgentType
	for _, entry := range old.Expose {
		changed = changed || !protocol.Exposes(&next, entry)
	}
	if changed && h.backend.References(old.ID, "") > 0 {
		return Route{}, errRouteInUse
	}
	for _, previous := range old.Endpoints {
		var replacement *Endpoint
		for i := range next.Endpoints {
			if next.Endpoints[i].ID == previous.ID {
				replacement = &next.Endpoints[i]
				break
			}
		}
		changed := replacement == nil
		if replacement != nil {
			for entry, iface := range previous.Interfaces {
				v, ok := replacement.Interfaces[entry]
				changed = changed || !ok || iface.URL != v.URL || iface.ProtocolVersion != v.ProtocolVersion || iface.Tenant != v.Tenant
			}
		}
		if changed && h.backend.References(old.ID, previous.ID) > 0 {
			return Route{}, errEndpointInUse
		}
	}
	next.Version = old.Version + 1
	return next, nil
}

// An upsert may reuse an endpoint returned by GET, where tokens are redacted.
// Omitted tokens retain the prior value; explicit null removes authentication.
func inheritEndpointTokens(ep *Endpoint, previous Endpoint, input []byte) {
	var provided struct {
		Interfaces map[string]map[string]json.RawMessage `json:"interfaces"`
	}
	if json.Unmarshal(input, &provided) != nil {
		return
	}
	for entry, iface := range ep.Interfaces {
		if _, ok := provided.Interfaces[entry]["token"]; ok {
			continue
		}
		if prior, ok := previous.Interfaces[entry]; ok {
			iface.Token = prior.Token
			ep.Interfaces[entry] = iface
		}
	}
}

// These errors retain the existing API error bodies while classifying as 409.
type adminConflict string

func (e adminConflict) Error() string { return string(e) }

func (e adminConflict) Unwrap() error { return errConfigConflict }

const (
	errRouteInUse    adminConflict = "ROUTE_IN_USE"
	errEndpointInUse adminConflict = "ENDPOINT_IN_USE"
)
