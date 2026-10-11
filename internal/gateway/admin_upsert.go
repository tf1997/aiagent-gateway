package gateway

import (
	"encoding/json"
	"fmt"
	"reflect"
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
func (g *Gateway) upsertRoute(old Route, fields map[string]json.RawMessage) (Route, error) {
	next := old
	next.Endpoints = append([]Endpoint(nil), old.Endpoints...)
	consumers, supplied, err := g.consumerInput(fields, old.Consumers)
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
				if previous.Name != ep.Name || (ep.Name == "" && !reflect.DeepEqual(previous.Interfaces, ep.Interfaces)) {
					continue
				}
				if match >= 0 {
					return Route{}, fmt.Errorf("%w: ambiguous endpoint identity", errConfigConflict)
				}
				match = i
			}
			if match >= 0 {
				previous := old.Endpoints[match]
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
		changed = changed || !exposes(&next, entry)
	}
	if changed && g.references(old.ID, "") > 0 {
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
		if changed && g.references(old.ID, previous.ID) > 0 {
			return Route{}, errEndpointInUse
		}
	}
	next.Version = old.Version + 1
	return next, nil
}

// These errors retain the existing API error bodies while classifying as 409.
type adminConflict string

func (e adminConflict) Error() string { return string(e) }
func (e adminConflict) Unwrap() error { return errConfigConflict }

const (
	errRouteInUse    adminConflict = "ROUTE_IN_USE"
	errEndpointInUse adminConflict = "ENDPOINT_IN_USE"
)
