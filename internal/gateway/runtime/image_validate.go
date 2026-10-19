package runtime

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

func (v *StateView) validate(o Options) error {
	image := &v.image
	if err := validateImageConfig(image.Config); err != nil {
		return err
	}
	if len(image.LegacyConfig) != 0 {
		legacy, _, err := configuration.Load(image.LegacyConfig)
		if err != nil || configuration.Validate(&legacy) != nil || legacy.Revision != image.Config.Revision {
			return fmt.Errorf("INVALID_IMAGE_LEGACY_CONFIG")
		}
	}
	if err := shared.ValidateTransfer(image.Transfer); err != nil {
		return err
	}
	v.config = configuration.Build(image.Config)
	routes := map[string]*protocol.Route{}
	for _, rt := range v.config.Routes {
		routes[rt.ID] = rt
	}
	validEndpoint := func(route, endpoint string) bool {
		rt := routes[route]
		return rt != nil && rt.Endpoint(endpoint) != nil
	}
	var err error
	v.resources, err = resourcedir.RestoreImage(image.Resources, resourcedir.Limits{
		MaxResources: o.MaxResources, MaxResourceAliases: o.MaxResourceAliases, MaxDataIdempotency: o.MaxDataIdempotency,
		MaxSessions: o.MaxSessions, MaxTasks: o.MaxTasks,
	}, validEndpoint)
	if err != nil {
		return err
	}
	if image.Bindings != int64(len(image.Sessions)) || len(image.Sessions) > o.MaxSessions {
		return fmt.Errorf("STATE_IMAGE_SESSION_CAPACITY")
	}
	for _, s := range image.Sessions {
		if _, exists := v.sessions[s.Key]; exists || s.Key.Owner == "" || s.Key.ID == "" || (s.Key.Interface != "proxy" && s.Key.Interface != "a2a") || !validEndpoint(s.Key.Route, s.Endpoint) || !protocol.Exposes(routes[s.Key.Route], s.Key.Interface) || s.Seen.IsZero() || s.TTL <= 0 || s.Active < 0 {
			return fmt.Errorf("INVALID_STATE_IMAGE_SESSION")
		}
		v.sessions[s.Key] = s
	}
	v.tasks, err = shared.RestoreListImage(image.Tasks)
	if err != nil {
		return err
	}
	v.sessionLists, err = shared.RestoreListImage(image.SessionLists)
	if err != nil {
		return err
	}
	for key, ep := range image.Endpoints {
		if !strings.Contains(key, "/") || ep.Active < 0 || ep.Proxy.Active < 0 || ep.A2A.Active < 0 {
			return fmt.Errorf("INVALID_STATE_IMAGE_ENDPOINT")
		}
		for _, iface := range []interfaceImage{ep.Proxy, ep.A2A} {
			if d := iface.Discovery; d != nil && (d.Value.Checked.IsZero() || !configuration.ValidURL(d.Interface.URL)) {
				return fmt.Errorf("INVALID_STATE_IMAGE_DISCOVERY")
			}
		}
	}
	for key, weights := range image.Schedules {
		if !(strings.HasSuffix(key, "/proxy") || strings.HasSuffix(key, "/a2a")) || weights == nil {
			return fmt.Errorf("INVALID_STATE_IMAGE_SCHEDULE")
		}
		for ep := range weights {
			if ep == "" {
				return fmt.Errorf("INVALID_STATE_IMAGE_SCHEDULE")
			}
		}
	}
	return v.validateLeaseAccounting(o)
}

func validateImageConfig(c protocol.Config) error {
	if err := configuration.Validate(&c); err != nil {
		return fmt.Errorf("INVALID_IMAGE_CONFIG: %w", err)
	}
	if c.Revision == 0 || len(c.Receipts) > 1000 || len(c.Audit) > 1000 || len(c.Receipts) != len(c.ReceiptOrder) {
		return fmt.Errorf("INVALID_IMAGE_CONFIG_METADATA")
	}
	for _, rt := range c.Routes {
		if rt.Version == 0 {
			return fmt.Errorf("INVALID_IMAGE_ROUTE_VERSION")
		}
		for _, ep := range rt.Endpoints {
			if ep.Version == 0 {
				return fmt.Errorf("INVALID_IMAGE_ENDPOINT_VERSION")
			}
		}
	}
	seen := map[string]bool{}
	for _, key := range c.ReceiptOrder {
		r, exists := c.Receipts[key]
		hash, err := hex.DecodeString(r.Hash)
		tag, tagErr := strconv.Unquote(r.ETag)
		version, versionErr := strconv.ParseUint(tag, 10, 64)
		if !exists || seen[key] || key == "" || len(key) > 256 || err != nil || len(hash) != 32 || !json.Valid(r.Body) || tagErr != nil || versionErr != nil || version == 0 {
			return fmt.Errorf("INVALID_IMAGE_CONFIG_RECEIPT")
		}
		seen[key] = true
	}
	var revision uint64
	for _, audit := range c.Audit {
		if audit.Revision <= revision || audit.Revision > c.Revision || audit.At.IsZero() {
			return fmt.Errorf("INVALID_IMAGE_CONFIG_AUDIT")
		}
		revision = audit.Revision
	}
	return nil
}

func (v *StateView) validateLeaseAccounting(o Options) error {
	image := &v.image
	var inflight int64
	callers := map[string]int64{}
	endpoints := map[string]endpointImage{}
	sessions := map[sessionKey]int{}
	pins := map[protocol.ResourceKey]int{}
	lists := map[string]int64{}
	for _, l := range image.Transfer.Leases {
		switch l.Kind {
		case "acquire":
			rt := v.config.Routes[l.Route]
			if rt == nil || l.Owner == "" || rt.Endpoint(l.Endpoint) == nil || (l.Entry != "proxy" && l.Entry != "a2a") || !protocol.Exposes(rt, l.Entry) {
				return fmt.Errorf("INVALID_IMAGE_EXECUTION_LEASE")
			}
			inflight++
			callers[l.Owner]++
			key := rt.ID + "/" + l.Endpoint
			ep := endpoints[key]
			ep.Active++
			if l.Entry == "proxy" {
				ep.Proxy.Active++
			} else {
				ep.A2A.Active++
			}
			endpoints[key] = ep
			if l.Session != "" {
				key := sessionKey{l.Owner, rt.ID, l.Session, l.Entry}
				s, ok := v.sessions[key]
				if !ok || s.Endpoint != l.Endpoint {
					return fmt.Errorf("IMAGE_LEASE_SESSION_MISMATCH")
				}
				sessions[key]++
			}
		case "pin":
			r, ok := v.resources.Peek(l.Resource)
			if !ok {
				return fmt.Errorf("IMAGE_LEASE_RESOURCE_MISSING")
			}
			pins[r.Key()]++
		case "cache_acquire":
			if l.Entry != "tasks" && l.Entry != "sessions" {
				return fmt.Errorf("INVALID_IMAGE_LIST_LEASE")
			}
			lists[l.Entry]++
		default:
			return fmt.Errorf("INVALID_IMAGE_LEASE_KIND")
		}
	}
	if image.Inflight != inflight || inflight > int64(o.MaxInflight) || len(image.Callers) != len(callers) {
		return fmt.Errorf("IMAGE_GLOBAL_ACCOUNTING_MISMATCH")
	}
	for owner, count := range image.Callers {
		if owner == "" || count <= 0 || count != callers[owner] || count > int64(o.MaxPerCaller) {
			return fmt.Errorf("IMAGE_CALLER_ACCOUNTING_MISMATCH")
		}
	}
	for key, ep := range image.Endpoints {
		expected := endpoints[key]
		if ep.Active != expected.Active || ep.Proxy.Active != expected.Proxy.Active || ep.A2A.Active != expected.A2A.Active || ep.Active > int64(o.MaxPerEndpoint) {
			return fmt.Errorf("IMAGE_ENDPOINT_ACCOUNTING_MISMATCH")
		}
	}
	for key := range endpoints {
		if _, ok := image.Endpoints[key]; !ok {
			return fmt.Errorf("IMAGE_LEASE_ENDPOINT_STATE_MISSING")
		}
	}
	for key, s := range v.sessions {
		if s.Active != sessions[key] {
			return fmt.Errorf("IMAGE_SESSION_ACCOUNTING_MISMATCH")
		}
	}
	for _, r := range image.Resources.Resources {
		if r.Resource.ActiveRefs != pins[r.Resource.Key()] {
			return fmt.Errorf("IMAGE_RESOURCE_ACCOUNTING_MISMATCH")
		}
	}
	if image.Tasks.Active != lists["tasks"] || image.SessionLists.Active != lists["sessions"] {
		return fmt.Errorf("IMAGE_LIST_ACCOUNTING_MISMATCH")
	}
	return nil
}
