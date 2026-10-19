package runtime

import (
	"bytes"
	"cmp"
	"fmt"
	"sort"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
	"aiagent-gateway/internal/gateway/runtime/shared"
)

const stateImageFormat = "gateway-shared-state-1"

type stateImage struct {
	Format       string                      `json:"format"`
	Config       protocol.Config             `json:"config"`
	LegacyConfig []byte                      `json:"legacy_config,omitempty"`
	ConfigFrozen bool                        `json:"config_frozen"`
	Sessions     []sessionImage              `json:"sessions"`
	Resources    resourcedir.Image           `json:"resources"`
	Tasks        shared.ListImage            `json:"task_lists"`
	SessionLists shared.ListImage            `json:"session_lists"`
	Endpoints    map[string]endpointImage    `json:"endpoints"`
	Schedules    map[string]map[string]int64 `json:"schedules"`
	Callers      map[string]int64            `json:"callers"`
	Inflight     int64                       `json:"inflight"`
	Bindings     int64                       `json:"bindings"`
	Requests     uint64                      `json:"requests"`
	Failures     uint64                      `json:"failures"`
	Transfer     shared.Transfer             `json:"dispatcher"`
}

type sessionImage struct {
	Key         sessionKey
	Endpoint    string
	WorkerOwner string
	Seen        time.Time
	TTL         time.Duration
	Active      int
}

type endpointImage struct {
	Active     int64
	Proxy, A2A interfaceImage
}

type interfaceImage struct {
	Active    int64
	Healthy   bool
	Discovery *discoveryImage
}

// Capabilities.Interface has json:"-" in business responses. Preserve it here
// so a restored cache still knows which URL and credentials it describes.
type discoveryImage struct {
	Interface protocol.EndpointInterface
	Value     protocol.Capabilities
}

// ExportState captures all shared business state under the dispatcher barrier.
// It excludes entry/release journals and the process ownership decision. The
// caller must Seal first and keep admission closed until its handoff decision.
func (g *Gateway) ExportState(maxBytes int) ([]byte, string, error) {
	if g.dispatcher == nil || g.remote != nil {
		return nil, "", fmt.Errorf("STATE_IMAGE_REQUIRES_SHARED_OWNER")
	}
	if maxBytes <= 0 {
		return nil, "", fmt.Errorf("INVALID_SNAPSHOT_BUDGET")
	}
	var raw []byte
	var digest string
	err := g.dispatcher.Capture(func(transfer shared.Transfer) error {
		if !g.StateHealthy() {
			return fmt.Errorf("STATE_NOT_READY")
		}
		v := stateImage{
			Format: stateImageFormat, Config: g.current.Load().Config,
			LegacyConfig: g.legacyRaw, ConfigFrozen: g.configFrozen.Load(),
			Resources: g.resources.ExportImage(), Tasks: g.taskLists.ExportImage(), SessionLists: g.sessionLists.ExportImage(),
			Sessions: []sessionImage{}, Endpoints: map[string]endpointImage{}, Schedules: map[string]map[string]int64{}, Callers: map[string]int64{},
			Inflight: g.inflight.Load(), Bindings: g.bindings.Load(), Requests: g.requests.Load(), Failures: g.failures.Load(), Transfer: transfer,
		}
		for i := range g.shards {
			sh := &g.shards[i]
			sh.mu.Lock()
			for key, b := range sh.items {
				v.Sessions = append(v.Sessions, sessionImage{key, b.Endpoint, b.WorkerOwner, b.Seen, b.TTL, b.Active})
			}
			sh.mu.Unlock()
		}
		sort.Slice(v.Sessions, func(i, j int) bool {
			a, b := v.Sessions[i].Key, v.Sessions[j].Key
			return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Route, b.Route), cmp.Compare(a.ID, b.ID), cmp.Compare(a.Interface, b.Interface)) < 0
		})
		g.statesMu.Lock()
		for key, st := range g.states {
			v.Endpoints[key] = endpointImage{st.active.Load(), captureInterface(&st.proxy), captureInterface(&st.a2a)}
		}
		g.statesMu.Unlock()
		g.schedMu.Lock()
		for key, schedule := range g.sched {
			schedule.mu.Lock()
			weights := map[string]int64{}
			for ep, weight := range schedule.weights {
				weights[ep] = weight
			}
			schedule.mu.Unlock()
			v.Schedules[key] = weights
		}
		g.schedMu.Unlock()
		g.callerMu.Lock()
		for caller, count := range g.callerCounts {
			v.Callers[caller] = count.Load()
		}
		g.callerMu.Unlock()
		var err error
		raw, digest, err = shared.EncodeTransfer(v, maxBytes)
		return err
	})
	return raw, digest, err
}

func captureInterface(st *interfaceState) interfaceImage {
	v := interfaceImage{Active: st.active.Load(), Healthy: st.healthy.Load()}
	if discovery := st.discovery.Load(); discovery != nil {
		v.Discovery = &discoveryImage{discovery.Interface, *discovery}
	}
	return v
}

// StateView is a validated, isolated candidate image. It has no writer handles,
// timers, goroutines, dispatcher, or business handler and cannot become owner.
// Activation requires the separate durable handoff/owner-lock protocol.
type StateView struct {
	raw                 []byte
	digest              string
	image               stateImage
	config              *configuration.Snapshot
	resources           *resourcedir.Directory
	tasks, sessionLists *shared.ListCache
	sessions            map[sessionKey]sessionImage
}

type StateImageMetadata struct {
	Format, Epoch, Digest                                    string
	LogicalRevision, ConfigRevision                          uint64
	Bytes, Sessions, Resources, Operations, Leases, Receipts int
}

func (v *StateView) Metadata() StateImageMetadata {
	return StateImageMetadata{v.image.Format, v.image.Transfer.Epoch, v.digest,
		v.image.Transfer.Revision, v.image.Config.Revision, len(v.raw), len(v.image.Sessions),
		len(v.image.Resources.Resources), len(v.image.Resources.Operations), len(v.image.Transfer.Leases), len(v.image.Transfer.Receipts)}
}

func (v *StateView) Bytes() []byte           { return bytes.Clone(v.raw) }
func (v *StateView) Config() protocol.Config { return configuration.Clone(v.config.Config) }

func (v *StateView) Resource(key protocol.ResourceKey) (protocol.Resource, bool) {
	return v.resources.Peek(key)
}

func (v *StateView) List(kind, key string) (protocol.ListSnapshot, bool) {
	switch kind {
	case "tasks":
		return v.tasks.Peek(key)
	case "sessions":
		return v.sessionLists.Peek(key)
	default:
		return protocol.ListSnapshot{}, false
	}
}

// OpenStateView checks size/digest before decoding, then reconstructs read-only
// indexes and reconciles every active reference with its transferable lease.
// Only capacity options are used; DataDir, sockets and clients are never opened.
func OpenStateView(raw []byte, digest string, maxBytes int, options Options) (*StateView, error) {
	var v stateImage
	if err := shared.DecodeTransfer(raw, digest, maxBytes, &v); err != nil {
		return nil, err
	}
	if v.Format != stateImageFormat {
		return nil, fmt.Errorf("UNSUPPORTED_STATE_IMAGE")
	}
	view := &StateView{raw: bytes.Clone(raw), digest: digest, image: v, sessions: map[sessionKey]sessionImage{}}
	if err := view.validate(defaultOptions(options)); err != nil {
		return nil, err
	}
	return view, nil
}
