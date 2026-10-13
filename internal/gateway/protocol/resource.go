package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode"
)

type ResourceKind string

const (
	HermesSession  ResourceKind = "hermes_session"
	HermesRun      ResourceKind = "hermes_run"
	HermesResponse ResourceKind = "hermes_response"
	A2AContext     ResourceKind = "a2a_context"
	A2ATask        ResourceKind = "a2a_task"
)

type ResourceState string

const (
	ResourceReserved ResourceState = "reserved"
	ResourceLive     ResourceState = "live"
	ResourceWaiting  ResourceState = "waiting"
	ResourceUnknown  ResourceState = "unknown"
	ResourceTerminal ResourceState = "terminal"
)

var (
	ErrResourceNotFound = errors.New("RESOURCE_NOT_FOUND")
	ErrResourceConflict = errors.New("RESOURCE_CONFLICT")
	ErrResourceCapacity = errors.New("RESOURCE_CAPACITY")
	ErrResourceInUse    = errors.New("RESOURCE_IN_USE")
	ErrResourceInvalid  = errors.New("INVALID_RESOURCE")
)

type ResourceKey struct {
	Owner, Route string
	Kind         ResourceKind
	ID           string
}

type ResourceReverseKey struct {
	Owner, Route string
	Kind         ResourceKind
	Endpoint, ID string
}

// Values returned to adapters are copies. Generation guards late responses and
// releases; only the directory may mutate live records.
type Resource struct {
	WorkerOwner string        `json:"worker_owner,omitempty"`
	ID          string        `json:"public_id"`
	Owner       string        `json:"owner_id"`
	RouteID     string        `json:"route_id"`
	Kind        ResourceKind  `json:"resource_kind"`
	EndpointID  string        `json:"endpoint_id"`
	Interface   string        `json:"interface"`
	Upstream    string        `json:"upstream_id"`
	ParentID    string        `json:"parent_id,omitempty"`
	ParentKind  ResourceKind  `json:"parent_kind,omitempty"`
	Generation  string        `json:"binding_generation"`
	State       ResourceState `json:"lifecycle_state"`
	Seen        time.Time     `json:"last_seen_at"`
	ActiveRefs  int           `json:"active_refs"`
	ChildRefs   int           `json:"child_refs"`
	SessionID   string        `json:"session_id,omitempty"` // Legacy A2A routing hint.
}

// ResourceInterface identifies the upstream interface that owns a resource.
func ResourceInterface(kind ResourceKind) string {
	switch kind {
	case HermesSession, HermesRun, HermesResponse:
		return "proxy"
	case A2AContext, A2ATask:
		return "a2a"
	}
	return ""
}

func (r Resource) Key() ResourceKey {
	return ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.Kind, ID: r.ID}
}

func (r Resource) Parent() ResourceKey {
	return ResourceKey{Owner: r.Owner, Route: r.RouteID, Kind: r.ParentKind, ID: r.ParentID}
}

func ValidResourceID(s string) bool {
	return s != "" && len(s) <= 256 && s != "." && s != ".." && !strings.ContainsAny(s, "/\\") && strings.IndexFunc(s, unicode.IsControl) < 0
}

func ValidUpstreamID(s string) bool {
	return s != "" && len(s) <= 4096 && strings.IndexFunc(s, unicode.IsControl) < 0
}

type ResourceCreation struct {
	Kind           ResourceKind
	Alias          string
	ParentKind     ResourceKind
	ParentID       string
	Operation      string
	IdempotencyKey string
	Fingerprint    string
}

type CreationReservation struct {
	Resource               Resource
	First                  bool
	UpstreamIdempotencyKey string
}

func NamespacedIdempotency(owner, route, operation, key string) string {
	raw, _ := json.Marshal([]string{owner, route, operation, key})
	sum := sha256.Sum256(raw)
	return "gw_" + hex.EncodeToString(sum[:])
}

// Adapters explicitly name the headers affecting a request's semantics.
func DataFingerprint(method, path string, body []byte, h http.Header, headerNames ...string) string {
	headers := map[string][]string{}
	for _, name := range headerNames {
		headers[http.CanonicalHeaderKey(name)] = h.Values(name)
	}
	raw, _ := json.Marshal(struct {
		Method, Path string
		Body         []byte
		Headers      map[string][]string
	}{method, path, body, headers})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type ResourceObservation struct {
	Kind                                ResourceKind
	Upstream, ParentUpstream, SessionID string
	State                               ResourceState
	ReferenceOnly                       bool
}

func NewID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}
