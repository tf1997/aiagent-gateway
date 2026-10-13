// Package config owns configuration validation, migration, storage and the
// management HTTP API. Runtime state is accessed only through Backend.
package config

import (
	"io"

	"aiagent-gateway/internal/gateway/protocol"
)

// Backend keeps configuration transactions under the runtime's lifecycle lock.
// BeginUpdate returns a release function only after checking freeze and health;
// Commit and References run while that transaction remains locked.
type Backend struct {
	Current        func() *Snapshot
	Healthy        func() bool
	Frozen         func() bool
	BeginUpdate    func() (release func(), failure *Error)
	Commit         func(Config) *Error
	References     func(route, endpoint string) int
	Sessions       func(route, endpoint string) []map[string]any
	Resources      func(owner, route string, kind protocol.ResourceKind, endpoint string) []protocol.Resource
	EndpointDetail func(*Route, *Endpoint) any
	Metrics        func(io.Writer)
}

type Error struct {
	Status int
	Code   string
}

func (e *Error) Error() string { return e.Code }

type handler struct {
	token   string
	backend Backend
}
