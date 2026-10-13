// Package gateway provides the public entry points for the agent gateway.
// Runtime state and protocol adapters live in their respective subpackages.
package gateway

import (
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime"
	"aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

type (
	Gateway           = runtime.Gateway
	Options           = runtime.Options
	SharedServer      = runtime.SharedServer
	Timeout           = protocol.Timeout
	SessionPolicy     = protocol.SessionPolicy
	Endpoint          = protocol.Endpoint
	EndpointInterface = protocol.EndpointInterface
	Route             = protocol.Route
	Audit             = protocol.Audit
	Receipt           = protocol.Receipt
	Config            = protocol.Config
	Caller            = protocol.Caller
	Consumer          = protocol.Consumer
)

func Open(options Options) (*Gateway, error) { return runtime.Open(options) }

func ValidateOptions(options Options) error { return runtime.ValidateOptions(options) }

func NewSharedServer(g *Gateway, journal *state.Journal) (*SharedServer, error) {
	return runtime.NewSharedServer(g, journal)
}

func ResolveShared(socket, token string, request releaseprotocol.Request) (releaseprotocol.Resolution, error) {
	return runtime.ResolveShared(socket, token, request)
}

func SharedControl(socket, token, worker, action string, flag bool) (int64, error) {
	return runtime.SharedControl(socket, token, worker, action, flag)
}
