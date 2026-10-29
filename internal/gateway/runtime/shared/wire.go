// Package shared implements the worker/state protocol over a Unix socket.
package shared

import (
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/releaseprotocol"
)

type Request struct {
	Epoch                                                             string
	Request                                                           releaseprotocol.Request
	Action, Worker, ID, Owner, Route, Endpoint, Entry, Session, Lease string
	Revision                                                          uint64
	Key                                                               protocol.ResourceKey
	Kind                                                              protocol.ResourceKind
	Resource, Execution                                               protocol.Resource
	Resources                                                         []protocol.Resource
	Creation                                                          protocol.ResourceCreation
	Observations                                                      []protocol.ResourceObservation
	IDs                                                               []string
	State                                                             protocol.ResourceState
	Flag                                                              bool
	Cache                                                             protocol.ListSnapshot
}

// These operations have no mutation receipt whose uncertain outcome would
// require fencing the client. Epoch validation still applies to every call.
func readOnlyAction(action string) bool {
	switch action {
	case "config", "status", "resolve", "list", "lookup", "cache_get":
		return true
	default:
		return false
	}
}

type ResourcePair struct {
	Key   protocol.ResourceReverseKey
	Value protocol.Resource
}

type Response struct {
	Epoch       string
	Resolution  releaseprotocol.Resolution
	Error       string
	Config      *protocol.Config
	Resource    protocol.Resource
	Resources   []protocol.Resource
	Reservation protocol.CreationReservation
	Mapped      []ResourcePair
	Lease, ID   string
	Flag        bool
	Endpoint    *protocol.Endpoint
	Cache       protocol.ListSnapshot
	Count       int64
}
