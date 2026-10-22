package process

import (
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway"
	"aiagent-gateway/internal/gateway/runtime/shared"
	p "aiagent-gateway/internal/releaseprotocol"
)

func ownerOptions(cfg appconfig.Config, policy p.HandoffPolicy) gateway.Options {
	opts := cfg.Options()
	opts.StateQueue = shared.QueueLimits{Requests: policy.QueueRequests, Bytes: policy.QueueBytes, Wait: time.Duration(policy.StateWaitMillis) * time.Millisecond}
	return opts
}

func (n *Gateway) HandoffPolicy() p.HandoffPolicy { return n.policy }
