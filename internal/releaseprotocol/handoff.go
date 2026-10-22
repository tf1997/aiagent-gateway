package releaseprotocol

import "fmt"

// HandoffPolicy is persisted with release policy and inherited by candidates.
// Zero values select defaults; negative or excessive budgets are rejected.
type HandoffPolicy struct {
	PrepareSeconds    int `json:"prepare_timeout_seconds"`
	BarrierMillis     int `json:"barrier_timeout_milliseconds"`
	StateWaitMillis   int `json:"state_wait_milliseconds"`
	QueueRequests     int `json:"state_queue_requests"`
	QueueBytes        int `json:"state_queue_bytes"`
	SnapshotBytes     int `json:"snapshot_bytes"`
	DeltaBytes        int `json:"delta_bytes"`
	ActivationSeconds int `json:"activation_timeout_seconds"`
}

func DefaultHandoffPolicy() HandoffPolicy {
	return HandoffPolicy{PrepareSeconds: 120, BarrierMillis: 5000, StateWaitMillis: 2000,
		QueueRequests: 1024, QueueBytes: 16 << 20, SnapshotBytes: 256 << 20,
		DeltaBytes: 32 << 20, ActivationSeconds: 30}
}

func (p HandoffPolicy) Effective() HandoffPolicy {
	d := DefaultHandoffPolicy()
	for _, field := range []struct{ value, fallback *int }{
		{&p.PrepareSeconds, &d.PrepareSeconds}, {&p.BarrierMillis, &d.BarrierMillis},
		{&p.StateWaitMillis, &d.StateWaitMillis}, {&p.QueueRequests, &d.QueueRequests},
		{&p.QueueBytes, &d.QueueBytes}, {&p.SnapshotBytes, &d.SnapshotBytes},
		{&p.DeltaBytes, &d.DeltaBytes}, {&p.ActivationSeconds, &d.ActivationSeconds},
	} {
		if *field.value == 0 {
			*field.value = *field.fallback
		}
	}
	return p
}

func (p HandoffPolicy) Validate() error {
	p = p.Effective()
	if p.PrepareSeconds < 1 || p.PrepareSeconds > 3600 || p.BarrierMillis < 1 || p.BarrierMillis > 60000 ||
		p.StateWaitMillis < 1 || p.StateWaitMillis > 60000 || p.QueueRequests < 1 || p.QueueRequests > 65536 ||
		p.QueueBytes < 1 || p.QueueBytes > 1<<30 || p.SnapshotBytes < 1 || p.SnapshotBytes > 256<<20 ||
		p.DeltaBytes < 1 || p.DeltaBytes > 256<<20 || p.ActivationSeconds < 1 || p.ActivationSeconds > 3600 {
		return fmt.Errorf("invalid handoff policy")
	}
	return nil
}
