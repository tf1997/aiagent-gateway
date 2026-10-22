package shared

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Barrier counts discovery and dispatch together. Sealing waits for accepted
// operations, including slow discovery, before a snapshot can be authoritative.
// Waiting requests retain their original context and consume a bounded budget.
type Barrier struct {
	mu                     sync.Mutex
	sealed                 bool
	sealing                bool
	captures               int
	revision               uint64
	limits                 QueueLimits
	timeouts, rejected     uint64
	active, waiting, bytes int
	changed                chan struct{}
}

// QueueLimits applies to calls waiting for a state handoff. Zero values select
// the production defaults; configuration is fixed before admission starts.
type QueueLimits struct {
	Requests int
	Bytes    int
	Wait     time.Duration
}

func (limits QueueLimits) effective() QueueLimits {
	if limits.Requests == 0 {
		limits.Requests = 1024
	}
	if limits.Bytes == 0 {
		limits.Bytes = 16 << 20
	}
	if limits.Wait == 0 {
		limits.Wait = 2 * time.Second
	}
	return limits
}

func (b *Barrier) Configure(limits QueueLimits) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != 0 || b.waiting != 0 || b.revision != 0 || b.sealed || limits.Requests < 0 || limits.Bytes < 0 || limits.Wait < 0 {
		return fmt.Errorf("INVALID_STATE_QUEUE_CONFIGURATION")
	}
	b.limits = limits
	return nil
}

func (b *Barrier) Revision() uint64 { b.mu.Lock(); defer b.mu.Unlock(); return b.revision }

var ErrSealed = errors.New("STATE_HANDOFF_SEALED")

// BarrierStatus reports admission and drain progress without taking any
// runtime or dispatcher lock. Sealed is true only after all writers leave.
type BarrierStatus struct {
	Sealing, Sealed        bool
	Active, Waiting, Bytes int
	Captures               int
	Revision               uint64
	Timeouts, Rejected     uint64
	Limits                 QueueLimits
}

func (b *Barrier) Status() BarrierStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BarrierStatus{b.sealing, b.sealed && !b.sealing, b.active, b.waiting, b.bytes, b.captures, b.revision, b.timeouts, b.rejected, b.limits.effective()}
}

// TryEnter admits owner-local work without queueing it for a later owner.
// Maintenance skips a sealed tick; configuration and control callers return
// the error to their caller. The returned function covers the whole operation,
// including network discovery, persistence, and asynchronous cleanup.
func (b *Barrier) TryEnter() (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil, ErrSealed
	}
	b.active++
	return b.leave(), nil
}

func (b *Barrier) leave() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.active--
			// Every admitted operation may change memory, even a cache lookup.
			// Advance only after persistence and cleanup have both completed.
			b.revision++
			b.signal()
			b.mu.Unlock()
		})
	}
}

func (b *Barrier) signal() {
	if b.changed != nil {
		close(b.changed)
	}
	b.changed = make(chan struct{})
}

func (b *Barrier) enter(ctx context.Context, size int) (func(), error) {
	b.mu.Lock()
	if b.changed == nil {
		b.changed = make(chan struct{})
	}
	if b.sealed {
		limits := b.limits.effective()
		if size < 0 || b.waiting >= limits.Requests || size > limits.Bytes-b.bytes {
			b.rejected++
			b.mu.Unlock()
			return nil, fmt.Errorf("STATE_HANDOFF_QUEUE_FULL")
		}
		b.waiting++
		b.bytes += size
		waitCtx, cancel := context.WithTimeout(ctx, limits.Wait)
		defer cancel()
		for b.sealed && waitCtx.Err() == nil {
			changed := b.changed
			b.mu.Unlock()
			select {
			case <-changed:
			case <-waitCtx.Done():
			}
			b.mu.Lock()
		}
		b.waiting--
		b.bytes -= size
		if waitCtx.Err() != nil {
			b.timeouts++
			b.mu.Unlock()
			return nil, fmt.Errorf("STATE_HANDOFF_WAIT_TIMEOUT: %w", waitCtx.Err())
		}
	}
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	b.active++
	b.mu.Unlock()
	return b.leave(), nil
}

func (b *Barrier) seal(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return fmt.Errorf("STATE_HANDOFF_ALREADY_SEALED")
	}
	b.sealed = true
	b.sealing = true
	defer func() { b.sealing = false }()
	b.signal()
	for b.active > 0 {
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		b.mu.Lock()
		if ctx.Err() != nil {
			b.sealed = false
			b.signal()
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		b.sealed = false
		b.signal()
		return err
	}
	return nil
}

func (b *Barrier) resume() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealing {
		return fmt.Errorf("STATE_HANDOFF_SEAL_IN_PROGRESS")
	}
	if b.captures != 0 {
		return fmt.Errorf("STATE_HANDOFF_SNAPSHOT_IN_PROGRESS")
	}
	b.sealed = false
	b.signal()
	return nil
}

// lockSealed pins the frozen state while an import/export accesses the backend.
// Resume must not reopen owner-local admission in the middle of that access.
func (b *Barrier) lockSealed() (func(), bool) {
	b.mu.Lock()
	if !b.sealed || b.sealing || b.active != 0 {
		b.mu.Unlock()
		return nil, false
	}
	b.captures++
	b.mu.Unlock()
	return sync.OnceFunc(func() {
		b.mu.Lock()
		b.captures--
		b.mu.Unlock()
	}), true
}
