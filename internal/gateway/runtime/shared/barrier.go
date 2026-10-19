package shared

import (
	"context"
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
	active, waiting, bytes int
	changed                chan struct{}
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
		if b.waiting >= 1024 || size > (16<<20)-b.bytes {
			b.mu.Unlock()
			return nil, fmt.Errorf("STATE_HANDOFF_QUEUE_FULL")
		}
		b.waiting++
		b.bytes += size
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
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
	return func() {
		b.mu.Lock()
		b.active--
		b.signal()
		b.mu.Unlock()
	}, nil
}

func (b *Barrier) seal(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return fmt.Errorf("STATE_HANDOFF_ALREADY_SEALED")
	}
	b.sealed = true
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

func (b *Barrier) resume() {
	b.mu.Lock()
	b.sealed = false
	b.signal()
	b.mu.Unlock()
}

func (b *Barrier) quiescent() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sealed && b.active == 0
}
