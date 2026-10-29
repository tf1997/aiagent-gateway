package shared

import (
	"context"
	"sync"
)

// contextMutex keeps the zero-value Lock/Unlock contract while allowing RPCs
// to stop waiting for a slow dispatcher when their deadline expires.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) init()   { m.once.Do(func() { m.gate = make(chan struct{}, 1) }) }
func (m *contextMutex) Lock()   { m.init(); m.gate <- struct{}{} }
func (m *contextMutex) Unlock() { <-m.gate }

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.init()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}
