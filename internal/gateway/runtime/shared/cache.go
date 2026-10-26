package shared

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/state"
)

const listCacheBytes = 32 << 20

// ListCache owns bounded snapshots for pagination within one list kind. The
// state service shares the same cache across workers during a rollout.
type ListCache struct {
	mu        sync.Mutex
	recorder  *state.Recorder
	prefix    string
	active    atomic.Int64
	snapshots map[string]protocol.ListSnapshot
	now       func() time.Time
}

func NewListCache(now func() time.Time) *ListCache {
	if now == nil {
		now = time.Now
	}
	return &ListCache{now: now}
}

func (c *ListCache) Get(key string) (protocol.ListSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, snapshot := range c.snapshots {
		if !c.now().Before(snapshot.Expires) {
			delete(c.snapshots, k)
			c.recorder.Delete(c.prefix + k)
		}
	}
	snapshot, ok := c.snapshots[key]
	return snapshot, ok
}

func (c *ListCache) Put(snapshot protocol.ListSnapshot) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	used := 0
	for k, existing := range c.snapshots {
		if !c.now().Before(existing.Expires) {
			delete(c.snapshots, k)
			c.recorder.Delete(c.prefix + k)
		} else {
			used += existing.Bytes
		}
	}
	if len(c.snapshots) >= 64 || used+snapshot.Bytes > listCacheBytes {
		return "", fmt.Errorf("LIST_CACHE_CAPACITY")
	}
	if c.snapshots == nil {
		c.snapshots = map[string]protocol.ListSnapshot{}
	}
	key := protocol.NewID("list_")
	c.snapshots[key] = snapshot
	if c.recorder.Active() {
		c.recorder.Put(c.prefix+key, listEntry(key, snapshot))
	}
	return key, nil
}

func (c *ListCache) Acquire() (func(), error) {
	if c.active.Add(1) > 4 {
		c.active.Add(-1)
		return nil, fmt.Errorf("LIST_CAPACITY")
	}
	return func() { c.active.Add(-1) }, nil
}

func (c *ListCache) SetRecorder(r *state.Recorder, prefix string) { c.recorder, c.prefix = r, prefix }
