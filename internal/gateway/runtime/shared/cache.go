package shared

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

const listCacheBytes = 32 << 20

// ListCache owns bounded snapshots for pagination within one list kind. The
// state service shares the same cache across workers during a rollout.
type ListCache struct {
	mu        sync.Mutex
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
	return key, nil
}

func (c *ListCache) Acquire() (func(), error) {
	if c.active.Add(1) > 4 {
		c.active.Add(-1)
		return nil, fmt.Errorf("LIST_CAPACITY")
	}
	return func() { c.active.Add(-1) }, nil
}
