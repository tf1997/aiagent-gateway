package gateway

import (
	"encoding/json"
	"fmt"
	"time"
)

type cacheRecord struct {
	Owner, Route, Filter string
	Revision             uint64
	Expires              time.Time
	Tasks                []json.RawMessage
	Size, Bytes          int
}

func cacheWire(s a2aListSnapshot) cacheRecord {
	return cacheRecord{s.owner, s.route, s.filter, s.revision, s.expires, s.tasks, s.size, s.bytes}
}
func (c cacheRecord) snapshot() a2aListSnapshot {
	return a2aListSnapshot{owner: c.Owner, route: c.Route, filter: c.Filter, revision: c.Revision, expires: c.Expires, tasks: c.Tasks, size: c.Size, bytes: c.Bytes}
}
func (g *Gateway) listCache(kind string) *a2aListCache {
	if kind == "sessions" {
		return &g.sessionLists
	}
	return &g.taskLists
}
func (g *Gateway) cacheGet(kind, key string) (cacheRecord, bool) {
	if g.remote != nil {
		var out sharedResponse
		g.remote.must("cache_get", sharedRequest{Entry: kind, Session: key}, &out)
		return out.Cache, out.Flag
	}
	c := g.listCache(kind)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, s := range c.snapshots {
		if !time.Now().Before(s.expires) {
			delete(c.snapshots, k)
		}
	}
	s, ok := c.snapshots[key]
	return cacheWire(s), ok
}
func (g *Gateway) cachePut(kind string, v cacheRecord) (string, error) {
	if g.remote != nil {
		var out sharedResponse
		err := g.remote.call("cache_put", sharedRequest{Entry: kind, Cache: v}, &out)
		return out.ID, err
	}
	c := g.listCache(kind)
	c.mu.Lock()
	defer c.mu.Unlock()
	used := 0
	for k, s := range c.snapshots {
		if !time.Now().Before(s.expires) {
			delete(c.snapshots, k)
		} else {
			used += s.bytes
		}
	}
	if len(c.snapshots) >= 64 || used+v.Bytes > a2aListCacheBytes {
		return "", fmt.Errorf("LIST_CACHE_CAPACITY")
	}
	if c.snapshots == nil {
		c.snapshots = map[string]a2aListSnapshot{}
	}
	key := newID("list_")
	c.snapshots[key] = v.snapshot()
	return key, nil
}

func (g *Gateway) acquireList(kind string) (func(), error) {
	if g.remote != nil {
		var out sharedResponse
		if err := g.remote.call("cache_acquire", sharedRequest{Entry: kind}, &out); err != nil {
			return nil, err
		}
		return g.remote.release(out.Lease), nil
	}
	cache := g.listCache(kind)
	if cache.active.Add(1) > 4 {
		cache.active.Add(-1)
		return nil, fmt.Errorf("LIST_CAPACITY")
	}
	return func() { cache.active.Add(-1) }, nil
}
