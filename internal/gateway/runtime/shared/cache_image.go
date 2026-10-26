package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

type ListImage struct {
	Active    int64       `json:"active"`
	Snapshots []ListEntry `json:"snapshots"`
}

// Tasks use byte strings so JSON encoding cannot compact cached responses and
// change their byte accounting. Their original expiry is never recalculated.
type ListEntry struct {
	Key, Owner, Route, Filter string
	Revision                  uint64
	Expires                   time.Time
	Tasks                     [][]byte
	Size, Bytes               int
}

func (c *ListCache) ExportImage() ListImage {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := ListImage{Active: c.active.Load(), Snapshots: []ListEntry{}}
	for key, snapshot := range c.snapshots {
		x := listEntry(key, snapshot)
		v.Snapshots = append(v.Snapshots, x)
	}
	sort.Slice(v.Snapshots, func(i, j int) bool { return v.Snapshots[i].Key < v.Snapshots[j].Key })
	return v
}

// RestoreListImage has no clock or storage side effects. Expired entries remain
// in the imported view until a future owner explicitly resumes normal cleanup.
func RestoreListImage(v ListImage) (*ListCache, error) {
	if v.Active < 0 || v.Active > 4 || len(v.Snapshots) > 64 {
		return nil, fmt.Errorf("LIST_IMAGE_CAPACITY")
	}
	c := NewListCache(nil)
	c.snapshots = map[string]protocol.ListSnapshot{}
	used := 0
	for _, x := range v.Snapshots {
		if _, ok := c.snapshots[x.Key]; ok || x.Key == "" || x.Owner == "" || x.Route == "" || x.Revision == 0 || x.Expires.IsZero() || x.Size <= 0 || x.Bytes < 0 || x.Bytes > listCacheBytes-used {
			return nil, fmt.Errorf("INVALID_LIST_IMAGE")
		}
		s := protocol.ListSnapshot{Owner: x.Owner, Route: x.Route, Filter: x.Filter, Revision: x.Revision, Expires: x.Expires, Size: x.Size, Bytes: x.Bytes, Tasks: []json.RawMessage{}}
		actual := 0
		for _, task := range x.Tasks {
			if len(task) > x.Bytes-actual || !json.Valid(task) {
				return nil, fmt.Errorf("INVALID_LIST_IMAGE_BYTES")
			}
			actual += len(task)
			s.Tasks = append(s.Tasks, bytes.Clone(task))
		}
		if actual != x.Bytes {
			return nil, fmt.Errorf("INVALID_LIST_IMAGE_BYTES")
		}
		used += x.Bytes
		c.snapshots[x.Key] = s
	}
	c.active.Store(v.Active)
	return c, nil
}

// Peek returns a detached snapshot without cleanup or an implicit TTL renewal.
// The caller must check Expires before using it to serve a page.
func (c *ListCache) Peek(key string) (protocol.ListSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.snapshots[key]
	if !ok {
		return protocol.ListSnapshot{}, false
	}
	tasks := make([]json.RawMessage, len(s.Tasks))
	for i := range s.Tasks {
		tasks[i] = bytes.Clone(s.Tasks[i])
	}
	s.Tasks = tasks
	return s, true
}

func listEntry(key string, snapshot protocol.ListSnapshot) ListEntry {
	x := ListEntry{Key: key, Owner: snapshot.Owner, Route: snapshot.Route, Filter: snapshot.Filter, Revision: snapshot.Revision, Expires: snapshot.Expires, Size: snapshot.Size, Bytes: snapshot.Bytes, Tasks: [][]byte{}}
	for _, task := range snapshot.Tasks {
		x.Tasks = append(x.Tasks, bytes.Clone(task))
	}
	return x
}
