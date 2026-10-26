package resource

import (
	"aiagent-gateway/internal/gateway/protocol"
)

// ResolveOwner follows parent links without touching activity or expiry. Unknown
// IDs retain their deterministic affinity key but have no owning worker.
func (d *Directory) ResolveOwner(key protocol.ResourceKey) (string, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.lookupLocked(key)
	if r == nil {
		return string(key.Kind) + "/" + key.ID, ""
	}
	for r.ParentID != "" {
		parent := d.items[r.Parent()]
		if parent == nil {
			break
		}
		r = parent
	}
	return string(r.Kind) + "/" + r.ID, r.WorkerOwner
}

func (d *Directory) AssignWorker(resources []protocol.Resource, worker string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	changed := false
	for _, res := range resources {
		r := d.items[res.Key()]
		if r == nil {
			continue
		}
		for r.ParentID != "" {
			p := d.items[r.Parent()]
			if p == nil {
				break
			}
			r = p
		}
		if r.WorkerOwner == "" {
			r.WorkerOwner = worker
			d.recordResource(r)
			changed = true
		}
	}
	if changed && d.persist != nil {
		return d.persist()
	}
	return nil
}
