package entry

import (
	"context"
	"fmt"
	"net/http"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

type Allocation struct {
	Resolution p.Resolution `json:"resolution"`
	Target     p.Target     `json:"target"`
	Result     p.Result     `json:"result"`
}

// Allocator is owned by the single state writer. A frontend can move to the
// candidate while allocations and result receipts remain with that writer.
type Allocator interface {
	Allocate(context.Context, p.Request, string, string) (Allocation, error)
	Complete(context.Context, p.Result) error
	Ready(context.Context) bool
}

func (e *Entry) Ready(ctx context.Context) bool {
	return ctx.Err() == nil && e.j.Healthy() == nil && validTarget(e.Traffic().Stable)
}

func (e *Entry) Allocate(ctx context.Context, q p.Request, ingress, id string) (Allocation, error) {
	resolved, err := e.resolve(ctx, q)
	out := Allocation{Resolution: resolved}
	if err != nil || resolved.NeedBody || resolved.Status != 0 {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	out.Target, out.Result, err = e.startIngress(resolved.Key, resolved.OwnerWorker, ingress, id)
	return out, err
}

func NewFrontend(allocator Allocator, target p.Target, execute func(http.ResponseWriter, *http.Request, func(p.Result))) (*Entry, error) {
	if allocator == nil {
		return nil, fmt.Errorf("MISSING_ALLOCATOR")
	}
	e := &Entry{allocator: allocator, transport: &http.Transport{Proxy: nil, MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, ResponseHeaderTimeout: 24 * time.Hour}}
	if err := e.SetLocal(target, execute); err != nil {
		return nil, err
	}
	return e, nil
}

// LocalActive counts only requests whose connections belong to this entry.
// Candidate ingress requests are transferred with their original receipts.
func (e *Entry) localActiveLocked() int {
	n := 0
	for _, r := range e.active {
		if r.Ingress == "" || e.localTarget.Incarnation == "" || r.Ingress == e.localTarget.Incarnation {
			n++
		}
	}
	return n
}

func (e *Entry) LocalActive() int { e.mu.Lock(); defer e.mu.Unlock(); return e.localActiveLocked() }

func (e *Entry) IngressActive(ingress string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, r := range e.active {
		if r.Ingress == ingress {
			n++
		}
	}
	return n
}

func (e *Entry) AuthorizeExecution(ingress, id, executor string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.active[id]
	return ok && r.Ingress == ingress && r.Incarnation == executor
}

// AbandonAllocation records an uncertain allocation without executing it again.
func (e *Entry) AbandonAllocation(ctx context.Context, ingress, id string) error {
	e.mu.Lock()
	r, ok := e.active[id]
	e.mu.Unlock()
	if !ok {
		return nil
	}
	if r.Ingress != ingress {
		return fmt.Errorf("INVALID_INGRESS_IDENTITY")
	}
	r.Completed, r.Profile, r.Verdict, r.Reason = time.Now(), "http", "failure", "allocation_reply_lost"
	return e.Complete(ctx, r)
}

func (e *Entry) RetireIngress(ingress string) error {
	e.mu.Lock()
	var lost []p.Result
	for _, r := range e.active {
		if r.Ingress == ingress {
			lost = append(lost, r)
		}
	}
	e.mu.Unlock()
	for _, r := range lost {
		r.Completed, r.Profile, r.Verdict, r.Reason = time.Now(), "http", "failure", "ingress_exited"
		if err := e.Complete(context.Background(), r); err != nil {
			return err
		}
	}
	return nil
}
