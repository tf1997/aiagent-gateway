package entry

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

// Transfer preserves the allocator sequence, affinity, and evidence. A source
// entry must drain its public and forwarded requests before relinquishing it.
type Transfer struct {
	Active   map[string]p.Result `json:"active,omitempty"`
	Traffic  p.Traffic           `json:"traffic"`
	Bindings map[string]string   `json:"bindings"`
	Results  map[string]p.Result `json:"results"`
	Counter  uint64              `json:"counter"`
	Lost     bool                `json:"lost"`
}

// SetAdmission connects entry mutations to the same owner sequence as runtime
// and release writes. Configure once before the entry starts serving requests.
func (e *Entry) SetAdmission(admit func() (func(), error)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.admission = admit
}

func (e *Entry) admit() (func(), error) {
	e.mu.Lock()
	admit := e.admission
	e.mu.Unlock()
	if admit != nil {
		return admit()
	}
	return func() {}, nil
}

func (e *Entry) Seal(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sealed {
		return fmt.Errorf("ENTRY_ALREADY_SEALED")
	}
	e.sealed = true
	for e.localActiveLocked() != 0 && ctx.Err() == nil {
		if e.changed == nil {
			e.changed = make(chan struct{})
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		e.mu.Lock()
	}
	if err := ctx.Err(); err != nil {
		e.sealed = false
		return err
	}
	return nil
}

func (e *Entry) Resume() { e.mu.Lock(); defer e.mu.Unlock(); e.sealed = false }

func (e *Entry) Export() (Transfer, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.sealed || e.localActiveLocked() != 0 {
		return Transfer{}, fmt.Errorf("ENTRY_NOT_DRAINED_AND_SEALED")
	}
	if err := e.j.Healthy(); err != nil {
		return Transfer{}, err
	}
	v := Transfer{Traffic: e.traffic, Bindings: e.bindings, Results: e.results, Counter: e.counter, Lost: e.lost, Active: e.active}
	return v.clone(), nil
}

// Results and bindings contain only values. Copy their maps and the one target
// pointer directly instead of serializing the entire evidence history twice
// while incoming allocation calls are waiting at the handoff barrier.
func (v Transfer) clone() Transfer {
	v.Bindings = maps.Clone(v.Bindings)
	v.Results = maps.Clone(v.Results)
	v.Active = maps.Clone(v.Active)
	if v.Traffic.Candidate != nil {
		candidate := *v.Traffic.Candidate
		v.Traffic.Candidate = &candidate
	}
	return v
}

func (v Transfer) Validate() error {
	if v.Traffic.Revision == 0 || !validTarget(v.Traffic.Stable) || v.Traffic.Percent < 0 || v.Traffic.Percent > 100 || (v.Traffic.Candidate == nil && v.Traffic.Percent != 0) || len(v.Bindings) > 100000 || len(v.Results) > 8192 {
		return fmt.Errorf("INVALID_ENTRY_IMAGE")
	}
	if c := v.Traffic.Candidate; c != nil && (!validTarget(*c) || c.Incarnation == v.Traffic.Stable.Incarnation) {
		return fmt.Errorf("INVALID_ENTRY_IMAGE_TARGET")
	}
	for key, worker := range v.Bindings {
		if key == "" || worker == "" {
			return fmt.Errorf("INVALID_ENTRY_IMAGE_BINDING")
		}
	}
	for key, r := range v.Results {
		if key == "" || key != r.ID || r.Incarnation == "" || r.Release == "" || r.RoutingRevision == 0 || r.RoutingRevision > v.Traffic.Revision || r.Started.IsZero() || r.Completed.Before(r.Started) {
			return fmt.Errorf("INVALID_ENTRY_IMAGE_RESULT")
		}
	}
	if len(v.Active) > 100000 {
		return fmt.Errorf("INVALID_ENTRY_ACTIVE_CAPACITY")
	}
	for key, r := range v.Active {
		if key == "" || key != r.ID || r.Ingress == "" || r.Incarnation == "" || r.Release == "" || r.RoutingRevision == 0 || r.RoutingRevision > v.Traffic.Revision || r.Started.IsZero() || !r.Completed.IsZero() {
			return fmt.Errorf("INVALID_ENTRY_ACTIVE")
		}
		if r.Ingress != v.Traffic.Stable.Incarnation && (v.Traffic.Candidate == nil || r.Ingress != v.Traffic.Candidate.Incarnation) {
			return fmt.Errorf("INVALID_ENTRY_ACTIVE_INGRESS")
		}
		if _, duplicate := v.Results[key]; duplicate {
			return fmt.Errorf("DUPLICATE_ENTRY_RESULT")
		}
	}
	return nil
}

// Import reconstructs a sealed entry without Open's cold-restart conversion or
// journal writes. Activation and evidence publication follow durable COMMITTED.
func Import(j *state.Journal, token string, resolve Resolver, image Transfer) (*Entry, error) {
	if j == nil || j.Healthy() != nil {
		return nil, fmt.Errorf("ENTRY_STORAGE_UNAVAILABLE")
	}
	if err := image.Validate(); err != nil {
		return nil, err
	}
	v := image.clone()
	if v.Bindings == nil {
		v.Bindings = map[string]string{}
	}
	if v.Results == nil {
		v.Results = map[string]p.Result{}
	}
	if v.Active == nil {
		v.Active = map[string]p.Result{}
	}
	return &Entry{j: j, token: token, resolve: resolve, traffic: v.Traffic, bindings: v.Bindings, results: v.Results, counter: v.Counter, lost: v.Lost, active: v.Active, sealed: true, transport: &http.Transport{Proxy: nil, MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, ResponseHeaderTimeout: 24 * time.Hour}}, nil
}

func (e *Entry) Active() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.active) }
