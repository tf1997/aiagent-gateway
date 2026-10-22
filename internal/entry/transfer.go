package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

// Transfer preserves the allocator sequence, affinity, and evidence. A source
// entry must drain its public and forwarded requests before relinquishing it.
type Transfer struct {
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
	for len(e.active) != 0 && ctx.Err() == nil {
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
	if !e.sealed || len(e.active) != 0 {
		return Transfer{}, fmt.Errorf("ENTRY_NOT_DRAINED_AND_SEALED")
	}
	if err := e.j.Healthy(); err != nil {
		return Transfer{}, err
	}
	v := Transfer{e.traffic, e.bindings, e.results, e.counter, e.lost}
	raw, err := json.Marshal(v)
	if err != nil {
		return Transfer{}, err
	}
	var detached Transfer
	err = json.Unmarshal(raw, &detached)
	return detached, err
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
	raw, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	var v Transfer
	if err = json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	if v.Bindings == nil {
		v.Bindings = map[string]string{}
	}
	if v.Results == nil {
		v.Results = map[string]p.Result{}
	}
	return &Entry{j: j, token: token, resolve: resolve, traffic: v.Traffic, bindings: v.Bindings, results: v.Results, counter: v.Counter, lost: v.Lost, active: map[string]p.Result{}, sealed: true, transport: &http.Transport{Proxy: nil, MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, ResponseHeaderTimeout: 24 * time.Hour}}, nil
}

func (e *Entry) Active() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.active) }
