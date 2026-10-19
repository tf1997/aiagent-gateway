// Package handoff coordinates durable single-writer process replacement.
package handoff

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

type Phase string

const (
	Stable    Phase = "STABLE"
	Prepared  Phase = "PREPARED"
	Sealed    Phase = "SEALED"
	Committed Phase = "COMMITTED"
	Completed Phase = "COMPLETED"
	Aborted   Phase = "ABORTED"
)

var (
	ErrConflict   = errors.New("HANDOFF_CONFLICT")
	ErrCommitted  = errors.New("ROLLBACK_AFTER_COMMIT")
	ErrNoBaseline = errors.New("HANDOFF_BASELINE_MISSING")
)

// Identity uses a content digest, never a mutable path or a persisted PID.
type Identity struct {
	Incarnation string `json:"incarnation"`
	Release     string `json:"release"`
}

type Snapshot struct {
	Digest   string `json:"digest"`
	Revision uint64 `json:"logical_revision"`
	Bytes    int64  `json:"bytes"`
}

// Decision is the single authority for both the state owner and cold start.
// COMMITTED is irreversible for this handoff, including when its reply is lost.
type Decision struct {
	ID        string   `json:"handoff_id,omitempty"`
	Rollout   string   `json:"rollout_id,omitempty"`
	Phase     Phase    `json:"phase"`
	Source    Identity `json:"source"`
	Target    Identity `json:"target"`
	Owner     Identity `json:"owner"`
	Epoch     string   `json:"owner_epoch"`
	NextEpoch string   `json:"next_owner_epoch,omitempty"`
	ColdStart string   `json:"cold_start_release"`
	Snapshot  Snapshot `json:"snapshot"`
}

func validID(s string) bool {
	return s != "" && len(s) <= 128 && !strings.ContainsAny(s, "/\\\x00\r\n")
}

func validDigest(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (i Identity) valid() bool { return validID(i.Incarnation) && validDigest(i.Release) }

func (d Decision) Validate() error {
	if !d.Owner.valid() || !validID(d.Epoch) || !validDigest(d.ColdStart) || d.ColdStart != d.Owner.Release {
		return fmt.Errorf("invalid handoff owner")
	}
	if d.Phase == Stable {
		if d.ID != "" || d.Rollout != "" || d.Source != d.Owner || d.Target != (Identity{}) || d.NextEpoch != "" || d.Snapshot != (Snapshot{}) {
			return fmt.Errorf("invalid handoff baseline")
		}
		return nil
	}
	if !validID(d.ID) || !validID(d.Rollout) || !d.Source.valid() || !d.Target.valid() || d.Source.Incarnation == d.Target.Incarnation || !validID(d.NextEpoch) {
		return fmt.Errorf("invalid handoff identity")
	}
	switch d.Phase {
	case Prepared, Sealed, Aborted:
		if d.Owner != d.Source || d.Epoch == d.NextEpoch {
			return fmt.Errorf("invalid source owner")
		}
	case Committed, Completed:
		if d.Owner != d.Target || d.Epoch != d.NextEpoch {
			return fmt.Errorf("invalid committed owner")
		}
	default:
		return fmt.Errorf("invalid handoff phase")
	}
	if d.Phase == Sealed || d.Phase == Committed || d.Phase == Completed {
		if !validDigest(d.Snapshot.Digest) || d.Snapshot.Bytes < 0 {
			return fmt.Errorf("invalid sealed snapshot")
		}
	}
	return nil
}

// Owner holds owner.lock before the handoff journal and releases it last.
// Acquiring it alone does not authorize business writes: callers must Bootstrap,
// Commit, Abort, or Recover and check the returned decision first.
type Owner struct {
	mu       sync.Mutex
	identity Identity
	lock     *os.File
	journal  *state.Journal
	decision Decision
}

func Acquire(dir string, identity Identity) (*Owner, error) {
	if !identity.valid() {
		return nil, fmt.Errorf("invalid process identity")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(dir, "owner.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "gateway-owner")
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("state owner already active: %w", err)
	}
	o := &Owner{identity: identity, lock: lock}
	fail := func(err error) (*Owner, error) { o.Close(); return nil, err }
	if o.journal, err = state.Open(filepath.Join(dir, "handoff")); err != nil {
		return fail(err)
	}
	d, err := decode(o.journal.Snapshot())
	if err != nil && !errors.Is(err, ErrNoBaseline) {
		return fail(err)
	}
	o.decision = d
	return o, nil
}

func decode(values map[string]json.RawMessage) (Decision, error) {
	var d Decision
	raw, ok := values["handoff/decision"]
	if !ok {
		if len(values) != 0 {
			return d, fmt.Errorf("handoff decision missing from nonempty journal")
		}
		return d, ErrNoBaseline
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return d, err
	}
	return d, d.Validate()
}

// ReadDecision never creates files or repairs the journal. Missing metadata is
// different from a corrupt existing decision; only the former permits bootstrap.
func ReadDecision(dir string) (Decision, error) {
	values, err := state.ReadSnapshot(filepath.Join(dir, "handoff"))
	if err != nil {
		return Decision{}, err
	}
	return decode(values)
}

func (o *Owner) Decision() Decision { o.mu.Lock(); defer o.mu.Unlock(); return o.decision }

func (o *Owner) ready() error {
	if o.journal == nil {
		return fmt.Errorf("state owner closed")
	}
	return o.journal.Healthy()
}

func (o *Owner) save(d Decision) error {
	if err := o.ready(); err != nil {
		return err
	}
	if err := d.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err = o.journal.Replace("handoff/", map[string]json.RawMessage{"handoff/decision": raw}); err != nil {
		return err
	}
	o.decision = d
	return nil
}

func (o *Owner) Bootstrap() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	if o.decision.Phase != "" {
		return ErrConflict
	}
	return o.save(Decision{Phase: Stable, Source: o.identity, Owner: o.identity, Epoch: p.ID("epoch_"), ColdStart: o.identity.Release})
}

// Recover installs a new incarnation of the selected cold-start artifact. The
// previous decision remains available to the caller for recovery reporting.
// Service lifecycle management must first remove all old process incarnations.
func (o *Owner) Recover() (Decision, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	previous := o.decision
	if err := o.ready(); err != nil {
		return previous, err
	}
	if previous.Phase == "" || previous.ColdStart != o.identity.Release || previous.Owner.Incarnation == o.identity.Incarnation {
		return previous, ErrConflict
	}
	err := o.save(Decision{Phase: Stable, Source: o.identity, Owner: o.identity, Epoch: p.ID("epoch_"), ColdStart: o.identity.Release})
	return previous, err
}

func (o *Owner) Prepare(id, rollout string, target Identity) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.Owner == o.identity && d.ID == id && d.Rollout == rollout && d.Target == target && (d.Phase == Prepared || d.Phase == Sealed) {
		return nil
	}
	if d.Owner != o.identity || (d.Phase != Stable && d.Phase != Completed && d.Phase != Aborted) {
		return ErrConflict
	}
	if id == d.ID {
		return ErrConflict
	}
	return o.save(Decision{ID: id, Rollout: rollout, Phase: Prepared, Source: o.identity, Target: target, Owner: o.identity, Epoch: d.Epoch, NextEpoch: p.ID("epoch_"), ColdStart: d.ColdStart})
}

func (o *Owner) Seal(id string, snapshot Snapshot) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.ID != id || d.Owner != o.identity {
		return ErrConflict
	}
	if d.Phase == Sealed && d.Snapshot == snapshot {
		return nil
	}
	if d.Phase != Prepared {
		return ErrConflict
	}
	d.Phase, d.Snapshot = Sealed, snapshot
	return o.save(d)
}

func (o *Owner) Commit(id string, snapshot Snapshot) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.ID != id || d.Target != o.identity || d.Snapshot != snapshot {
		return ErrConflict
	}
	if d.Phase == Committed || d.Phase == Completed {
		return nil
	}
	if d.Phase != Sealed {
		return ErrConflict
	}
	d.Phase, d.Owner, d.Epoch, d.ColdStart = Committed, o.identity, d.NextEpoch, o.identity.Release
	return o.save(d)
}

func (o *Owner) Abort(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.ID != id {
		return ErrConflict
	}
	if d.Phase == Committed || d.Phase == Completed {
		return ErrCommitted
	}
	if d.Source != o.identity || (d.Phase != Prepared && d.Phase != Sealed && d.Phase != Aborted) {
		return ErrConflict
	}
	if d.Phase == Aborted {
		return nil
	}
	d.Phase = Aborted
	return o.save(d)
}

func (o *Owner) Complete(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	d := o.decision
	if d.ID != id || d.Owner != o.identity {
		return ErrConflict
	}
	if d.Phase == Completed {
		return nil
	}
	if d.Phase != Committed {
		return ErrConflict
	}
	d.Phase = Completed
	return o.save(d)
}

func (o *Owner) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	var err error
	if o.journal != nil {
		err = o.journal.Close()
		o.journal = nil
	}
	if o.lock != nil {
		err = errors.Join(err, syscall.Flock(int(o.lock.Fd()), syscall.LOCK_UN), o.lock.Close())
		o.lock = nil
	}
	return err
}
