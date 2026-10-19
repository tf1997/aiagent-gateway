package shared

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

type Receipt struct {
	Key         string   `json:"key"`
	Fingerprint string   `json:"fingerprint"`
	Response    Response `json:"response"`
}

// Transfer preserves lease identity and operation receipts across a planned
// handoff. Runtime state is supplied by the backend in the same sealed section.
// It is not a cold-start image: dead process leases must not be resurrected.
type Transfer struct {
	Epoch    string        `json:"epoch"`
	Revision uint64        `json:"logical_revision"`
	Leases   []LeaseRecord `json:"leases"`
	Receipts []Receipt     `json:"receipts"`
}

func (s *Server) Seal(ctx context.Context) error { return s.barrier.seal(ctx) }
func (s *Server) Resume()                        { s.barrier.resume() }

func (s *Server) Export() (Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.barrier.quiescent() {
		return Transfer{}, fmt.Errorf("STATE_NOT_SEALED")
	}
	v := Transfer{Epoch: s.epoch, Revision: s.revision}
	for _, l := range s.leases.items {
		v.Leases = append(v.Leases, l.record)
	}
	sort.Slice(v.Leases, func(i, j int) bool { return v.Leases[i].ID < v.Leases[j].ID })
	for _, key := range s.order {
		r := s.replies[key]
		v.Receipts = append(v.Receipts, Receipt{Key: key, Fingerprint: r.fingerprint, Response: copyResponse(r.response)})
	}
	return v, nil
}

// Import is called on an unused, sealed dispatcher after its backend has been
// restored. The caller must hold the process owner lock, verify the sealed
// snapshot, commit ownership, then explicitly activate clients and Resume.
func (s *Server) Import(v Transfer, nextEpoch string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.barrier.quiescent() || s.revision != 0 || len(s.leases.items) != 0 || len(s.replies) != 0 {
		return fmt.Errorf("STATE_IMPORT_REQUIRES_UNUSED_SEALED_DISPATCHER")
	}
	if v.Epoch == "" || nextEpoch == "" || nextEpoch == v.Epoch || len(v.Receipts) > 8192 || s.backend.RestoreLease == nil {
		return fmt.Errorf("INVALID_STATE_TRANSFER")
	}
	seen := map[string]bool{}
	for _, l := range v.Leases {
		if l.ID == "" || l.Worker == "" || seen[l.ID] {
			return fmt.Errorf("INVALID_TRANSFER_LEASE")
		}
		seen[l.ID] = true
	}
	seen = map[string]bool{}
	for _, r := range v.Receipts {
		fingerprint, err := hex.DecodeString(r.Fingerprint)
		if r.Key == "" || seen[r.Key] || err != nil || len(fingerprint) != sha256.Size || r.Response.Epoch != v.Epoch {
			return fmt.Errorf("INVALID_TRANSFER_RECEIPT")
		}
		seen[r.Key] = true
	}
	leases := map[string]lease{}
	for _, record := range v.Leases {
		release, err := s.backend.RestoreLease(record)
		if err != nil {
			for _, restored := range leases {
				restored.release()
			}
			return fmt.Errorf("restore lease: %w", err)
		}
		leases[record.ID] = lease{record: record, release: release}
	}
	for _, r := range v.Receipts {
		out := copyResponse(r.Response)
		out.Epoch = nextEpoch
		s.replies[r.Key] = cachedReply{fingerprint: r.Fingerprint, response: out}
		s.order = append(s.order, r.Key)
	}
	s.leases.items = leases
	s.previousEpoch = v.Epoch
	s.epoch, s.revision = nextEpoch, v.Revision
	return nil
}

// EncodeTransfer applies a byte limit before the image may be sealed or sent.
func EncodeTransfer(v any, maxBytes int) ([]byte, string, error) {
	if maxBytes <= 0 {
		return nil, "", fmt.Errorf("INVALID_SNAPSHOT_BUDGET")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	if len(raw) > maxBytes {
		return nil, "", fmt.Errorf("SNAPSHOT_CAPACITY")
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}
