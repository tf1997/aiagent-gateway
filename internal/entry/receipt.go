package entry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

const maxResultReceipts = 65536

// ResultReceipt keeps exact sampling fields and a fingerprint of the complete
// response. Completed requests no longer retain repeated release/instance,
// routing, status and reason fields. Retries still verify the entire result.
type ResultReceipt struct {
	Digest      string    `json:"digest"`
	Started     time.Time `json:"started"`
	Incarnation string    `json:"incarnation"`
	Profile     string    `json:"profile"`
	Verdict     string    `json:"verdict"`
	Direct      bool      `json:"direct,omitempty"`
}

func resultDigest(r p.Result) string {
	r.Started = r.Started.Round(0).UTC()
	r.Completed = r.Completed.Round(0).UTC()
	raw, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func resultReceipt(r p.Result) ResultReceipt {
	return ResultReceipt{Digest: resultDigest(r), Started: r.Started, Incarnation: r.Incarnation,
		Profile: r.Profile, Verdict: r.Verdict, Direct: r.Ingress == r.Incarnation}
}

func (r ResultReceipt) validate() error {
	digest, err := hex.DecodeString(r.Digest)
	if err != nil || len(digest) != sha256.Size || r.Started.IsZero() || r.Incarnation == "" {
		return fmt.Errorf("INVALID_ENTRY_RESULT_RECEIPT")
	}
	return nil
}

// UseCompactResults is chosen after peer capability negotiation and before a
// new rollout. Peers without receipt support keep the original representation.
func (e *Entry) UseCompactResults(enabled bool) error {
	leave, err := e.admit()
	if err != nil {
		return err
	}
	defer leave()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sealed || e.traffic.Rollout != "" || len(e.results) != 0 || len(e.receipts) != 0 {
		return fmt.Errorf("ENTRY_RESULT_MODE_IN_USE")
	}
	if err := e.j.Healthy(); err != nil {
		return err
	}
	e.compact = enabled
	return e.save()
}
