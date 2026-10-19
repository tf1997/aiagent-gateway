package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// DecodeTransfer accepts only the canonical encoding emitted by EncodeTransfer.
// This rejects duplicate/case-aliased DTO keys and lossy number conversions as
// well as unknown fields. The expected digest must come from the handoff peer.
func DecodeTransfer(raw []byte, digest string, maxBytes int, out any) error {
	if maxBytes <= 0 {
		return fmt.Errorf("INVALID_SNAPSHOT_BUDGET")
	}
	if len(raw) > maxBytes {
		return fmt.Errorf("SNAPSHOT_CAPACITY")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("SNAPSHOT_DIGEST_MISMATCH")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("INVALID_SNAPSHOT: %w", err)
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, raw) {
		return fmt.Errorf("NON_CANONICAL_SNAPSHOT")
	}
	return nil
}
