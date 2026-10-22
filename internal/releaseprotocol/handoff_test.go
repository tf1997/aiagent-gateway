package releaseprotocol

import (
	"encoding/json"
	"testing"
)

func TestHandoffPolicyDefaultsOverridesAndBounds(t *testing.T) {
	var empty HandoffPolicy
	if empty.Effective() != DefaultHandoffPolicy() || empty.Validate() != nil {
		t.Fatal("missing policy did not get explicit defaults")
	}
	var override HandoffPolicy
	if err := json.Unmarshal([]byte(`{"snapshot_bytes":1048576,"state_wait_milliseconds":25}`), &override); err != nil {
		t.Fatal(err)
	}
	effective := override.Effective()
	if effective.SnapshotBytes != 1048576 || effective.StateWaitMillis != 25 || effective.BarrierMillis != 5000 || effective.Validate() != nil {
		t.Fatal("policy override lost defaults", effective)
	}
	for _, bad := range []HandoffPolicy{
		{PrepareSeconds: -1}, {BarrierMillis: 60001}, {StateWaitMillis: -1},
		{QueueRequests: 65537}, {QueueBytes: 1<<30 + 1}, {SnapshotBytes: 256<<20 + 1},
		{DeltaBytes: -1}, {ActivationSeconds: 3601},
	} {
		policy := DefaultPolicy()
		policy.Handoff = bad
		if policy.Validate() == nil {
			t.Fatal("invalid handoff policy accepted", bad)
		}
	}
}
