package process

import (
	"aiagent-gateway/internal/entry"
	"aiagent-gateway/internal/gateway/runtime"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
	"encoding/json"
	"strings"
)

func processRecords(raw []byte) (gatewayImage, map[string]json.RawMessage, error) {
	var v gatewayImage
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, nil, err
	}
	records, err := runtime.StateRecords(v.Shared)
	if err != nil {
		return v, nil, err
	}
	for key, raw := range entry.StateRecords(v.Entry) {
		records[key] = raw
	}
	for key, raw := range v.Release {
		records["release/"+key] = raw
	}
	// Keep only immutable handoff identity, never another full baseline copy.
	v.Shared, v.Release, v.Entry = nil, nil, entry.Transfer{}
	return v, records, nil
}

func renderProcessRecords(v gatewayImage, records map[string]json.RawMessage, at state.Cursor, maxBytes int) ([]byte, error) {
	var err error
	v.Shared, v.SharedDigest, err = runtime.RenderStateRecords(records, at.Epoch, at.Revision, maxBytes)
	if err != nil {
		return nil, err
	}
	v.Entry, err = entry.RenderStateRecords(records)
	if err != nil {
		return nil, err
	}
	v.Release = map[string]json.RawMessage{}
	for key, raw := range records {
		if strings.HasPrefix(key, "release/") {
			v.Release[strings.TrimPrefix(key, "release/")] = raw
		}
	}
	raw, _, err := shared.EncodeTransfer(v, maxBytes)
	return raw, err
}
