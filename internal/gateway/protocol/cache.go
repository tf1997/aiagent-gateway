package protocol

import (
	"encoding/json"
	"time"
)

type ListSnapshot struct {
	Owner, Route, Filter string
	Revision             uint64
	Expires              time.Time
	Tasks                []json.RawMessage
	Size, Bytes          int
}

type Capabilities struct {
	Interface EndpointInterface `json:"-"`
	Checked   time.Time         `json:"checked_at"`
	Ready     bool              `json:"ready"`
	Features  map[string]any    `json:"features,omitempty"`
}

type AttemptKey struct{}
