package a2a

import (
	"encoding/json"
	"errors"
	"net/http"

	"aiagent-gateway/internal/gateway/protocol"
)

type upstreamRPCFailure struct{ detail map[string]any }

func (e *upstreamRPCFailure) Error() string { return protocol.StringField(e.detail, "message") }

func writeUpstreamRPCError(w http.ResponseWriter, id json.RawMessage, message string, err error) {
	var rpc *upstreamRPCFailure
	if errors.As(err, &rpc) {
		writeRPCError(w, id, rpc.detail)
		return
	}
	detail := map[string]any{"code": -32603, "message": message}
	var upstream *protocol.UpstreamError
	if errors.As(err, &upstream) {
		detail["data"] = map[string]any{"upstream": upstream}
	}
	writeRPCError(w, id, detail)
}

func upstreamHTTPError(status int, raw []byte, readErr error, id json.RawMessage, token string) error {
	if readErr == nil && len(raw) <= 2<<20 {
		if env, err := decodeEnvelope(raw, id, token); err == nil {
			if detail, ok := env["error"].(map[string]any); ok {
				return &upstreamRPCFailure{detail: detail}
			}
		}
	}
	return protocol.DescribeUpstreamError(status, raw, readErr, token)
}
