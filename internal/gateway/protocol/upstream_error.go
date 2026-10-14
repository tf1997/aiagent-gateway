package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const maxUpstreamErrorBody = 64 << 10

// UpstreamError carries diagnostics separately from the gateway's stable code.
// Body is a JSON value when possible, otherwise text. No response headers are
// copied, and error bodies are bounded independently of successful responses.
type UpstreamError struct {
	Status    int    `json:"status,omitempty"`
	Body      any    `json:"body,omitempty"`
	Message   string `json:"message,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

func (e *UpstreamError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("upstream HTTP %d", e.Status)
}

func ReadUpstreamError(status int, body io.Reader, secrets ...string) *UpstreamError {
	raw, err := io.ReadAll(io.LimitReader(body, maxUpstreamErrorBody+1))
	return DescribeUpstreamError(status, raw, err, secrets...)
}

func DescribeUpstreamError(status int, raw []byte, cause error, secrets ...string) *UpstreamError {
	e := &UpstreamError{Status: status, Truncated: len(raw) > maxUpstreamErrorBody}
	if e.Truncated {
		raw = raw[:maxUpstreamErrorBody]
	}
	if len(raw) > 0 {
		var value any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if !json.Valid(raw) || d.Decode(&value) != nil {
			value = string(raw)
		}
		e.Body = RedactErrorDetails(value, secrets...)
	}
	if cause != nil {
		// net/http wraps transport failures with a URL, which can contain
		// credentials in query parameters. The underlying cause is sufficient.
		var u *url.Error
		if errors.As(cause, &u) {
			cause = u.Err
		}
		e.Message = redactErrorText(cause.Error(), secrets)
	}
	return e
}

func UpstreamProblem(w http.ResponseWriter, status int, code string, err error) {
	value := map[string]any{"error": code}
	var detail *UpstreamError
	if errors.As(err, &detail) {
		value["upstream"] = detail
	}
	WriteJSON(w, status, value)
}

var authorizationPattern = regexp.MustCompile(`(?i)\b(Bearer|Basic)\s+[a-z0-9._~+/=-]+`)
var credentialPattern = regexp.MustCompile(`(?i)(["']?\b(?:api[_-]?key|x[_-]?api[_-]?key|access[_-]?token|refresh[_-]?token|token|password|secret|client[_-]?secret)["']?\s*[:=]\s*)(?:"[^"]*(?:"|$)|'[^']*(?:'|$)|[^\s,;&}]+)`)
var credentialKeyNormalizer = strings.NewReplacer("_", "", "-", "")

func redactErrorText(s string, secrets []string) string {
	s = credentialPattern.ReplaceAllString(s, `$1"[REDACTED]"`)
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	return authorizationPattern.ReplaceAllString(s, "$1 [REDACTED]")
}

// RedactErrorDetails retains provider messages, codes and diagnostic data while
// hiding known upstream tokens and explicitly named credential fields.
func RedactErrorDetails(value any, secrets ...string) any {
	switch v := value.(type) {
	case string:
		return redactErrorText(v, secrets)
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			name := strings.ToLower(credentialKeyNormalizer.Replace(k))
			switch name {
			case "authorization", "proxyauthorization", "cookie", "setcookie", "apikey", "xapikey", "token", "accesstoken", "refreshtoken", "password", "secret", "clientsecret":
				out[k] = "[REDACTED]"
			default:
				out[k] = RedactErrorDetails(item, secrets...)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = RedactErrorDetails(item, secrets...)
		}
		return out
	default:
		return value
	}
}
