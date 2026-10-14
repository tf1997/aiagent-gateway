package protocol

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
)

func TestUpstreamErrorRedactionAndNumbers(t *testing.T) {
	raw := `{"error":{"message":"denied: route-secret; Bearer other-secret","api_key":"provider-secret"},"trace":9007199254740993,"nested":[{"Authorization":"Basic abc123","password":"password-secret"}],"url":"https://example.test/?token=url-secret"}`
	detail := ReadUpstreamError(401, strings.NewReader(raw), "route-secret")
	encoded, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"route-secret", "other-secret", "provider-secret", "abc123", "password-secret", "url-secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("credential exposed", secret, string(encoded))
		}
	}
	if !strings.Contains(string(encoded), "9007199254740993") || !strings.Contains(string(encoded), "denied:") || detail.Status != 401 {
		t.Fatal("diagnostics lost", string(encoded))
	}
}

func TestUpstreamErrorBoundsAndPartialRead(t *testing.T) {
	prefix := `{"api_key":"provider-secret","message":"`
	detail := ReadUpstreamError(502, strings.NewReader(prefix+strings.Repeat("x", maxUpstreamErrorBody)+`"}`))
	text, ok := detail.Body.(string)
	if !ok || !detail.Truncated || len(text) > maxUpstreamErrorBody || strings.Contains(text, "provider-secret") {
		t.Fatal("oversized error not bounded/redacted")
	}
	detail = ReadUpstreamError(502, io.MultiReader(strings.NewReader("provider failed"), failingErrorReader{}))
	if detail.Body != "provider failed" || detail.Message != "connection reset" {
		t.Fatal("partial response lost", detail)
	}
	detail = DescribeUpstreamError(0, nil, &url.Error{Op: "Get", URL: "https://example.test/?key=private", Err: errors.New("connection refused")})
	if detail.Message != "connection refused" {
		t.Fatal("transport cause lost or request URL exposed", detail)
	}
}

type failingErrorReader struct{}

func (failingErrorReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }
