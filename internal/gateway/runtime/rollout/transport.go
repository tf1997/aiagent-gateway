package rollout

import (
	"net/http"
	"sync/atomic"

	"aiagent-gateway/internal/gateway/protocol"
)

type attemptKey = protocol.AttemptKey

type proxyAttempt struct {
	sent   atomic.Bool
	failed atomic.Bool
}

type observedTransport struct{ base http.RoundTripper }

func (t observedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	a, _ := r.Context().Value(attemptKey{}).(*proxyAttempt)
	if a != nil && r.Header.Get("X-Gateway-Caller-ID") != "" {
		a.sent.Store(true)
	}
	res, err := t.base.RoundTrip(r)
	if a != nil && err != nil && r.Context().Err() == nil && a.sent.Load() {
		a.failed.Store(true)
	}
	return res, err
}

// ObserveTransport records dispatch and transport failures for worker verdicts.
func ObserveTransport(base http.RoundTripper) http.RoundTripper {
	return observedTransport{base: base}
}
