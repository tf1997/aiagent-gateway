package process

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
)

func TestObservationWithoutDirectSamplesCanResume(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer upstream.Close()
	n, candidate, _, _ := candidateFixture(t, upstream.URL)
	s := &Service{}
	s.node.Store(n)
	policy := p.DefaultPolicy()
	policy.ObserveSeconds = 1
	since := time.Now().Add(-time.Minute)
	c := &ProcessController{service: s, report: ProcessReport{Report: rollout.Report{ID: "observation", Phase: "OBSERVING", Policy: policy, Stable: n.target, Candidate: &candidate.target, Since: since}, SampleSince: since}}
	if err := c.save(); err != nil {
		t.Fatal(err)
	}
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := c.Report()
	if got.Phase != "RECOVERY_REQUIRED" || got.ResumePhase != "OBSERVING" || got.Committed || !n.Ready() {
		t.Fatal("missing samples did not preserve a resumable owner", got)
	}
	if err := c.Resume(); err != nil {
		t.Fatal(err)
	}
	got = c.Report()
	if got.Phase != "OBSERVING" || !got.SampleSince.Equal(since) || got.Reason != "" {
		t.Fatal("resume discarded existing evidence", got)
	}
	// A slow mutation holds the transition mutex. Reading the last durable
	// report must remain possible so an operator can see the blocked phase.
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(chan ProcessReport, 1)
	go func() { result <- c.Report() }()
	select {
	case r := <-result:
		if r.Phase != "OBSERVING" {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("status blocked on a running transition")
	}
}
