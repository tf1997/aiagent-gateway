package process

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/rollout"
)

func drainingController(t *testing.T) (*Gateway, *CandidateProcess, *ProcessController) {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			w.WriteHeader(http.StatusFound)
		case "/fail":
			http.Error(w, "upstream failed", http.StatusInternalServerError)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	t.Cleanup(up.Close)
	policy := p.DefaultPolicy()
	policy.AutoProfiles = false
	policy.Handoff.SnapshotBytes = 4 << 20
	n, candidate, opts, _ := candidateFixtureWithPolicy(t, up.URL, policy.Handoff, func(opts *CandidateOptions) { opts.Service = true })
	traffic := n.entry.Traffic()
	traffic.Revision++
	traffic.Candidate, traffic.Percent, traffic.Rollout = &candidate.target, 100, "result-check"
	if err := n.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	s := &Service{sockets: opts.Listeners, exit: make(chan struct{}), errs: make(chan error, 1)}
	s.node.Store(n)
	now := time.Now()
	c := &ProcessController{service: s, report: ProcessReport{Report: rollout.Report{
		ID: "result-check", Phase: "DRAINING", Policy: policy, Stable: n.target, Candidate: &candidate.target,
		Artifact: candidate.target.Artifact, Since: now, Started: now,
	}, SampleSince: now.Add(-time.Second), HandoffID: "result-handoff", SourcePID: os.Getpid()}}
	s.controller = c
	t.Cleanup(s.close)
	if err := candidate.Accept(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.save(); err != nil {
		t.Fatal(err)
	}
	return n, candidate, c
}

func controllerPublicRequest(t *testing.T, c *ProcessController, path string) int {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	req, err := http.NewRequest("POST", "http://"+c.service.sockets[publicSocket].Addr().String()+"/agents/test"+path, strings.NewReader(`{"input":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "result-session")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if _, err = io.Copy(io.Discard, res.Body); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode
}

func TestDrainingUnknownResultWaitsAndTimesOut(t *testing.T) {
	n, candidate, c := drainingController(t)
	if code := controllerPublicRequest(t, c, "/redirect"); code != http.StatusFound {
		t.Fatal(code)
	}
	stats := n.entry.Stats(c.report.SampleSince, candidate.target.Incarnation)
	if stats.Counts["http"].Unknown != 1 || stats.Active != 0 {
		t.Fatal("missing completed unknown result", stats)
	}
	if err := c.Tick(context.Background()); err != nil || c.report.Phase != "DRAINING" {
		t.Fatal("unknown result advanced handoff", c.report.Phase, err)
	}
	c.report.Since = time.Now().Add(-time.Duration(c.report.Policy.DrainSeconds+1) * time.Second)
	if err := c.Tick(context.Background()); err != nil || c.report.Phase != "RECOVERY_REQUIRED" || c.report.Reason != "result_observation_incomplete" {
		t.Fatal("unknown result did not reach bounded recovery", c.report, err)
	}
	if err := c.Resume(); err != nil {
		t.Fatal(err)
	}
	if err := c.Tick(context.Background()); err != nil || c.report.Phase != "DRAINING" {
		t.Fatal("resume discarded unknown evidence", c.report.Phase, err)
	}
}

func TestControllerChecksResultsBeforeAndDuringHandoff(t *testing.T) {
	for _, duringPreview := range []bool{false, true} {
		for _, path := range []string{"/ok", "/fail", "/redirect"} {
			t.Run(fmt.Sprintf("preview=%t%s", duringPreview, path), func(t *testing.T) {
				n, candidate, c := drainingController(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if err := c.Tick(ctx); err != nil || c.report.Phase != "HANDOFF" {
					t.Fatal(c.report.Phase, err)
				}
				var pending chan error
				unblock := func() {}
				if duringPreview {
					blocked := &pauseSnapshotReply{Conn: candidate.channel.conn, seen: make(chan struct{}), resume: make(chan struct{})}
					candidate.channel.conn = blocked
					unblock = sync.OnceFunc(func() { close(blocked.resume) })
					defer unblock()
					pending = make(chan error, 1)
					go func() { pending <- c.Tick(ctx) }()
					select {
					case <-blocked.seen:
					case err := <-pending:
						t.Fatalf("handoff ended before preview: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				code := controllerPublicRequest(t, c, path)
				want := map[string]int{"/ok": 200, "/fail": 500, "/redirect": 302}[path]
				if code != want {
					t.Fatalf("request = %d, want %d", code, want)
				}
				unblock()
				var tickErr error
				if pending != nil {
					tickErr = <-pending
				} else {
					tickErr = c.Tick(ctx)
				}
				if tickErr != nil {
					t.Fatal(tickErr)
				}
				d, err := handoff.ReadDecision(n.dataDir)
				if err != nil {
					t.Fatal(err)
				}
				if path == "/ok" {
					if d.Phase != handoff.Committed || !c.committed {
						t.Fatal("valid handoff was blocked", d, c.report)
					}
					return
				}
				if d.Phase == handoff.Committed || d.Phase == handoff.Completed || !n.Ready() || n.shared.BarrierStatus().Sealed {
					t.Fatal("invalid results committed or left source unavailable", d, c.report)
				}
				if duringPreview || path == "/fail" {
					if c.report.Phase != "ROLLING_BACK" {
						t.Fatal("invalid candidate did not roll back", c.report)
					}
					if err := c.Tick(ctx); err != nil || c.report.Phase != "ROLLED_BACK" {
						t.Fatal("rollback did not complete", c.report, err)
					}
				} else if c.report.Phase != "RECOVERY_REQUIRED" {
					t.Fatal("unknown results were not preserved for recovery", c.report)
				}
			})
		}
	}
}
