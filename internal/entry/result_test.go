package entry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

type failedResponseWriter struct{ header http.Header }

func (w *failedResponseWriter) Header() http.Header       { return w.header }
func (w *failedResponseWriter) WriteHeader(int)           {}
func (w *failedResponseWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestDownstreamWriteErrorPreservesConfirmedFailure(t *testing.T) {
	for _, verdict := range []string{"failure", "success", "unknown"} {
		t.Run(verdict, func(t *testing.T) {
			e, _, _ := receiptFixture(t)
			e.resolve = func(context.Context, p.Request) (p.Resolution, error) { return p.Resolution{}, nil }
			if err := e.SetLocal(*e.Traffic().Candidate, func(w http.ResponseWriter, r *http.Request, done func(p.Result)) {
				status := http.StatusOK
				if verdict == "failure" {
					status = http.StatusInternalServerError
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte("result"))
				done(p.Result{Status: status, Profile: "http", Verdict: verdict, Reason: "worker_result"})
			}); err != nil {
				t.Fatal(err)
			}
			e.ServeHTTP(&failedResponseWriter{header: http.Header{}}, httptest.NewRequest("GET", "http://gateway/result", nil))
			stats := e.Stats(time.Time{}, "b")
			want := p.Counts{Excluded: 1}
			if verdict == "failure" {
				want = p.Counts{Failure: 1}
			}
			if stats.Counts["http"] != want || stats.Failures != want.Failure || stats.Active != 0 {
				t.Fatal("downstream error changed known failure or retained success", stats)
			}
		})
	}
}
