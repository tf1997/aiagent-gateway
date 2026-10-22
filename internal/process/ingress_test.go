package process

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func publicRequest(t *testing.T, address, path, session string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", address+"/agents/test/"+path, strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", session)
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		t.Fatalf("public response %d: %s", res.StatusCode, raw)
	}
	return res
}

func TestCandidateDirectIngressSurvivesHandoff(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"started\":true}\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-release:
				fmt.Fprint(w, "data: [DONE]\n\n")
			case <-r.Context().Done():
			}
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	n, c, opts, address := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	traffic := n.entry.Traffic()
	traffic.Revision++
	traffic.Candidate = &c.target
	traffic.Percent = 100
	traffic.Rollout = "direct"
	if err := n.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	if err := c.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	res := publicRequest(t, address, "ok", "direct-ok")
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	for n.entry.Stats(time.Time{}, c.target.Incarnation).IngressCounts["http"].Success != 1 {
		if time.Now().After(deadline) {
			t.Fatal("direct ingress receipt not recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stream := publicRequest(t, address, "stream", "direct-stream")
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, "started") {
		t.Fatal(line, err)
	}
	if n.entry.LocalActive() != 0 || n.entry.Active() != 1 {
		t.Fatal("source drain includes candidate connection")
	}
	result, err := n.TransferHandoff(ctx, c, "direct-handoff", "direct", opts.MaxSnapshotBytes)
	if err != nil || !result.Activated {
		t.Fatal(result, err)
	}
	close(release)
	if raw, err := io.ReadAll(reader); err != nil || !strings.Contains(string(raw), "[DONE]") {
		t.Fatal(string(raw), err)
	}
	for {
		reply, err := c.channel.call(ctx, peerMessage{Action: "stats"})
		if err != nil {
			t.Fatal(err)
		}
		if reply.Stats.Active == 0 && reply.Stats.IngressCounts["sse"].Success == 1 && reply.Stats.IngressCounts["http"].Success == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("candidate lost migrated evidence: %+v", reply.Stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Detach()
	res = publicRequest(t, address, "ok", "independent")
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if calls.Load() != 3 {
		t.Fatal("upstream replay", calls.Load())
	}
}

func TestCandidateIngressRollbackAndReaccept(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, _, address := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	traffic := n.entry.Traffic()
	traffic.Revision++
	traffic.Candidate = &c.target
	traffic.Percent = 0
	traffic.Rollout = "rollback"
	if err := n.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Accept(ctx); err != nil {
			t.Fatal(err)
		}
		res := publicRequest(t, address, "ok", fmt.Sprint(i))
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if err := c.Pause(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 {
		t.Fatal("rollback execution failed or replayed", calls.Load())
	}
	stats := n.entry.Stats(time.Time{}, n.target.Incarnation)
	if stats.Counts["http"].Success != 3 || stats.IngressCounts["http"].Success != 0 {
		t.Fatalf("wrong ingress/execution identities: %+v", stats)
	}
}
