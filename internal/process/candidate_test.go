package process

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"go.yaml.in/yaml/v3"
)

func TestCandidateProcessHelper(t *testing.T) {
	if os.Args[len(os.Args)-1] != "candidate-helper" {
		t.Skip("subprocess entry")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err = ServeCandidate(ctx, hex.EncodeToString(digest[:])); err != nil && ctx.Err() == nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func candidateFixture(t *testing.T, upstream string) (*Gateway, *CandidateProcess, CandidateOptions, string) {
	t.Helper()
	// Unix paths must fit sockaddr_un on both supported operating systems.
	base, err := os.MkdirTemp("/tmp", "gw-peer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	root := filepath.Join(base, "releases")
	artifact := filepath.Join(root, hex.EncodeToString(digest[:]), "gateway")
	if err = os.MkdirAll(filepath.Dir(artifact), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(artifact, raw, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(artifact, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var sockets [3]*Socket
	for i, addr := range []struct{ network, address string }{{"tcp", "127.0.0.1:0"}, {"tcp", "127.0.0.1:0"}, {"unix", filepath.Join(base, "control.sock")}} {
		sockets[i], err = Listen(addr.network, addr.address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sockets[i].Close() })
	}
	cfg := appconfig.Defaults()
	cfg.DataDir, cfg.StateSocket, cfg.AdminToken = filepath.Join(base, "data"), filepath.Join(base, "state.sock"), "admin-secret"
	_, cfg.Port, _ = net.SplitHostPort(sockets[0].Addr().String())
	_, cfg.AdminPort, _ = net.SplitHostPort(sockets[1].Addr().String())
	configPath := filepath.Join(base, "config.yaml")
	encoded, _ := yaml.Marshal(cfg)
	if err = os.WriteFile(configPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	id := p.Identity{PID: os.Getpid(), Release: strings.Repeat("a", 64), Incarnation: "source", Instance: "a"}
	target := p.Target{Release: id.Release, Incarnation: id.Incarnation, Instance: id.Instance, URL: "http://gateway", Socket: filepath.Join(base, "source.sock")}
	n, err := OpenGateway(cfg, target, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	configureGateway(t, n, upstream)
	opts := CandidateOptions{ConfigPath: configPath, Artifact: artifact, ReleasesDir: root, OwnerUID: uint32(os.Geteuid()), Listeners: sockets, MaxSnapshotBytes: 4 << 20, Output: os.Stderr}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := n.startCandidate(ctx, opts, []string{"-test.run=^TestCandidateProcessHelper$", "--", "candidate-helper"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-c.done:
		default:
			_ = c.cmd.Process.Kill()
			<-c.done
		}
		c.Detach()
	})
	return n, c, opts, "http://" + sockets[0].Addr().String()
}

func candidateRequest(t *testing.T, c *CandidateProcess, path, session string) *http.Response {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", c.target.Socket)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	req, _ := http.NewRequest("POST", "http://gateway/agents/test"+path, strings.NewReader(`{"input":"hello"}`))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", session)
	req.Header.Set(p.AuthHeader, c.token)
	req.Header.Set(p.WorkerHeader, c.target.Incarnation)
	req.Header.Set(p.RequestHeader, p.ID("request_"))
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestCandidateProcessKeepsLiveStreamAcrossCommit(t *testing.T) {
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
			fmt.Fprint(w, "data: started\n\n")
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
	if repeated, err := n.StartCandidate(ctx, opts); err != nil || repeated != c {
		t.Fatal("duplicate submission started another child", err)
	}
	other := opts
	other.Artifact += "-another"
	if _, err := n.StartCandidate(ctx, other); err == nil {
		t.Fatal("third process allowed")
	}
	// Before commit the inherited public descriptors have not started Accept.
	probe := &http.Client{Timeout: 100 * time.Millisecond}
	if res, err := probe.Get(address + "/readyz"); err == nil {
		res.Body.Close()
		t.Fatal("candidate accepted public request before commit")
	}
	stream := candidateRequest(t, c, "/stream", "live")
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, "started") {
		t.Fatal("stream did not start", line, err)
	}
	if n.shared.Count(c.target.Incarnation) != 1 {
		t.Fatal("candidate stream has no source lease")
	}
	d, err := n.PrepareHandoff(ctx, "handoff", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateHandoff(ctx, n, d, opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Commit(ctx, d); err == nil {
		t.Fatal("candidate committed while source held writer locks")
	}
	if err = n.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Commit(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err = c.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.Stop(ctx, n); !errors.Is(err, handoff.ErrCommitted) {
		t.Fatal("old owner could stop committed process", err)
	}
	close(release)
	if body, err := io.ReadAll(reader); err != nil || !strings.Contains(string(body), "[DONE]") {
		t.Fatal("stream failed across owner change", string(body), err)
	}
	for {
		id, err := c.Identity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if id.Inflight == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("lease did not release", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Detach() // committed child must survive source channel EOF
	req, _ := http.NewRequest("POST", address+"/agents/test/ok", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "live")
	res, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || calls.Load() != 2 {
		body, _ := io.ReadAll(res.Body)
		t.Fatal("candidate not independent or upstream replayed", res.StatusCode, string(body), calls.Load())
	}
}

func TestCandidatePrecommitEOFLeavesSourceAuthoritative(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, _, _ := candidateFixture(t, upstream.URL)
	c.Detach()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("orphan candidate stayed alive")
	}
	if !n.Ready() {
		t.Fatal("candidate exit stopped original owner")
	}
	if w := processRequest(n, "/agents/test/ok", "existing"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

// Consume the actual reply, then lose it at the sender's transport boundary.
// This proves the decision was committed before the caller sees an I/O error.
type lostPeerReply struct{ net.Conn }

func (c lostPeerReply) Read([]byte) (int, error) {
	_, err := readPeer(c.Conn)
	if err != nil {
		return 0, err
	}
	return 0, io.ErrUnexpectedEOF
}

func TestCandidateRecoversActivationAfterLostCommitReply(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer upstream.Close()
	n, c, opts, address := candidateFixture(t, upstream.URL)
	cfg, err := appconfig.Load(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d, err := n.PrepareHandoff(ctx, "lost-reply", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateHandoff(ctx, n, d, opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if err = n.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	c.channel.conn = lostPeerReply{c.channel.conn}
	if _, err = c.Commit(ctx, d); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("commit reply was not lost", err)
	}
	if err = c.Stop(ctx, n); !errors.Is(err, handoff.ErrCommitted) {
		t.Fatal("lost reply allowed the source to kill the owner", err)
	}
	if restored, err := RestoreGateway(ctx, cfg, n.target, n.identity, d, opts.MaxSnapshotBytes); err == nil {
		restored.Close()
		t.Fatal("lost reply allowed rollback")
	}
	if _, err = c.RecoverCommitted(ctx, cfg.DataDir, d); err != nil {
		t.Fatal("could not recover activation", err)
	}
	if _, err = c.RecoverCommitted(ctx, cfg.DataDir, d); err != nil {
		t.Fatal("recovery not idempotent", err)
	}
	req, _ := http.NewRequest("POST", address+"/agents/test/ok", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer alice-secret")
	req.Header.Set("X-Session-ID", "after-recovery")
	res, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || calls.Load() != 1 {
		t.Fatal("recovery replayed business or failed to serve", res.StatusCode, calls.Load())
	}
}

func TestCandidateStreamSurvivesYieldedSourceRestoration(t *testing.T) {
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			fmt.Fprint(w, `{}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: started\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			fmt.Fprint(w, "data: [DONE]\n\n")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	cfg, err := appconfig.Load(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream := candidateRequest(t, c, "/stream", "live-restoration")
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, "started") {
		t.Fatal(line, err)
	}
	d, err := n.PrepareHandoff(ctx, "restore", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateHandoff(ctx, n, d, opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if err = n.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreGateway(ctx, cfg, n.target, n.identity, d, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	aborted, stopAbort := context.WithCancel(ctx)
	stopAbort()
	if err = c.RestoreSource(aborted, restored, d); !errors.Is(err, context.Canceled) {
		t.Fatal("restore command cancellation was ignored", err)
	}
	if restored.candidate != c {
		t.Fatal("uncertain restoration lost a live candidate and allowed a third instance")
	}
	other := opts
	other.Artifact += "-third"
	if _, err = restored.StartCandidate(ctx, other); err == nil || !strings.Contains(err.Error(), "CANDIDATE_ALREADY_ACTIVE") {
		t.Fatal("third candidate was not fenced after uncertain restoration", err)
	}
	if err = c.RestoreSource(ctx, restored, d); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Commit(ctx, d); err == nil {
		t.Fatal("late commit succeeded after ABORTED")
	}
	if restored.shared.Count(c.target.Incarnation) != 1 {
		t.Fatal("source lost candidate lease")
	}
	close(release)
	if body, err := io.ReadAll(reader); err != nil || !strings.Contains(string(body), "[DONE]") {
		t.Fatal("stream did not finish after restoration", string(body), err)
	}
	for restored.shared.Count(c.target.Incarnation) != 0 {
		if ctx.Err() != nil {
			t.Fatal("restored source retained finished lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The restored allocator also needs the original ephemeral peer credential.
	traffic := restored.entry.Traffic()
	traffic.Revision++
	traffic.Candidate, traffic.Percent = &c.target, 100
	if err = restored.entry.Apply(traffic.Revision-1, traffic); err != nil {
		t.Fatal(err)
	}
	if res := processRequest(restored, "/agents/test/ok", "live-restoration"); res.Code != 200 {
		t.Fatal("restored entry could not reach candidate", res.Code, res.Body.String())
	}
	if err = c.Stop(ctx, restored); err != nil {
		t.Fatal("restored candidate did not drain", err)
	}
}

func TestPeerFrameRejectsOversizeAndTruncation(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxPeerFrame+1)
	if _, err := readPeer(strings.NewReader(string(header[:]))); err == nil {
		t.Fatal("unbounded frame accepted")
	}
	binary.BigEndian.PutUint32(header[:], 20)
	if _, err := readPeer(strings.NewReader(string(header[:]) + `{}`)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("partial frame accepted", err)
	}
}
