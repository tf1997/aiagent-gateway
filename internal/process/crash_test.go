package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"aiagent-gateway/internal/appconfig"
	"aiagent-gateway/internal/gateway/protocol"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/handoff"
	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

type crashCheckpoint struct {
	Config       appconfig.Config
	Source       p.Target
	Candidate    p.Target
	CandidatePID int
	Decision     handoff.Decision
	Creation     shared.Request
	Reservation  protocol.CreationReservation
}

// The helper runs both real gateway runtimes, including an actual candidate
// child. The parent kills the selected process at a named protocol boundary,
// then clears the remaining test process group before cold recovery.
func TestGatewayCrashHelper(t *testing.T) {
	args := os.Args
	if len(args) < 5 || args[len(args)-1] != "gateway-crash-helper" {
		t.Skip("subprocess entry")
	}
	marker, point, upstream := args[len(args)-4], args[len(args)-3], args[len(args)-2]
	n, c, opts, address := candidateFixture(t, upstream)
	if response := processRequest(n, "/agents/test/ok", "source-session"); response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	response := candidateRequest(t, c, "/ok", "candidate-session")
	_, err := io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatal(response.StatusCode, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for n.shared.Count(c.target.Incarnation) != 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if err = ctx.Err(); err != nil {
		t.Fatal(err)
	}
	client := shared.NewLocalClient(n.shared, c.target.Incarnation)
	var config, reserved, pin shared.Response
	if err = client.Call("config", shared.Request{}, &config); err != nil {
		t.Fatal(err)
	}
	route := config.Config.Routes[0]
	creation := shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version,
		Creation: protocol.ResourceCreation{Kind: protocol.HermesSession, Alias: "crash-resource", Operation: "create", Fingerprint: "original-body", IdempotencyKey: "create-once"}}
	if err = client.Call("reserve", creation, &reserved); err != nil || !reserved.Reservation.First {
		t.Fatal("resource reservation failed", err)
	}
	if err = client.Call("pin", shared.Request{Key: reserved.Reservation.Resource.Key()}, &pin); err != nil {
		t.Fatal(err)
	}
	// This pin intentionally survives until SIGKILL, like the candidate stream.
	stream := candidateRequest(t, c, "/stream", "live-session")
	defer stream.Body.Close()
	if line, err := bufio.NewReader(stream.Body).ReadString('\n'); err != nil || line != "data: started\n" || stream.StatusCode != 200 {
		t.Fatal("candidate stream did not start", line, err, stream.StatusCode)
	}
	if n.shared.Count(c.target.Incarnation) != 2 {
		t.Fatal("stream or resource pin has no active source lease")
	}
	checkpoint := func(stage string, decision handoff.Decision) {
		if stage != point {
			return
		}
		for {
			id, err := c.Identity(ctx)
			// Identity counts the HTTP handler and its execution lease. Wait
			// for any just-completed request to finish recording its result.
			if err != nil || id.Inflight < 2 {
				t.Fatal("candidate lost the live stream before crash", id, err)
			}
			if id.Inflight == 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		raw, err := json.Marshal(crashCheckpoint{n.config, n.target, c.target, c.PID(), decision, creation, reserved.Reservation})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(marker+".tmp", raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(marker+".tmp", marker); err != nil {
			t.Fatal(err)
		}
		select {} // only the parent decides when and which process to SIGKILL
	}
	target := handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}
	if err = n.owner.Prepare("crash", "rollout", target); err != nil {
		t.Fatal(err)
	}
	checkpoint("prepared", n.owner.Decision())
	if _, err = c.FollowHandoff(ctx, n, "crash", "rollout", opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	checkpoint("previewed", n.owner.Decision())
	d, err := n.PrepareHandoff(ctx, "crash", "rollout", target, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint("sealed", d)
	if err = c.ValidateHandoff(ctx, n, d, opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	checkpoint("validated", d)
	if err = n.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	checkpoint("yielded", d)
	d, err = c.Commit(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint("committed", d)
	if err = c.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint("activated", d)
	request, _ := http.NewRequestWithContext(ctx, "POST", address+"/agents/test/ok", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer alice-secret")
	request.Header.Set("X-Session-ID", "after-commit")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatal(response.StatusCode, err)
	}
	checkpoint("post-write", d)
	t.Fatal("unknown crash point")
}

func TestGatewayColdRecoveryAfterProcessKill(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range []string{"prepared", "previewed", "sealed", "validated", "yielded", "committed", "activated", "post-write"} {
		for _, victim := range []string{"source", "candidate"} {
			t.Run(point+"/"+victim, func(t *testing.T) {
				var calls atomic.Int64
				streamDone := make(chan struct{})
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path == "/stream" {
						defer close(streamDone)
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "data: started\n\n")
						w.(http.Flusher).Flush()
						<-r.Context().Done()
						return
					}
					fmt.Fprint(w, `{}`)
				}))
				t.Cleanup(upstream.Close)
				marker := filepath.Join(t.TempDir(), "checkpoint.json")
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestGatewayCrashHelper$", "--", marker, point, upstream.URL, "gateway-crash-helper")
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				var output bytes.Buffer
				cmd.Stdout, cmd.Stderr = &output, &output
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				var checkpoint crashCheckpoint
				waited, cleared := false, false
				t.Cleanup(func() {
					if !cleared {
						_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					}
					if !waited {
						<-done
					}
					if root := filepath.Dir(checkpoint.Config.DataDir); strings.HasPrefix(root, "/tmp/gw-peer-") {
						_ = os.RemoveAll(root)
					}
				})
				for {
					if raw, err := os.ReadFile(marker); err == nil {
						if err = json.Unmarshal(raw, &checkpoint); err != nil {
							t.Fatal(err)
						}
						break
					}
					select {
					case err := <-done:
						waited = true
						t.Fatalf("helper stopped before %s: %v\n%s", point, err, output.String())
					case <-ctx.Done():
						t.Fatalf("helper did not reach %s: %v", point, ctx.Err())
					case <-time.After(5 * time.Millisecond):
					}
				}
				dir := checkpoint.Config.DataDir
				before, err := state.ReadSnapshot(filepath.Join(dir, "shared"))
				if err != nil {
					t.Fatal(err)
				}
				count := calls.Load()
				pid := cmd.Process.Pid
				if victim == "candidate" {
					pid = checkpoint.CandidatePID
				}
				if err = syscall.Kill(pid, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				// Cold start is allowed only after all old service processes exit.
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				cleared = true
				err = <-done
				waited = true
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
					t.Fatalf("helper did not die from SIGKILL: %v\n%s", err, output.String())
				}
				select {
				case <-streamDone:
				case <-ctx.Done():
					t.Fatal("crashed candidate kept the upstream stream alive")
				}
				d, err := handoff.ReadDecision(dir)
				if err != nil || d != checkpoint.Decision {
					t.Fatal("kill changed the durable decision", d, err)
				}
				target := checkpoint.Source
				wrong := checkpoint.Candidate
				if d.Phase == handoff.Committed {
					target, wrong = wrong, target
				}
				wrong.Incarnation = "wrong-restart"
				bad, err := OpenGateway(checkpoint.Config, wrong, p.Identity{Incarnation: wrong.Incarnation, Release: wrong.Release, Instance: wrong.Instance})
				if !errors.Is(err, handoff.ErrReleaseChanged) {
					if bad != nil {
						bad.Close()
					}
					t.Fatal("cold recovery accepted the wrong artifact", err)
				}
				target.Incarnation = "restarted"
				id := p.Identity{Incarnation: target.Incarnation, Release: target.Release, Instance: target.Instance}
				n, err := OpenGateway(checkpoint.Config, target, id)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { n.Close() })
				if n.previous != d || !n.Ready() || n.shared.Count("") != 0 || !reflect.DeepEqual(before, n.journal.Snapshot()) {
					t.Fatal("cold recovery lost durable state or retained dead leases")
				}
				if second, err := OpenGateway(checkpoint.Config, target, id); err == nil {
					second.Close()
					t.Fatal("second writer opened after recovery")
				}
				client := shared.NewLocalClient(n.shared, target.Incarnation)
				defer client.Close()
				var config shared.Response
				if err = client.Call("config", shared.Request{}, &config); err != nil {
					t.Fatal(err)
				}
				var resource, reserved shared.Response
				key := checkpoint.Reservation.Resource.Key()
				if err = client.Call("lookup", shared.Request{Key: key}, &resource); err != nil || resource.Resource.ActiveRefs != 0 || resource.Resource.EndpointID != checkpoint.Reservation.Resource.EndpointID {
					t.Fatal("resource lost affinity or retained a dead pin", resource.Resource, err)
				}
				if err = client.Call("reserve", checkpoint.Creation, &reserved); err != nil || reserved.Reservation.First || reserved.Reservation.Resource.ID != key.ID || reserved.Reservation.UpstreamIdempotencyKey != checkpoint.Reservation.UpstreamIdempotencyKey {
					t.Fatal("cold recovery lost the idempotent reservation", reserved.Reservation, err)
				}
				conflict := checkpoint.Creation
				conflict.Creation.Fingerprint = "different-body"
				if err = client.Call("reserve", conflict, nil); err == nil {
					t.Fatal("cold recovery accepted conflicting idempotent content")
				}
				key.Owner = "another-caller"
				if err = client.Call("lookup", shared.Request{Key: key}, nil); err == nil {
					t.Fatal("cold recovery exposed another caller's resource")
				}
				sessions := 0
				for key, raw := range before {
					if !strings.HasPrefix(strings.TrimPrefix(key, "runtime/"), "session/") {
						continue
					}
					var saved struct {
						Key      struct{ ID string }
						Endpoint string
					}
					if err = json.Unmarshal(raw, &saved); err != nil {
						t.Fatal(err)
					}
					route := config.Config.Routes[0]
					var lease shared.Response
					if err = client.Call("acquire", shared.Request{Owner: "alice", Route: route.Key, Revision: route.Version, Session: saved.Key.ID, Entry: "proxy"}, &lease); err != nil || lease.Endpoint.ID != saved.Endpoint {
						t.Fatal("recovered session changed Agent affinity", err)
					}
					client.Release(lease.Lease)()
					sessions++
				}
				want := 3
				if point == "post-write" {
					want = 4
				}
				if sessions != want || calls.Load() != count || count != int64(want) {
					t.Fatal("recovery lost a post-commit session or replayed upstream", sessions, calls.Load(), count)
				}
				if response := processRequest(n, "/agents/test/ok", "source-session"); response.Code != 200 || calls.Load() != count+1 {
					t.Fatal("recovered business request failed or executed twice", response.Code, response.Body.String())
				}
			})
		}
	}
}
