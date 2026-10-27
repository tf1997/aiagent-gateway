package process

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

func TestCompactResultNegotiationRequiresValidatedReply(t *testing.T) {
	for _, name := range []string{"supported", "legacy", "mismatched sequence", "mismatched action", "wrong identity", "other error", "truncated reply", "closed", "timeout"} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			c := &CandidateProcess{channel: &peerChannel{conn: client}, cmd: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}}, target: p.Target{Release: "release", Incarnation: "incarnation", Instance: "a"}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer server.Close()
				m, err := readPeer(server)
				if err != nil {
					return
				}
				reply := peerMessage{Sequence: m.Sequence, Action: m.Action, Identity: p.Identity{PID: c.PID(), Release: c.target.Release, Incarnation: c.target.Incarnation, Instance: c.target.Instance, Protocol: p.Version, StateProtocol: 1}}
				switch name {
				case "legacy":
					reply.Error, reply.Identity = "UNKNOWN_CANDIDATE_COMMAND", p.Identity{}
				case "mismatched sequence":
					reply.Sequence++
					reply.Error = "UNKNOWN_CANDIDATE_COMMAND"
				case "mismatched action":
					reply.Action = "identity"
					reply.Error = "UNKNOWN_CANDIDATE_COMMAND"
				case "wrong identity":
					reply.Identity.PID++
				case "other error":
					reply.Error = "RESULT_NEGOTIATION_TOO_LATE"
				case "truncated reply":
					_, _ = server.Write([]byte{0, 0, 0, 50, '{'})
					return
				case "closed":
					return
				case "timeout":
					_, _ = io.Copy(io.Discard, server)
					return
				}
				_ = writePeer(server, reply)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			supported, err := c.negotiateCompactResults(ctx)
			<-done
			if name == "supported" {
				if !supported || err != nil {
					t.Fatal("new peer not enabled", supported, err)
				}
			} else if name == "legacy" {
				if supported || err != nil {
					t.Fatal("explicit legacy rejection not supported", supported, err)
				}
			} else if err == nil || supported {
				t.Fatal("bad reply authorized result representation", supported, err)
			}
		})
	}
}
