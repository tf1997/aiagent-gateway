package process

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"aiagent-gateway/internal/handoff"
)

type dropActionReply struct {
	net.Conn
	action string
	buffer bytes.Buffer
}

func (c *dropActionReply) Read(p []byte) (int, error) {
	if c.buffer.Len() == 0 {
		reply, err := readPeer(c.Conn)
		if err != nil {
			return 0, err
		}
		if reply.Action == c.action {
			return 0, io.ErrUnexpectedEOF
		}
		if err = writePeer(&c.buffer, reply); err != nil {
			return 0, err
		}
	}
	return c.buffer.Read(p)
}

func TestTransferHandoffReconcilesLostReplies(t *testing.T) {
	for _, action := range []string{"commit", "activate"} {
		t.Run(action, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
			defer upstream.Close()
			n, c, opts, address := candidateFixture(t, upstream.URL)
			c.channel.conn = &dropActionReply{Conn: c.channel.conn, action: action}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			result, err := n.TransferHandoff(ctx, c, "coordinated", "rollout", opts.MaxSnapshotBytes)
			if err != nil {
				t.Fatal(err)
			}
			if result.Source != nil || !result.Activated || result.Decision.Phase != handoff.Committed || n.Ready() {
				t.Fatal("uncertain acknowledgement changed authority", result)
			}
			repeated, err := n.ResolveHandoff(ctx, c, c.verified, opts.MaxSnapshotBytes)
			if err != nil || repeated != result {
				t.Fatal("repeat recovery changed the decision", repeated, err)
			}
			res, err := (&http.Client{Timeout: time.Second}).Get(address + "/readyz")
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatal("candidate did not activate", res.StatusCode)
			}
		})
	}
}

func TestTransferHandoffValidationFailureRetainsSource(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	c.channel.conn = &dropActionReply{Conn: c.channel.conn, action: "validate"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := n.TransferHandoff(ctx, c, "failed-validation", "rollout", opts.MaxSnapshotBytes)
	if err == nil || result.Source != n || result.Activated || result.Decision.Phase != handoff.Aborted || !n.Ready() {
		t.Fatal("validation failure did not retain original authority", result, err)
	}
	if res := processRequest(n, "/agents/test/ok", "source-after-abort"); res.Code != 200 {
		t.Fatal("source remained sealed", res.Code, res.Body.String())
	}
}

func TestResolveHandoffRestoresAndReusesLockedSource(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer upstream.Close()
	n, c, opts, _ := candidateFixture(t, upstream.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d, err := n.PrepareHandoff(ctx, "resolve", "rollout", handoff.Identity{Incarnation: c.target.Incarnation, Release: c.target.Release}, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateHandoff(ctx, n, d, opts.MaxSnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if err = n.YieldHandoff(d); err != nil {
		t.Fatal(err)
	}
	result, err := n.ResolveHandoff(ctx, c, d, opts.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	if result.Source == nil {
		t.Fatal("missing restored source")
	}
	defer result.Source.Close()
	if result.Decision.Phase != handoff.Aborted || result.Activated || !result.Source.Ready() {
		t.Fatal("source not restored", result)
	}
	repeated, err := result.Source.ResolveHandoff(ctx, c, d, opts.MaxSnapshotBytes)
	if err != nil || repeated != result {
		t.Fatal("repeat recovery did not retain the same locks", repeated, err)
	}
	if res := processRequest(result.Source, "/agents/test/ok", "restored"); res.Code != 200 {
		t.Fatal("source unavailable", res.Code, res.Body.String())
	}
}
