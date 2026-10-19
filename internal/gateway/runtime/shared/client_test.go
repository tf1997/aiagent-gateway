package shared

import (
	"context"
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
)

func TestLocalStateValuesAreDetachedAndRetriesUseSemanticIdentity(t *testing.T) {
	config := &protocol.Config{Revision: 1, Receipts: map[string]protocol.Receipt{}}
	executions := 0
	s := NewServer("token", Backend{Execute: func(q Request, _ *Leases) Response {
		if q.Action == "config" {
			return Response{Config: config}
		}
		executions++
		return Response{Config: config}
	}})
	c := NewLocalClient(s, "local")
	defer c.Close()
	var out Response
	// The first request learns the epoch. A retry with that epoch must still
	// hit its original receipt, despite different transport metadata.
	q := Request{ID: "same-operation"}
	if err := c.Call("mutation", q, &out); err != nil {
		t.Fatal(err)
	}
	out.Config.Revision = 99
	out.Config.Receipts["injected"] = protocol.Receipt{}
	if err := c.Call("mutation", q, &out); err != nil {
		t.Fatal(err)
	}
	if executions != 1 || out.Config.Revision != 1 || len(out.Config.Receipts) != 0 || config.Revision != 1 || len(config.Receipts) != 0 {
		t.Fatal("local values alias owner or cached receipt")
	}
	if err := c.Call("different-mutation", q, nil); err == nil {
		t.Fatal("conflicting operation ID was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.CallContext(ctx, "mutation", Request{ID: "cancelled"}, nil); err == nil {
		t.Fatal("cancelled call was admitted")
	}
	if executions != 1 {
		t.Fatal("cancelled or conflicting request executed")
	}
}
