package hermes

import (
	"context"
	"io"
	"net/http"

	"aiagent-gateway/internal/gateway/protocol"
)

func (g *Handler) Reconcile(ctx context.Context, caller protocol.Caller, rt *protocol.Route, ref protocol.Resource) {
	g.DiscoverHermes(ctx, rt)
	l, current, err := g.AcquireOwnedResource(caller.ID, rt, ref.Kind, ref.ID, true)
	if err != nil {
		return
	}
	defer l.Release()
	if current.State == protocol.ResourceTerminal {
		return
	}
	if current.Kind == protocol.HermesSession {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://gateway.invalid", nil)
		g.readHermesSession(req, l, caller, current)
		return
	}
	session, err := g.Resources.Lookup(current.Parent())
	if err != nil {
		return
	}
	path := "/v1/runs/" + current.Upstream
	action := "run.get"
	if current.Kind == protocol.HermesResponse {
		path = "/v1/responses/" + current.Upstream
		action = "response"
	}
	target, _ := protocol.UpstreamURL(l.Interface.URL, path, "")
	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		return
	}
	g.UpstreamHeaders(req.Header, caller, l.Interface)
	client := &http.Client{Transport: g.Transport(3000), CheckRedirect: protocol.NoRedirect}
	res, err := client.Do(req)
	if err != nil {
		g.Resources.Update(ref, protocol.ResourceUnknown)
		return
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || res.StatusCode != 200 || len(raw) > 2<<20 {
		g.Resources.Update(ref, protocol.ResourceUnknown)
		return
	}
	value, err := protocol.JSONObject(raw)
	h := hermesRequest{g: g, caller: caller, rt: rt, session: session, execution: current, op: hermesOperation{action: action}}
	if err == nil {
		err = h.observe(value, "reconcile", res.Header)
	}
	if err != nil {
		g.Resources.Update(ref, protocol.ResourceUnknown)
	}
}
