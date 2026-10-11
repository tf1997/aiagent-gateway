package gateway

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Reconcile a bounded, rotating set independently of health checks. Temporary
// failures retain ownership; a missing upstream task is not proof of completion.
func (g *Gateway) reconcileLoop(ctx context.Context) {
	defer g.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refs := g.resources.list("", "", "", "")
			if len(refs) == 0 {
				continue
			}
			for n := 0; n < min(16, len(refs)); n++ {
				ref := refs[offset%len(refs)]
				offset++
				if ref.ActiveRefs > 0 || ref.State == resourceTerminal || ref.Upstream == "" || (ref.Kind == hermesSession && ref.State != resourceUnknown) || ref.Kind == a2aContext {
					continue
				}
				probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				g.reconcileResource(probeCtx, ref)
				cancel()
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}
func (g *Gateway) reconcileResource(ctx context.Context, ref resource) {
	var rt *Route
	for _, r := range g.current.Load().Routes {
		if r.ID == ref.RouteID {
			rt = r
			break
		}
	}
	if rt == nil || !rt.Enabled {
		return
	}
	// Revoking access does not cancel upstream work; continue observing known
	// resources using the stored owner, without re-enabling consumer access.
	caller := Caller{ID: ref.Owner, reconcile: true}
	if ref.Kind == a2aTask {
		value, err := g.fetchA2ATask(ctx, caller, rt, ref, map[string]any{})
		if err != nil || value == nil {
			g.resources.update(ref, resourceUnknown)
		}
		return
	}
	if rt.AgentType != "hermes" {
		return
	}
	g.discoverHermes(ctx, rt)
	l, current, err := g.acquireOwnedResource(caller.ID, rt, ref.Kind, ref.ID, true)
	if err != nil {
		return
	}
	defer l.release()
	if current.State == resourceTerminal {
		return
	}
	if current.Kind == hermesSession {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://gateway.invalid", nil)
		g.readHermesSession(req, l, caller, current)
		return
	}
	session, err := g.resources.lookup(current.parent())
	if err != nil {
		return
	}
	path := "/v1/runs/" + current.Upstream
	action := "run.get"
	if current.Kind == hermesResponse {
		path = "/v1/responses/" + current.Upstream
		action = "response"
	}
	target, _ := hermesURL(l.iface.URL, path, "")
	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		return
	}
	g.upstreamHeaders(req.Header, caller, l.iface)
	client := &http.Client{Transport: g.transport(3000), CheckRedirect: noRedirect}
	res, err := client.Do(req)
	if err != nil {
		g.resources.update(ref, resourceUnknown)
		return
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || res.StatusCode != 200 || len(raw) > 2<<20 {
		g.resources.update(ref, resourceUnknown)
		return
	}
	value, err := hermesJSON(raw)
	h := hermesRequest{g: g, caller: caller, rt: rt, session: session, execution: current, op: hermesOperation{action: action}}
	if err == nil {
		err = h.observe(value, "reconcile", res.Header)
	}
	if err != nil {
		g.resources.update(ref, resourceUnknown)
	}
}
