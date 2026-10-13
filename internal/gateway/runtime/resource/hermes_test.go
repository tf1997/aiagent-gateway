package resource

import (
	"testing"

	"aiagent-gateway/internal/gateway/protocol"
)

func TestHermesObservationCapacityIsAtomic(t *testing.T) {
	d, _ := directoryTest()
	session := reserveTest(t, d, "alice", protocol.HermesSession, "atomic", "", "", "").Resource
	updateTest(t, d, session, protocol.ResourceLive)
	reservation := reserveTest(t, d, "alice", protocol.HermesRun, "", session.ID, protocol.HermesSession, "")
	d.mu.Lock()
	d.maxAliases = d.aliasCount
	d.mu.Unlock()
	if err := d.ObserveHermes(session, reservation.Resource, []string{"rotated-session"}, "rotated-session", "run-upstream", protocol.ResourceLive); err != protocol.ErrResourceCapacity {
		t.Fatal(err)
	}
	unchanged, _ := d.Lookup(session.Key())
	execution, _ := d.Lookup(reservation.Resource.Key())
	if unchanged.Upstream != session.Upstream || execution.Upstream != "" || d.PublicUpstream("alice", "route", protocol.HermesSession, session.EndpointID, "rotated-session") != "" {
		t.Fatal("partial ID mapping after capacity rejection")
	}
	d.mu.Lock()
	d.maxAliases++
	d.mu.Unlock()
	if err := d.ObserveHermes(session, reservation.Resource, nil, "rotated-session", "run-upstream", protocol.ResourceLive); err != nil {
		t.Fatal(err)
	}
}
