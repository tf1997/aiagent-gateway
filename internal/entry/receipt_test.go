package entry

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
	"aiagent-gateway/internal/state"
)

func receiptFixture(t *testing.T) (*Entry, *state.Journal, string) {
	t.Helper()
	dir := t.TempDir()
	j, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	e, err := Open(j, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	if err := e.UseCompactResults(true); err != nil {
		t.Fatal(err)
	}
	stable := p.Target{Release: "a", Incarnation: "a", URL: "http://127.0.0.1:80"}
	candidate := p.Target{Release: "b", Incarnation: "b", URL: "http://127.0.0.1:81"}
	if err := e.Apply(0, p.Traffic{Revision: 1, Stable: stable, Candidate: &candidate, Percent: 100, Rollout: "r"}); err != nil {
		t.Fatal(err)
	}
	return e, j, dir
}

func finishReceipt(t *testing.T, e *Entry, id, ingress, verdict string) p.Result {
	t.Helper()
	_, r, err := e.startIngress("", "", ingress, id)
	if err != nil {
		t.Fatal(err)
	}
	r.Profile, r.Verdict, r.Status, r.Completed = "http", verdict, 200, time.Now()
	if err := e.Complete(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCompactReceiptsExceedLegacyCapacityAndSurviveRestart(t *testing.T) {
	e, j, dir := receiptFixture(t)
	var last p.Result
	for i := 0; i < 8200; i++ {
		last = finishReceipt(t, e, fmt.Sprint(i), "b", "success")
	}
	if stats := e.Stats(time.Time{}, "b"); stats.Lost || stats.Total != 8200 || stats.IngressCounts["http"].Success != 8200 || len(e.results) != 0 {
		t.Fatalf("compact evidence hit legacy limit: %+v", stats)
	}
	_, pending, err := e.startIngress("", "", "a", "interrupted")
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	recovered, err := Open(j, "token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	stats := recovered.Stats(time.Time{}, "b")
	if stats.Lost || stats.Total != 8201 || stats.Failures != 1 || stats.Active != 0 || stats.IngressCounts["http"].Success != 8200 {
		t.Fatalf("restart lost evidence or fabricated success: %+v", stats)
	}
	if err := recovered.Complete(context.Background(), last); err != nil {
		t.Fatal("durable retry rejected", err)
	}
	if _, _, err := recovered.startIngress("", "", "b", last.ID); err == nil {
		t.Fatal("completed ID reused")
	}
	if recovered.receipts[pending.ID].Verdict != "failure" {
		t.Fatal("interrupted request not failed")
	}
}

func TestCompactReceiptTransferStatisticsAndExactRetries(t *testing.T) {
	e, _, _ := receiptFixture(t)
	finishReceipt(t, e, "early", "a", "failure")
	since := time.Now()
	r := finishReceipt(t, e, "direct", "b", "success")
	finishReceipt(t, e, "forwarded", "a", "excluded")
	finishReceipt(t, e, "unknown", "b", "unknown")
	want := e.Stats(since, "b")
	if want.Total != 3 || want.Failures != 1 || want.Counts["http"] != (p.Counts{Success: 1, Excluded: 1, Unknown: 1}) || want.IngressCounts["http"] != (p.Counts{Success: 1, Unknown: 1}) {
		t.Fatalf("wrong window or ingress counts: %+v", want)
	}
	if err := e.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := e.Export()
	if err != nil {
		t.Fatal(err)
	}
	image, err := RenderStateRecords(StateRecords(v))
	if err != nil {
		t.Fatal(err)
	}
	j, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	before := j.Snapshot()
	imported, err := Import(j, "token", nil, image)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	if !reflect.DeepEqual(before, j.Snapshot()) {
		t.Fatal("import persisted before commit")
	}
	if got := imported.Stats(since, "b"); !reflect.DeepEqual(got, want) {
		t.Fatalf("stats changed across transfer: %+v", got)
	}
	v.Receipts[r.ID] = ResultReceipt{}
	image.Receipts[r.ID] = ResultReceipt{}
	for _, allocator := range []*Entry{e, imported} {
		roundtrip := r
		roundtrip.Started = r.Started.In(time.FixedZone("offset", 8*60*60))
		roundtrip.Completed = r.Completed.Round(0).UTC()
		if err := allocator.Complete(context.Background(), roundtrip); err != nil {
			t.Fatal("equivalent timestamp rejected", err)
		}
		for name, change := range map[string]func(*p.Result){
			"release":   func(r *p.Result) { r.Release += "x" },
			"instance":  func(r *p.Result) { r.Instance += "x" },
			"ingress":   func(r *p.Result) { r.Ingress = "a" },
			"revision":  func(r *p.Result) { r.RoutingRevision++ },
			"reason":    func(r *p.Result) { r.Reason = "changed" },
			"status":    func(r *p.Result) { r.Status = 500 },
			"completed": func(r *p.Result) { r.Completed = r.Completed.Add(time.Nanosecond) },
		} {
			changed := r
			change(&changed)
			if err := allocator.Complete(context.Background(), changed); err == nil {
				t.Fatal("conflicting compact retry accepted", name)
			}
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := imported.Complete(context.Background(), r); err == nil {
		t.Fatal("retry acknowledged after storage failure")
	}
}

func TestCompactReceiptLimitAndLegacyReset(t *testing.T) {
	e, _, _ := receiptFixture(t)
	r := finishReceipt(t, e, "first", "b", "success")
	for i := 1; i < maxResultReceipts; i++ {
		e.receipts[fmt.Sprint(i)] = resultReceipt(r)
	}
	finishReceipt(t, e, "overflow", "b", "success")
	if !e.Stats(time.Time{}, "b").Lost || len(e.receipts) != maxResultReceipts {
		t.Fatal("overflow did not fail evidence closed")
	}
	if err := e.UseCompactResults(false); err == nil {
		t.Fatal("active rollout changed representation")
	}
	traffic := e.Traffic()
	traffic.Revision, traffic.Rollout, traffic.Candidate, traffic.Percent = 2, "", nil, 0
	if err := e.Apply(1, traffic); err != nil {
		t.Fatal(err)
	}
	if len(e.receipts) != 0 || e.lost {
		t.Fatal("rollout reset kept old receipts")
	}
	if err := e.UseCompactResults(false); err != nil {
		t.Fatal(err)
	}
	traffic.Revision, traffic.Rollout = 3, "legacy"
	if err := e.Apply(2, traffic); err != nil {
		t.Fatal(err)
	}
	finishReceipt(t, e, "legacy-result", "a", "success")
	if len(e.results) != 1 || len(e.receipts) != 0 {
		t.Fatal("legacy peer received compact representation")
	}
	for key := range e.j.Snapshot() {
		if key == "entry/compact" || key == "entry/receipt/first" {
			t.Fatal("legacy reset retained compact state")
		}
	}
}

func TestCompactReceiptInvalidImages(t *testing.T) {
	e, j, _ := receiptFixture(t)
	r := finishReceipt(t, e, "done", "b", "success")
	if err := e.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := e.Export()
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Transfer){
		"unmarked":             func(v *Transfer) { v.Compact = false },
		"mixed":                func(v *Transfer) { v.Results[r.ID] = r },
		"digest":               func(v *Transfer) { receipt := v.Receipts[r.ID]; receipt.Digest = "bad"; v.Receipts[r.ID] = receipt },
		"empty id":             func(v *Transfer) { v.Receipts[""] = v.Receipts[r.ID] },
		"completed and active": func(v *Transfer) { pending := r; pending.Completed = time.Time{}; v.Active[r.ID] = pending },
	} {
		t.Run(name, func(t *testing.T) {
			bad := v.clone()
			change(&bad)
			before := j.Snapshot()
			if _, err := Import(j, "token", nil, bad); err == nil {
				t.Fatal("invalid import accepted")
			}
			if _, err := RenderStateRecords(StateRecords(bad)); err == nil {
				t.Fatal("invalid operation image accepted")
			}
			if !reflect.DeepEqual(before, j.Snapshot()) {
				t.Fatal("invalid import changed durable state")
			}
		})
	}
}

func TestCompactReceiptRejectsUnencodableResultWithoutCompleting(t *testing.T) {
	e, j, _ := receiptFixture(t)
	_, r, err := e.startIngress("", "", "b", "invalid-time")
	if err != nil {
		t.Fatal(err)
	}
	r.Profile, r.Verdict = "http", "success"
	r.Completed = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	before := j.Snapshot()
	if err := e.Complete(context.Background(), r); err == nil {
		t.Fatal("unencodable result was acknowledged")
	}
	if e.Active() != 1 || len(e.receipts) != 0 || !reflect.DeepEqual(before, j.Snapshot()) {
		t.Fatal("rejected result changed the allocation or durable state")
	}
	r.Completed = time.Now()
	if err := e.Complete(context.Background(), r); err != nil {
		t.Fatal("valid completion could not follow rejected result", err)
	}
}
