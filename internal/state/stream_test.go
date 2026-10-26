package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func operationBase() Cursor {
	return Cursor{Epoch: "owner", Handoff: "handoff", Revision: 17, Digest: strings.Repeat("0", 64)}
}

func TestJournalOperationStreamDurabilityAndCompaction(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var recorder Recorder
	base := operationBase()
	if err := recorder.Start(base, map[string]json.RawMessage{}, 8192); err != nil {
		t.Fatal(err)
	}
	j.SetRecorder(&recorder, "release/")
	j.commits = 1023
	if err := j.Apply(map[string]json.RawMessage{"report": json.RawMessage(`1`)}, nil); err != nil {
		t.Fatal(err)
	}
	records, at, err := recorder.Records()
	if err != nil || string(records["release/report"]) != "1" || at.Revision != base.Revision+1 || j.commits != 0 {
		t.Fatal("compaction reset or duplicated the operation sequence", at, err)
	}
	j.f = &failingJournalFile{journalFile: j.f, sync: func() error { return syscall.EIO }}
	if err := j.Apply(map[string]json.RawMessage{"report": json.RawMessage(`2`)}, nil); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	after, cursor, err := recorder.Records()
	if err != nil || cursor != at || !reflect.DeepEqual(records, after) {
		t.Fatal("unconfirmed storage write entered the operation stream", cursor, err)
	}
}

func TestJournalWriteSurvivesOperationOverflow(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var recorder Recorder
	if err := recorder.Start(operationBase(), map[string]json.RawMessage{}, 1); err != nil {
		t.Fatal(err)
	}
	j.SetRecorder(&recorder, "release/")
	if err := j.Apply(map[string]json.RawMessage{"report": json.RawMessage(`1`)}, nil); err != nil || j.Healthy() != nil || recorder.Err() == nil {
		t.Fatal("replication overflow failed a committed business write", err)
	}
	if err := j.Apply(map[string]json.RawMessage{"report": json.RawMessage(`2`)}, nil); err != nil {
		t.Fatal("overflow stopped later business writes", err)
	}
	recovered, err := ReadSnapshot(dir)
	if err != nil || string(recovered["report"]) != "2" {
		t.Fatal("business writes were not durable", err)
	}
}

func TestOperationStreamAcknowledgesOnlyConfirmedPrefix(t *testing.T) {
	base := operationBase()
	initial := map[string]json.RawMessage{"keep": json.RawMessage(`1`), "remove": json.RawMessage(`2`)}
	var r Recorder
	if err := r.Start(base, initial, 8192); err != nil {
		t.Fatal(err)
	}
	initial["keep"][0] = '9'
	r.Patch(map[string]json.RawMessage{"new": json.RawMessage(`{"value":3}`)}, []string{"remove"})
	raw, first, err := r.Batch(base)
	if err != nil {
		t.Fatal(err)
	}
	mirror := map[string]json.RawMessage{"keep": json.RawMessage(`1`), "remove": json.RawMessage(`2`)}
	at, err := ApplyOperations(mirror, base, raw, 8192, 8192)
	if err != nil || at != first || string(mirror["keep"]) != "1" || mirror["remove"] != nil {
		t.Fatal(at, err, mirror)
	}
	// A newer operation during ACK must survive releasing the older prefix.
	r.Put("new", 4)
	r.Advance()
	if err := r.Acknowledge(first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Batch(base); err == nil {
		t.Fatal("old acknowledgement accepted")
	}
	raw, last, err := r.Batch(first)
	if err != nil {
		t.Fatal(err)
	}
	at, err = ApplyOperations(mirror, first, raw, 8192, 8192)
	if err != nil || at != last || string(mirror["new"]) != "4" {
		t.Fatal(at, err, mirror)
	}
	want, _, err := r.Records()
	if err != nil || !reflect.DeepEqual(mirror, want) {
		t.Fatal("mirror diverged", err)
	}
	want["keep"][0] = '8'
	detached, _, _ := r.Records()
	if string(detached["keep"]) != "1" {
		t.Fatal("mutable alias")
	}
	if err := r.Acknowledge(last); err != nil {
		t.Fatal(err)
	}
	empty, _, _ := r.Batch(last)
	var batch OperationBatch
	json.Unmarshal(empty, &batch)
	if len(batch.Events) != 0 {
		t.Fatal("acknowledged events retained")
	}
	r.Stop()
	if err := r.Acknowledge(last); err == nil {
		t.Fatal("inactive stream acknowledged")
	}
}

func TestOperationBatchFailureIsAtomic(t *testing.T) {
	for _, fault := range []string{"base", "epoch", "handoff", "digest", "gap", "delete-conflict", "unknown-field", "trailing", "batch-budget", "image-budget"} {
		t.Run(fault, func(t *testing.T) {
			base := operationBase()
			var r Recorder
			initial := map[string]json.RawMessage{"old": json.RawMessage(`1`)}
			if err := r.Start(base, initial, 8192); err != nil {
				t.Fatal(err)
			}
			r.Put("valid-first", 2)
			r.Delete("old")
			raw, _, err := r.Batch(base)
			if err != nil {
				t.Fatal(err)
			}
			var b OperationBatch
			json.Unmarshal(raw, &b)
			maxBatch, maxImage := 8192, 8192
			switch fault {
			case "base":
				b.From.Revision++
			case "epoch":
				b.To.Epoch = "another"
			case "handoff":
				b.To.Handoff = "another"
			case "digest":
				b.To.Digest = strings.Repeat("f", 64)
			case "gap":
				b.Events[1] = json.RawMessage(`{"revision":999}`)
			case "delete-conflict":
				b.Events[1] = json.RawMessage(fmt.Sprintf(`{"revision":%d,"put":{"x":1},"delete":["x"]}`, base.Revision+2))
			case "unknown-field":
				b.Events[1] = json.RawMessage(fmt.Sprintf(`{"revision":%d,"unknown":1}`, base.Revision+2))
			case "batch-budget":
				maxBatch = 1
			case "image-budget":
				maxImage = 1
			}
			raw, _ = json.Marshal(b)
			if fault == "trailing" {
				raw = append(raw, []byte(` {}`)...)
			}
			mirror := cloneRecords(initial)
			got, err := ApplyOperations(mirror, base, raw, maxBatch, maxImage)
			if err == nil || got != base || !reflect.DeepEqual(mirror, initial) {
				t.Fatal("invalid batch partially applied", got, err, mirror)
			}
		})
	}
}

func TestRecorderOverflowPoisonsOnlyPreparation(t *testing.T) {
	var r Recorder
	base := operationBase()
	if err := r.Start(base, map[string]json.RawMessage{}, 1200); err != nil {
		t.Fatal(err)
	}
	r.Put("too-large", strings.Repeat("x", 2000))
	if r.Err() == nil {
		t.Fatal("overflow ignored")
	}
	r.Put("later", 1)
	if _, _, err := r.Batch(base); err == nil {
		t.Fatal("poisoned prefix exported")
	}
	r.Stop()
	base.Handoff = "retry"
	if err := r.Start(base, map[string]json.RawMessage{}, 8192); err != nil {
		t.Fatal(err)
	}
	r.Put("healthy", true)
	if _, _, err := r.Batch(base); err != nil {
		t.Fatal("fresh preparation failed", err)
	}
}

func TestConcurrentOperationRecordingAndCatchup(t *testing.T) {
	var r Recorder
	at := operationBase()
	if err := r.Start(at, map[string]json.RawMessage{}, 8<<20); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for n := 0; n < 100; n++ {
				r.Put(fmt.Sprintf("worker/%d", i), n)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	mirror := map[string]json.RawMessage{}
	for {
		raw, next, err := r.Batch(at)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ApplyOperations(mirror, at, raw, 8<<20, 8<<20); err != nil {
			t.Fatal(err)
		}
		if err = r.Acknowledge(next); err != nil {
			t.Fatal(err)
		}
		at = next
		select {
		case <-done:
			if at != r.Cursor() {
				continue
			}
			want, _, err := r.Records()
			if err != nil || !reflect.DeepEqual(mirror, want) {
				t.Fatal("lost concurrent change", err)
			}
			for _, value := range mirror {
				if !bytes.Equal(value, []byte("99")) {
					t.Fatal("stale final value")
				}
			}
			return
		default:
		}
	}
}
