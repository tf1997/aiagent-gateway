package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"testing"
)

func deltaFixture() ([]byte, []byte, ImageVersion, ImageVersion) {
	base := make([]byte, 2<<20)
	_, _ = rand.New(rand.NewSource(42)).Read(base)
	next := append([]byte("inserted configuration"), base[:1<<20]...)
	next = append(next, []byte("changed state")...)
	next = append(next, base[(1<<20)+100:]...)
	return base, next, VersionOf(base, "owner", 10), VersionOf(next, "owner", 25)
}

func TestDeltaReconstructsShiftedImageAndRejectsOldBatch(t *testing.T) {
	base, next, from, to := deltaFixture()
	raw, err := EncodeDelta(context.Background(), base, next, from, to, 256<<10, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) >= len(next)/4 {
		t.Fatal("unchanged shifted pages were transmitted again", len(raw), len(next))
	}
	got, version, err := ApplyDelta(context.Background(), base, from, raw, 256<<10, 4<<20)
	if err != nil || !bytes.Equal(got, next) || version != to {
		t.Fatal("delta changed the image", version, err)
	}
	if partial, _, err := ApplyDelta(context.Background(), got, to, raw, 256<<10, 4<<20); err == nil || partial != nil {
		t.Fatal("old batch applied to a newer view")
	}
	next[0] ^= 1
	if bytes.Equal(got, next) || VersionOf(base, from.Epoch, from.Revision) != from {
		t.Fatal("delta receiver retained mutable sender references")
	}
}

func TestDeltaRejectsCorruptionAndRevisionGaps(t *testing.T) {
	base, next, from, to := deltaFixture()
	raw, err := EncodeDelta(context.Background(), base, next, from, to, 256<<10, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*imageDelta)
	}{
		{"gap", func(d *imageDelta) { d.From.Revision++ }},
		{"epoch", func(d *imageDelta) { d.To.Epoch = "untrusted" }},
		{"regression", func(d *imageDelta) { d.To.Revision = 9 }},
		{"unrecorded_change", func(d *imageDelta) { d.To.Revision = d.From.Revision }},
		{"digest", func(d *imageDelta) { d.To.Digest = from.Digest }},
		{"capacity", func(d *imageDelta) { d.To.Bytes = 5 << 20 }},
		{"truncated", func(d *imageDelta) { d.Parts = d.Parts[:len(d.Parts)-1] }},
		{"copy_range", func(d *imageDelta) { d.Parts[0] = imagePart{Copy: &imageSpan{len(base), 1}} }},
		{"ambiguous", func(d *imageDelta) { d.Parts[0] = imagePart{Copy: &imageSpan{0, 1}, Data: []byte("x")} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d imageDelta
			if err := json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			tc.edit(&d)
			bad, _ := json.Marshal(d)
			if got, _, err := ApplyDelta(context.Background(), base, from, bad, 256<<10, 4<<20); err == nil || got != nil {
				t.Fatal("invalid delta published an image")
			}
		})
	}
	if _, _, err = ApplyDelta(context.Background(), base, from, append(raw, []byte(`{}`)...), 256<<10, 4<<20); err == nil {
		t.Fatal("trailing payload accepted")
	}
}

func TestDeltaCapacityCancellationAndUnchangedImage(t *testing.T) {
	base, next, from, to := deltaFixture()
	if _, err := EncodeDelta(context.Background(), base, next, from, to, 128, 4<<20); err == nil {
		t.Fatal("delta exceeded its budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := EncodeDelta(ctx, base, next, from, to, 256<<10, 4<<20); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, err := EncodeDelta(context.Background(), base, base, from, from, 256<<10, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, stamp, err := ApplyDelta(context.Background(), base, from, raw, 256<<10, 4<<20)
	if err != nil || stamp != from || !bytes.Equal(got, base) {
		t.Fatal("unchanged checkpoint failed", err)
	}
	if _, _, err = ApplyDelta(ctx, base, from, raw, 256<<10, 4<<20); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled application published a view", err)
	}
}
