package shared

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const DeltaFormat = "gateway-image-delta-1"

// ImageVersion binds an isolated image to an epoch and a consistent revision.
// Each batch must start at the receiver's exact acknowledged version.
type ImageVersion struct {
	Epoch    string `json:"epoch"`
	Revision uint64 `json:"revision"`
	Digest   string `json:"sha256"`
	Bytes    int    `json:"bytes"`
}

func VersionOf(raw []byte, epoch string, revision uint64) ImageVersion {
	sum := sha256.Sum256(raw)
	return ImageVersion{epoch, revision, hex.EncodeToString(sum[:]), len(raw)}
}

type imageSpan struct {
	Offset int `json:"offset"`
	Bytes  int `json:"bytes"`
}

type imagePart struct {
	Copy *imageSpan `json:"copy,omitempty"`
	Data []byte     `json:"data,omitempty"`
}

// imageDelta coalesces changes between two consistent captures. It reconstructs
// exact bytes; applying it cannot execute business operations or timers.
type imageDelta struct {
	Format string       `json:"format"`
	From   ImageVersion `json:"from"`
	To     ImageVersion `json:"to"`
	Parts  []imagePart  `json:"parts"`
}

// Content-defined boundaries resynchronize after insertions. A shifted JSON
// field must not turn every subsequent unchanged page into literal data.
var deltaGear = func() (out [256]uint64) {
	for i := range out {
		x := uint64(i) + 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		out[i] = x ^ (x >> 31)
	}
	return
}()

func imageChunks(ctx context.Context, raw []byte, visit func(int, []byte) error) error {
	start := 0
	var hash uint64
	for i, ch := range raw {
		hash = (hash << 1) + deltaGear[ch]
		size := i + 1 - start
		if i+1 != len(raw) && (size < 4096 || (size < 64<<10 && hash&0x3fff != 0)) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(start, raw[start:i+1]); err != nil {
			return err
		}
		start, hash = i+1, 0
	}
	return ctx.Err()
}

func validImageVersion(v ImageVersion, maxBytes int) bool {
	digest, err := hex.DecodeString(v.Digest)
	return v.Epoch != "" && len(v.Epoch) <= 128 && v.Bytes > 0 && v.Bytes <= maxBytes && err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == v.Digest
}

// EncodeDelta uses only acknowledged base bytes and literal replacements.
// Both its serialized bytes and reconstructed image have explicit bounds.
// Capacity failures require a fresh preparation; they never silently fall
// back to a larger full-image transfer during the final ownership barrier.
func EncodeDelta(ctx context.Context, base, next []byte, from, to ImageVersion, maxDeltaBytes, maxImageBytes int) ([]byte, error) {
	if maxDeltaBytes <= 0 || !validImageVersion(from, maxImageBytes) || !validImageVersion(to, maxImageBytes) || from.Epoch != to.Epoch || to.Revision < from.Revision || (to.Revision == from.Revision && to != from) || VersionOf(base, from.Epoch, from.Revision) != from || VersionOf(next, to.Epoch, to.Revision) != to {
		return nil, fmt.Errorf("INVALID_DELTA_VERSIONS")
	}
	index := map[[sha256.Size]byte]imageSpan{}
	if err := imageChunks(ctx, base, func(offset int, chunk []byte) error {
		key := sha256.Sum256(chunk)
		if _, ok := index[key]; !ok {
			index[key] = imageSpan{offset, len(chunk)}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	d := imageDelta{Format: DeltaFormat, From: from, To: to, Parts: []imagePart{}}
	header, _ := json.Marshal(d)
	size := len(header)
	if size > maxDeltaBytes {
		return nil, fmt.Errorf("DELTA_CAPACITY")
	}
	if err := imageChunks(ctx, next, func(_ int, chunk []byte) error {
		part := imagePart{}
		if span, ok := index[sha256.Sum256(chunk)]; ok && bytes.Equal(base[span.Offset:span.Offset+span.Bytes], chunk) {
			part.Copy = &span
		} else {
			part.Data = chunk
		}
		encoded, err := json.Marshal(part)
		if err != nil {
			return err
		}
		needed := len(encoded)
		if len(d.Parts) != 0 {
			needed++ // array separator
		}
		if needed > maxDeltaBytes-size {
			return fmt.Errorf("DELTA_CAPACITY")
		}
		size += needed
		d.Parts = append(d.Parts, part)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxDeltaBytes {
		return nil, fmt.Errorf("DELTA_CAPACITY")
	}
	return raw, nil
}

// ApplyDelta publishes no partial image. Wrong bases, epochs, revision gaps,
// copy ranges, lengths, malformed payloads and digest mismatches all fail.
func ApplyDelta(ctx context.Context, base []byte, current ImageVersion, raw []byte, maxDeltaBytes, maxImageBytes int) ([]byte, ImageVersion, error) {
	fail := func() ([]byte, ImageVersion, error) { return nil, ImageVersion{}, fmt.Errorf("INVALID_IMAGE_DELTA") }
	if len(raw) == 0 || len(raw) > maxDeltaBytes || !validImageVersion(current, maxImageBytes) || VersionOf(base, current.Epoch, current.Revision) != current {
		return fail()
	}
	var d imageDelta
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil || decoder.Decode(new(any)) != io.EOF || d.Format != DeltaFormat || d.From != current || !validImageVersion(d.To, maxImageBytes) || d.To.Epoch != current.Epoch || d.To.Revision < current.Revision || (d.To.Revision == current.Revision && d.To != current) {
		return fail()
	}
	total := 0
	for _, part := range d.Parts {
		size := len(part.Data)
		if part.Copy != nil {
			span := *part.Copy
			if len(part.Data) != 0 || span.Offset < 0 || span.Offset > len(base) || span.Bytes <= 0 || span.Bytes > len(base)-span.Offset {
				return fail()
			}
			size = span.Bytes
		}
		if size == 0 || size > d.To.Bytes-total {
			return fail()
		}
		total += size
	}
	if total != d.To.Bytes {
		return fail()
	}
	if err := ctx.Err(); err != nil {
		return nil, ImageVersion{}, err
	}
	next := make([]byte, 0, total)
	for _, part := range d.Parts {
		if err := ctx.Err(); err != nil {
			return nil, ImageVersion{}, err
		}
		if part.Copy != nil {
			next = append(next, base[part.Copy.Offset:part.Copy.Offset+part.Copy.Bytes]...)
		} else {
			next = append(next, part.Data...)
		}
	}
	if VersionOf(next, d.To.Epoch, d.To.Revision) != d.To {
		return fail()
	}
	return next, d.To, nil
}
