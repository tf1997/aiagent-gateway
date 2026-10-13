package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
)

func Exposes(r *Route, entry string) bool {
	for _, v := range r.Expose {
		if v == entry {
			return true
		}
	}
	return false
}

func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func Problem(w http.ResponseWriter, code int, msg string) {
	WriteJSON(w, code, map[string]string{"error": msg})
}

func SessionID(r *http.Request, route *Route) (string, error) {
	s := r.Header.Get(route.Session.Header)
	if len(s) > 256 || strings.IndexFunc(s, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("invalid session ID")
	}
	return s, nil
}

func StringField(m map[string]any, k string) string { s, _ := m[k].(string); return s }

func UpstreamURL(base, path, query string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	if u.RawQuery != "" && query != "" {
		u.RawQuery += "&" + query
	} else if query != "" {
		u.RawQuery = query
	}
	return u.String(), nil
}

func JSONObject(raw []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrResourceInvalid
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return nil, ErrResourceInvalid
	}
	return v, nil
}

func NoRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// Read timer cancels stalled upstream reads, including streams that never emit data.
type idleBody struct {
	io.ReadCloser
	timer  *time.Timer
	cancel context.CancelFunc
	idle   time.Duration
	mu     sync.Mutex
	closed bool
}

func NewIdleBody(b io.ReadCloser, c context.CancelFunc, d time.Duration) *idleBody {
	x := &idleBody{ReadCloser: b, cancel: c, idle: d}
	x.timer = time.AfterFunc(d, c)
	return x
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.mu.Lock()
		if !b.closed {
			b.timer.Reset(b.idle)
		}
		b.mu.Unlock()
	}
	return n, e
}

func (b *idleBody) Close() error {
	b.mu.Lock()
	b.closed = true
	b.timer.Stop()
	b.mu.Unlock()
	return b.ReadCloser.Close()
}

// SSE lines may end with CR, LF or CRLF. Bound the entire event, not just a line.
func SplitSSELine(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i, b := range data {
		if b == '\n' {
			return i + 1, data[:i], nil
		}
		if b == '\r' {
			if i+1 == len(data) && !atEOF {
				return 0, nil, nil
			}
			n := i + 1
			if n < len(data) && data[n] == '\n' {
				n++
			}
			return n, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
