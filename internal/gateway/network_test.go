package gateway

// The sandbox fallback exercises net/http and cancellation over net.Pipe.
// Without GATEWAY_TEST_IN_MEMORY it uses real loopback TCP listeners.
import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var memRegistry sync.Map
var memSequence atomic.Int64

type testServer struct {
	URL   string
	close func()
}

func (s *testServer) Close() { s.close() }

type pipeListener struct {
	incoming chan net.Conn
	done     chan struct{}
	once     sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return pipeAddr("memory") }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }
func newTestServer(h http.Handler) *testServer {
	if os.Getenv("GATEWAY_TEST_IN_MEMORY") != "true" {
		s := httptest.NewServer(h)
		return &testServer{s.URL, s.Close}
	}
	host := fmt.Sprintf("test-%d.invalid:80", memSequence.Add(1))
	l := &pipeListener{incoming: make(chan net.Conn), done: make(chan struct{})}
	memRegistry.Store(host, l)
	srv := &http.Server{Handler: h}
	go srv.Serve(l)
	return &testServer{"http://" + host, func() { memRegistry.Delete(host); srv.Close() }}
}
func testDial(ctx context.Context, network, address string) (net.Conn, error) {
	if v, ok := memRegistry.Load(address); ok {
		l := v.(*pipeListener)
		a, b := net.Pipe()
		select {
		case l.incoming <- b:
			return a, nil
		case <-ctx.Done():
			a.Close()
			b.Close()
			return nil, ctx.Err()
		case <-l.done:
			a.Close()
			b.Close()
			return nil, net.ErrClosed
		}
	}
	return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
}
func testClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: testDial}, Timeout: 3 * time.Second}
}
