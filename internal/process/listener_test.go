package process

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func serve(t *testing.T, s *Socket, handler http.Handler) *http.Server {
	t.Helper()
	l, err := s.Activate()
	if err != nil {
		t.Fatal(err)
	}
	h := &http.Server{Handler: handler}
	go func() { _ = h.Serve(l) }()
	t.Cleanup(func() { h.Close() })
	return h
}

func TestSocketHandoffKeepsEstablishedStreamAndAllowsRollback(t *testing.T) {
	a, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	finish := make(chan struct{})
	defer close(finish)
	old := serve(t, a, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			fmt.Fprintln(w, "old-stream")
			http.NewResponseController(w).Flush()
			<-finish
			return
		}
		fmt.Fprint(w, "A")
	}))
	conn, err := net.DialTimeout("tcp", a.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprint(conn, "GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	stream := bufio.NewReader(response.Body)
	if line, _ := stream.ReadString('\n'); line != "old-stream\n" {
		t.Fatal("old stream not established")
	}
	f, err := a.File()
	if err != nil {
		t.Fatal(err)
	}
	b, err := Inherit(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = a.Pause(); err != nil {
		t.Fatal(err)
	}
	serve(t, b, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "B") }))
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	check := func(want string) {
		t.Helper()
		r, e := client.Get("http://" + a.Addr().String() + "/")
		if e != nil {
			t.Fatal(e)
		}
		raw, e := io.ReadAll(r.Body)
		r.Body.Close()
		if e != nil || string(raw) != want {
			t.Fatalf("response %q: %v", raw, e)
		}
	}
	check("B")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err = old.Shutdown(ctx); err == nil {
		t.Fatal("old stream disappeared during handoff")
	}
	if err = b.Pause(); err != nil {
		t.Fatal(err)
	}
	serve(t, a, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "A") }))
	check("A")
}

func TestUnixListenerSurvivesOldClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "gateway-socket-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "control.sock")
	a, err := Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	f, err := a.File()
	if err != nil {
		t.Fatal(err)
	}
	b, err := Inherit(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("old owner removed shared control socket", err)
	}
	l, err := b.Activate()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		c, e := l.Accept()
		if e == nil {
			c.Write([]byte("B"))
			c.Close()
		}
	}()
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(time.Second))
	raw, err := io.ReadAll(c)
	if err != nil || string(raw) != "B" {
		t.Fatalf("control socket unavailable: %q, %v", raw, err)
	}
}

func TestInheritRejectsNonListener(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "file")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = Inherit(f); err == nil {
		t.Fatal("accepted regular file as listener")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if _, err = Inherit(r); err == nil {
		t.Fatal("accepted pipe as listener")
	}
}
