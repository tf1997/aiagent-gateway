package host

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestPersistedTrafficCannotPassReadinessBeforeRecovery(t *testing.T) {
	var accepting atomic.Bool
	workerReady := false
	called := false
	h := dataHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }), &accepting,
		func(context.Context) bool { return workerReady })
	for _, tc := range []struct {
		path                   string
		accepting, workerReady bool
		status                 int
	}{
		{"/readyz", false, true, 503}, {"/agents/probe", false, true, 503},
		{"/readyz", true, false, 503}, {"/readyz", true, true, 200},
	} {
		accepting.Store(tc.accepting)
		workerReady, called = tc.workerReady, false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || (tc.status == 503 && called) {
			t.Fatalf("%+v: %d called=%v", tc, w.Code, called)
		}
	}
}

func TestControlRequiresInstallerPeerAndToken(t *testing.T) {
	called := false
	h := authorizePeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(401)
		}
	}), 0)
	for _, tc := range []struct {
		peer   *peer
		token  string
		status int
	}{{nil, "secret", 403}, {&peer{uid: 999}, "secret", 403}, {&peer{uid: 0}, "wrong", 401}, {&peer{uid: 0}, "secret", 200}} {
		called = false
		r := httptest.NewRequest("POST", "/control/v1/rollout", nil)
		if tc.peer != nil {
			r = r.WithContext(context.WithValue(r.Context(), peerKey{}, *tc.peer))
		}
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status || (tc.status == 403 && called) {
			t.Fatalf("control identity: %d called=%v", w.Code, called)
		}
	}
}

func TestUnixPeerIdentityAndSocketProtection(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "host-peer-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "control.sock")
	l, err := unixListener(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	uid, err := peerUID(accepted)
	if err != nil || uid != uint32(os.Geteuid()) {
		t.Fatalf("peer UID=%d: %v", uid, err)
	}
	if next, err := unixListener(path); err == nil {
		next.Close()
		t.Fatal("replaced live socket")
	}
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := unixListener(regular); err == nil {
		next.Close()
		t.Fatal("replaced regular file")
	}
}
