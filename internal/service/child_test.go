package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	p "aiagent-gateway/internal/releaseprotocol"
)

// A real child executable, with controllable identity and inflight count. It
// speaks the worker handshake without opening a gateway store or an Agent.
func TestMain(m *testing.M) {
	if os.Getenv("GATEWAY_CHILD_TEST") == "1" && len(os.Args) > 1 && os.Args[1] == "worker" {
		fs := flag.NewFlagSet("child", flag.ExitOnError)
		config := fs.String("config", "", "")
		slot := fs.String("slot", "", "")
		release := fs.String("release", "", "")
		incarnation := fs.String("incarnation", "", "")
		parentFD := fs.Int("parent-fd", -1, "")
		listen := fs.String("listen", "", "")
		admin := fs.String("admin", "", "")
		_ = fs.Parse(os.Args[2:])
		parent := os.NewFile(uintptr(*parentFD), "parent")
		go func() { var b [1]byte; _, _ = parent.Read(b[:]); os.Exit(1) }()
		data, err := net.Listen("tcp", *listen)
		if err != nil {
			os.Exit(2)
		}
		defer data.Close()
		err = http.ListenAndServe(*admin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer child-test" {
				w.WriteHeader(401)
				return
			}
			id := p.Identity{PID: os.Getpid(), Protocol: p.Version, StateProtocol: 1, Release: *release,
				Instance: *slot, Incarnation: *incarnation, Ready: true}
			mode, _ := os.ReadFile(*config)
			switch string(mode) {
			case "busy":
				id.Inflight = 1
			case "wrong-pid":
				id.PID++
			}
			_ = json.NewEncoder(w).Encode(id)
		}))
		if err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func childFixture(t *testing.T) (*ChildProcessDriver, string, string, *int64, *[]string) {
	t.Helper()
	t.Setenv("GATEWAY_CHILD_TEST", "1")
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary := read(t, exe)
	digest := sha256.Sum256(binary)
	root := filepath.Join(dir, "releases")
	artifact := filepath.Join(root, hex.EncodeToString(digest[:]), "gateway")
	if err := atomicWrite(artifact, binary, 0755); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "mode")
	var leases int64
	var retired []string
	d := NewChildProcessDriver(ChildOptions{ConfigPath: config, ReleasesDir: root, Token: "child-test",
		OwnerUID: uint32(os.Geteuid()), Freeze: func(bool) {}, Count: func(string) int64 { return leases },
		Retire: func(worker string) { retired = append(retired, worker) }})
	t.Cleanup(d.Close)
	return d, artifact, config, &leases, &retired
}

func TestChildLifecycleAndDrain(t *testing.T) {
	d, artifact, config, leases, retired := childFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, err := d.Start(ctx, artifact, "a")
	if err != nil {
		t.Fatal(err)
	}
	again, err := d.Start(ctx, artifact, "a")
	if err != nil || again != a {
		t.Fatalf("non-idempotent start: %v", err)
	}
	b, err := d.Start(ctx, artifact, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Start(ctx, artifact, "c"); err == nil {
		t.Fatal("accepted third slot")
	}
	if err := os.WriteFile(config, []byte("busy"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(ctx, a); err == nil {
		t.Fatal("stopped active execution")
	}
	if err := os.WriteFile(config, nil, 0600); err != nil {
		t.Fatal(err)
	}
	*leases = 1
	if err := d.Stop(ctx, a); err == nil {
		t.Fatal("stopped outstanding state lease")
	}
	*leases = 0
	forged := a
	forged.Instance = "b"
	if err := d.Stop(ctx, forged); err == nil || len(*retired) != 0 {
		t.Fatal("retired mismatched target")
	}
	if err := d.Retire(ctx, a); err == nil {
		t.Fatal("retired live worker")
	}
	childA := d.children["a"]
	if err := d.Stop(ctx, a); err != nil {
		t.Fatal(err)
	}
	select {
	case <-childA.done:
	default:
		t.Fatal("child not reaped before Stop returned")
	}
	if err := d.Stop(ctx, a); err != nil {
		t.Fatal(err)
	}
	childB := d.children["b"]
	if err := childB.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-childB.done
	restarted, err := d.Start(ctx, artifact, "b")
	if err != nil || restarted.Incarnation == b.Incarnation {
		t.Fatalf("child recovery: %v", err)
	}
	if err := d.Stop(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Identity(ctx, restarted); err != nil {
		t.Fatal("stale stop killed replacement", err)
	}
	last := d.children["b"]
	d.Close()
	select {
	case <-last.done:
	default:
		t.Fatal("Close left live child")
	}
}

func TestChildRejectsRacedIdentityAndCleansFailedStart(t *testing.T) {
	d, artifact, config, _, retired := childFixture(t)
	if err := os.WriteFile(config, []byte("wrong-pid"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := d.Start(ctx, artifact, "a"); err == nil {
		t.Fatal("accepted wrong child PID")
	}
	if len(d.children) != 0 || len(*retired) != 1 {
		t.Fatal("failed start leaked child or leases")
	}
}

func TestValidateArtifactRejectsUntrustedPaths(t *testing.T) {
	d, artifact, _, _, _ := childFixture(t)
	if err := ValidateArtifact(d.opts.ReleasesDir, artifact, d.opts.OwnerUID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(artifact, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifact(d.opts.ReleasesDir, artifact, d.opts.OwnerUID); err == nil {
		t.Fatal("accepted writable artifact")
	}
	if err := os.Chmod(artifact, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("changed"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifact(d.opts.ReleasesDir, artifact, d.opts.OwnerUID); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("accepted changed content: %v", err)
	}
	if err := os.Remove(artifact); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/bin/sh", artifact); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifact(d.opts.ReleasesDir, artifact, d.opts.OwnerUID); err == nil {
		t.Fatal("accepted executable symlink")
	}
}
