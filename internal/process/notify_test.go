package process

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadyWaitsForNotificationBarrier(t *testing.T) {
	dir, err := os.MkdirTemp("", "gateway-notify-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "notify.sock")
	l, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- (Notifier{Socket: path}).Ready(ctx) }()
	buf := make([]byte, 128)
	l.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _, err := l.ReadFromUnix(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "READY=1" {
		t.Fatalf("ready: %q %v", buf[:n], err)
	}
	oob := make([]byte, syscall.CmsgSpace(4))
	n, on, _, _, err := l.ReadMsgUnix(buf, oob)
	if err != nil {
		select {
		case sendErr := <-done:
			t.Fatalf("barrier read: %v; notifier: %v", err, sendErr)
		default:
			t.Fatal(err)
		}
	}
	if string(buf[:n]) != "BARRIER=1" {
		t.Fatalf("barrier: %q %v", buf[:n], err)
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:on])
	if err != nil || len(msgs) != 1 {
		t.Fatalf("rights: %v", err)
	}
	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil || len(fds) != 1 {
		t.Fatalf("descriptors: %v", err)
	}
	fd := fds[0]
	defer func() {
		if fd >= 0 {
			syscall.Close(fd)
		}
	}()
	select {
	case err := <-done:
		t.Fatalf("barrier did not wait: %v", err)
	default:
	}
	if err = syscall.Close(fd); err != nil {
		t.Fatal(err)
	}
	fd = -1
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReadyCancellationDoesNotHang(t *testing.T) {
	dir, err := os.MkdirTemp("", "gateway-notify-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "notify.sock")
	l, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err = (Notifier{Socket: path}).Ready(ctx); err == nil {
		t.Fatal("unsynchronized READY succeeded")
	}
}
