package process

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Notifier uses the service manager's NOTIFY_SOCKET, not application config.
type Notifier struct{ Socket string }

func (n Notifier) connect() (*net.UnixConn, error) {
	if n.Socket == "" {
		return nil, fmt.Errorf("NOTIFY_SOCKET is not set")
	}
	return net.DialUnix("unixgram", nil, &net.UnixAddr{Name: n.Socket, Net: "unixgram"})
}

// MainPID must be sent by the current main process. It does not claim that the
// manager accepted the notification: the new process verifies MainPID itself.
func (n Notifier) MainPID(pid int) error {
	if pid < 2 {
		return fmt.Errorf("invalid main PID")
	}
	c, err := n.connect()
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.Write([]byte("MAINPID=" + strconv.Itoa(pid)))
	return err
}

// Ready waits for systemd to process READY=1 using BARRIER=1 and an SCM_RIGHTS
// pipe. Merely sending the datagram is not enough to retire the previous owner.
func (n Notifier) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, err := n.connect()
	if err != nil {
		return err
	}
	defer c.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.SetWriteDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if _, err = c.Write([]byte("READY=1")); err != nil {
		return err
	}
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	defer r.Close()
	defer w.Close()
	stopRead := context.AfterFunc(ctx, func() { r.Close() })
	defer stopRead()
	err = sendRights(c, []byte("BARRIER=1"), syscall.UnixRights(int(w.Fd())))
	if err != nil {
		return err
	}
	w.Close()
	var b [1]byte
	_, err = r.Read(b[:])
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != io.EOF {
		return fmt.Errorf("invalid notification barrier: %v", err)
	}
	return nil
}

// Go's WriteMsgUnix rejects a connected unixgram socket. Use RawConn.Write so
// sendmsg can attach the descriptor while retaining deadlines and netpoll.
func sendRights(c *net.UnixConn, body, rights []byte) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var sendErr error
	err = raw.Write(func(fd uintptr) bool {
		for {
			n, e := syscall.SendmsgN(int(fd), body, rights, nil, 0)
			if e == syscall.EINTR {
				continue
			}
			if e == syscall.EAGAIN || e == syscall.EWOULDBLOCK {
				return false
			}
			sendErr = e
			if e == nil && n != len(body) {
				sendErr = io.ErrShortWrite
			}
			return true
		}
	})
	if err != nil {
		return err
	}
	return sendErr
}

func MainPID(ctx context.Context, unit string) (int, error) {
	if unit == "" || strings.HasPrefix(unit, "-") || strings.ContainsAny(unit, "/\x00\r\n") {
		return 0, fmt.Errorf("invalid service name")
	}
	raw, err := exec.CommandContext(ctx, "systemctl", "show", "--property=MainPID", "--value", unit).Output()
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid < 0 {
		return 0, fmt.Errorf("invalid systemd MainPID")
	}
	return pid, nil
}

func WaitMainPID(ctx context.Context, unit string, pid int) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		actual, err := MainPID(ctx, unit)
		if err == nil && actual == pid {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("systemd did not adopt PID %d: %w", pid, ctx.Err())
		case <-ticker.C:
		}
	}
}
