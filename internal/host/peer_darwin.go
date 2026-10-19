package host

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

func peerUID(conn net.Conn) (uint32, error) {
	u, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("not a unix connection")
	}
	raw, err := u.SyscallConn()
	if err != nil {
		return 0, err
	}
	// Darwin xucred: version, uid, group count, aligned group array.
	var cred struct {
		Version uint32
		UID     uint32
		Groups  int16
		Pad     uint16
		GIDs    [16]uint32
	}
	size := uint32(unsafe.Sizeof(cred))
	var sockErr syscall.Errno
	err = raw.Control(func(fd uintptr) {
		_, _, sockErr = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, 1, uintptr(unsafe.Pointer(&cred)), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil {
		return 0, err
	}
	if sockErr != 0 {
		return 0, sockErr
	}
	if cred.Version != 0 || size != uint32(unsafe.Sizeof(cred)) {
		return 0, fmt.Errorf("invalid peer credentials")
	}
	return cred.UID, nil
}
