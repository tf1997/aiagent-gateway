package process

import (
	"errors"
	"fmt"
	"syscall"
)

// Darwin does not expose SO_ACCEPTCONN via getsockopt. Local development can
// validate the socket kind and reject connected streams; production Linux
// additionally requires the kernel's explicit listening flag.
func validateListener(fd int) error {
	kind, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_TYPE)
	if err != nil || (kind != syscall.SOCK_STREAM && kind != syscall.SOCK_SEQPACKET) {
		return fmt.Errorf("inherited FD is not a stream socket: %v", err)
	}
	if _, err = syscall.Getpeername(fd); !errors.Is(err, syscall.ENOTCONN) {
		return fmt.Errorf("inherited FD is connected: %v", err)
	}
	return nil
}
