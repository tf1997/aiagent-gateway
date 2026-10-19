package process

import (
	"fmt"
	"syscall"
)

func validateListener(fd int) error {
	accept, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN)
	if err != nil || accept != 1 {
		return fmt.Errorf("inherited FD is not listening: %v", err)
	}
	return nil
}
