// Package process provides listener and service lifecycle handoff primitives.
package process

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
)

// Socket retains a listening FD even when public Accept is paused. Established
// connections belong to their HTTP servers and are drained separately.
type Socket struct {
	mu      sync.Mutex
	file    *os.File
	active  net.Listener
	address net.Addr
}

// Listen binds a socket without beginning Accept.
func Listen(network, address string) (*Socket, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	if u, ok := l.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	defer l.Close()
	f, err := l.(interface{ File() (*os.File, error) }).File()
	if err != nil {
		return nil, err
	}
	return &Socket{file: f, address: l.Addr()}, nil
}

// Inherit copies an explicitly supplied listening FD. The caller still owns f.
// Non-listening connected sockets must never enter the public listener set.
func Inherit(f *os.File) (*Socket, error) {
	if f == nil {
		return nil, fmt.Errorf("missing inherited listener")
	}
	if err := validateListener(int(f.Fd())); err != nil {
		return nil, err
	}
	l, err := net.FileListener(f)
	if err != nil {
		return nil, err
	}
	if u, ok := l.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	defer l.Close()
	copy, err := l.(interface{ File() (*os.File, error) }).File()
	if err != nil {
		return nil, err
	}
	return &Socket{file: copy, address: l.Addr()}, nil
}

func (s *Socket) Addr() net.Addr { return s.address }

// File returns a close-on-exec copy for a candidate's ExtraFiles whitelist.
func (s *Socket) File() (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil, net.ErrClosed
	}
	// Serialize dup + CLOEXEC with Go's process creation so a simultaneous
	// exec cannot accidentally inherit an unlisted listener.
	syscall.ForkLock.RLock()
	fd, err := syscall.Dup(int(s.file.Fd()))
	if err != nil {
		syscall.ForkLock.RUnlock()
		return nil, err
	}
	syscall.CloseOnExec(fd)
	syscall.ForkLock.RUnlock()
	return os.NewFile(uintptr(fd), s.file.Name()), nil
}

func (s *Socket) Activate() (net.Listener, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil, net.ErrClosed
	}
	if s.active != nil {
		return nil, fmt.Errorf("listener already accepting")
	}
	l, err := net.FileListener(s.file)
	if err != nil {
		return nil, err
	}
	if u, ok := l.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	s.active = l
	return l, nil
}

func (s *Socket) Pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return nil
	}
	err := s.active.Close()
	s.active = nil
	return err
}

// Close never unlinks a Unix socket another incarnation may still be using.
func (s *Socket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.active != nil {
		err = s.active.Close()
		s.active = nil
	}
	if s.file != nil {
		err = errors.Join(err, s.file.Close())
		s.file = nil
	}
	return err
}
