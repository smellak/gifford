//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package session

import "golang.org/x/sys/unix"

// ioctlGetTermios reads a terminal's attributes.
const ioctlGetTermios = unix.TIOCGETA
