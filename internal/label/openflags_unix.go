//go:build unix

package label

import "syscall"

// noFollow and noBlock keep an open from following a symlink or waiting
// on a FIFO.
const (
	noFollow = syscall.O_NOFOLLOW
	noBlock  = syscall.O_NONBLOCK
)
