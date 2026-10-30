//go:build unix

package capture

import "syscall"

// noBlock keeps opening a source from waiting on a FIFO.
const noBlock = syscall.O_NONBLOCK
