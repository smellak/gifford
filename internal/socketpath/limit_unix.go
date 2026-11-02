//go:build unix

package socketpath

import "golang.org/x/sys/unix"

// MaxBytes is the largest Unix-domain socket path that leaves room for the
// terminating NUL in this platform's sockaddr_un.sun_path field.
const MaxBytes = len(unix.RawSockaddrUnix{}.Path) - 1
