//go:build unix

package capture

import (
	"os"
	"syscall"
)

// fileIdentity reports the owner uid and whether fi has exactly one
// link. The comparison avoids naming the platform link-count type.
func fileIdentity(fi os.FileInfo) (uint32, bool, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false, false
	}
	return st.Uid, st.Nlink == 1, true
}
