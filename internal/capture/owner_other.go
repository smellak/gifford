//go:build !unix

package capture

import "os"

// fileIdentity is unavailable off unix: identity checks fail closed.
func fileIdentity(os.FileInfo) (uint32, bool, bool) {
	return 0, false, false
}
