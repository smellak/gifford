//go:build unix && !linux && !darwin

package capture

import "os"

// renameNoReplace is unsupported here: attach fails closed.
func renameNoReplace(*os.File, string, string) error {
	return errRenameUnsupported
}
