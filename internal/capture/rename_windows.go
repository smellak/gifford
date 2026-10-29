//go:build !unix

package capture

import "os"

// renameNoReplace is unsupported here: entry points fail closed before
// any publication.
func renameNoReplace(*os.File, string, string) error {
	return errRenameUnsupported
}
