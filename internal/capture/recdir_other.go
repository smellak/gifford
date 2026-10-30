//go:build !unix

package capture

import "fmt"

// recordable is false: without unix file modes no recordings dir is
// private, so an import is not implemented on this system.
const recordable = false

func checkRecordingsDir(string) error {
	return fmt.Errorf("%w: recordings need unix file modes", ErrNotImplemented)
}
