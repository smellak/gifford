//go:build !unix

package label

// Without these flags the checks before and after open still refuse a
// symlink or a special file.
const (
	noFollow = 0
	noBlock  = 0
)
