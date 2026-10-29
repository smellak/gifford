//go:build !unix

package session

// Without these flags the check after open still refuses a link or a
// special file.
const (
	noFollow = 0
	noBlock  = 0
)
