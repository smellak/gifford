package socketpath

import "fmt"

// LengthError reports a Unix-domain socket path that does not leave room for
// the terminating NUL in the platform sockaddr.
type LengthError struct {
	Path   string
	Length int
	Limit  int
}

// Error returns the actionable diagnostic shared by every socket listener.
func (e *LengthError) Error() string {
	return fmt.Sprintf(
		"socket path %s is %d bytes, the limit is %d; use a shorter XDG_RUNTIME_DIR",
		e.Path,
		e.Length,
		e.Limit,
	)
}

// Check rejects paths that do not fit the platform's Unix sockaddr.
func Check(path string) error {
	if len(path) <= MaxBytes {
		return nil
	}
	return &LengthError{Path: path, Length: len(path), Limit: MaxBytes}
}
