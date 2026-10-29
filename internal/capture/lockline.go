package capture

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// lockLineLimit caps a holder line read: role, pid and nonce fit in 64.
const lockLineLimit = 128

// readLockLine reads a lock file's holder line read-only, never
// creating it. A missing file is an empty line, not an error: the
// caller tells absence from liveness separately.
func readLockLine(root *os.Root, name string) (string, error) {
	f, err := root.Open(name)
	if err != nil {
		return "", fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, lockLineLimit+1))
	if err != nil {
		return "", fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	if len(data) > lockLineLimit {
		return "", fmt.Errorf("%w: lock %s too long", ErrUnsafePath, name)
	}
	return strings.TrimSuffix(string(data), "\n"), nil
}

// writeLockLine rewrites the holder line in place on a held lock file:
// truncate, then write, with one trailing newline.
func writeLockLine(f *os.File, line string) error {
	if strings.ContainsAny(line, "\n\x00") {
		return fmt.Errorf("%w: lock line", ErrUnsafePath)
	}
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("lock line: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("lock line: %w", err)
	}
	if _, err := io.WriteString(f, line+"\n"); err != nil {
		return fmt.Errorf("lock line: %w", err)
	}
	return nil
}
