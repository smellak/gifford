//go:build !unix

package proxy

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// run reports that the PTY proxy needs unix PTY support.
func run(_ context.Context, _ Options) (Exit, error) {
	return Exit{}, fmt.Errorf("pty proxy on %s: %w", runtime.GOOS, errors.ErrUnsupported)
}
