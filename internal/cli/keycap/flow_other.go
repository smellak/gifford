//go:build !unix

package keycap

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// runFlow needs PTYs on both ends, so outside unix the command reports
// the platform as unsupported.
func runFlow(_ context.Context, _ flowParams) error {
	return fmt.Errorf("gifford keycap on %s: %w", runtime.GOOS, errors.ErrUnsupported)
}
