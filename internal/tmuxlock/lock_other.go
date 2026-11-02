//go:build !linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func acquirePlatform(
	context.Context,
	string,
	uint32,
	lockPathParts,
	time.Duration,
) (*PanelGuard, LockOutcome, error) {
	return nil, LockUnprovisioned, fmt.Errorf("panel lock: %w", errors.ErrUnsupported)
}

func releasePanelLock(int) error {
	return errors.ErrUnsupported
}
