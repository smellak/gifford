//go:build !linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func acquireOwnedAt(
	context.Context,
	string,
	validatedRecordPolicy,
	LiveBinding,
	time.Duration,
) (*PanelGuard, LockOutcome, error) {
	return nil, LockUnprovisioned, fmt.Errorf("owned panel lock: %w", errors.ErrUnsupported)
}

func requireProtocolModePlatform(string, string, uint32, uint32) error {
	return errors.ErrUnsupported
}
