//go:build !linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
)

func (w *NodeWriter) reconcilePlatform(
	context.Context,
	LiveBinding,
	string,
	ReconcileObserver,
) (RecoveryResult, error) {
	return emptyRecoveryResult(), fmt.Errorf("reconcile pane: %w", errors.ErrUnsupported)
}

func (w *NodeWriter) repairPlatform(
	context.Context,
	LiveBinding,
) (RecoveryResult, error) {
	return emptyRecoveryResult(), fmt.Errorf("repair pane: %w", errors.ErrUnsupported)
}
