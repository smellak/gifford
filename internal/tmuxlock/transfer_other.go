//go:build !linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
)

func (w *NodeWriter) transferPlatform(
	context.Context,
	LiveBinding,
	string,
	string,
) (OwnershipResult, error) {
	return OwnershipResult{}, fmt.Errorf("transfer pane ownership: %w", errors.ErrUnsupported)
}
