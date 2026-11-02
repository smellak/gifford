//go:build !linux

package adapter

import (
	"context"
	"errors"

	"github.com/smellak/gifford/internal/nativeevents"
)

func emitMuseFallback(
	context.Context,
	nativeevents.Hello,
	nativeevents.Event,
) error {
	return errors.New("muse socket discovery unavailable")
}
