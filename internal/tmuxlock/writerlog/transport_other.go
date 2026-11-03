//go:build !linux

package writerlog

import (
	"context"
	"errors"
	"os"
)

func exchange(context.Context, string, *os.File, []byte) ([]byte, error) {
	return nil, errors.New("local descriptor transport is unavailable")
}
