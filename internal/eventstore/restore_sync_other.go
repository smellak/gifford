//go:build !windows

package eventstore

import (
	"errors"
	"fmt"
	"os"
)

func syncDirectory(path string) (err error) {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	defer func() {
		err = errors.Join(err, dir.Close())
	}()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}
