//go:build windows

package eventstore

import (
	"errors"
	"fmt"
	"os"
)

// Windows does not let os.Rename replace an existing file. The durable
// marker and permanent recovery copy make the remove/rename pair recoverable
// if the process stops between them.
func replaceDatabaseFile(sourcePath, databasePath string) error {
	if err := os.Remove(databasePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove replaced database file: %w", err)
	}
	if err := os.Rename(sourcePath, databasePath); err != nil {
		return fmt.Errorf("rename database file: %w", err)
	}
	return nil
}
