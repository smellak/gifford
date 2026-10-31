//go:build !windows

package eventstore

import (
	"fmt"
	"os"
)

// replaceDatabaseFile is atomic when source and destination share a
// filesystem, which Restore guarantees by staging beside the database.
//
//nolint:gosec // both paths are validated same-directory restore paths under the runtime lock.
func replaceDatabaseFile(sourcePath, databasePath string) error {
	if err := os.Rename(sourcePath, databasePath); err != nil {
		return fmt.Errorf("rename database file: %w", err)
	}
	return nil
}
