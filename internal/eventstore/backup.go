package eventstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const backupPagesPerStep int32 = 128

type onlineBackuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// Backup writes a consistent online copy of the store to path. The copy is
// published only after SQLite finishes it, so cancellation leaves no partial
// backup at path and does not interrupt active writers.
func (s *Store) Backup(ctx context.Context, path string) (err error) {
	return s.backup(ctx, path, nil)
}

func (s *Store) backup(ctx context.Context, path string, progress func(remaining, total int)) (err error) {
	if path == "" || strings.Contains(path, "?") || strings.HasPrefix(path, "file:") || path == ":memory:" {
		return ErrInvalidArgument
	}
	clean := filepath.Clean(path)
	if _, statErr := os.Lstat(clean); statErr == nil {
		return fmt.Errorf("backup destination exists: %w", ErrInvalidArgument)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect backup destination: %w", statErr)
	}

	temporary, err := os.CreateTemp(filepath.Dir(clean), "."+filepath.Base(clean)+".backup-*")
	if err != nil {
		return fmt.Errorf("create backup temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath != "" {
			err = errors.Join(err, removeBackupTemporary(temporaryPath))
		}
	}()
	if closeErr := temporary.Close(); closeErr != nil {
		return fmt.Errorf("close backup temporary file: %w", closeErr)
	}

	if err := s.copyOnlineBackup(ctx, temporaryPath, progress); err != nil {
		return fmt.Errorf("copy event store backup: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("copy event store backup: %w", err)
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return fmt.Errorf("set backup permissions: %w", err)
	}
	// The hard link publishes without replacing a destination created by a
	// racing caller. Both names are in the same directory by construction.
	if err := os.Link(temporaryPath, clean); err != nil {
		return fmt.Errorf("publish event store backup: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove backup temporary file: %w", err)
	}
	temporaryPath = ""
	return nil
}

func (s *Store) copyOnlineBackup(
	ctx context.Context,
	path string,
	progress func(remaining, total int),
) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get backup connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close backup connection: %w", closeErr))
		}
	}()

	err = conn.Raw(func(driverConn any) (rawErr error) {
		backuper, ok := driverConn.(onlineBackuper)
		if !ok {
			return errors.New("sqlite driver does not support online backup")
		}
		backup, err := backuper.NewBackup(path)
		if err != nil {
			return fmt.Errorf("start sqlite backup: %w", err)
		}
		defer func() {
			if finishErr := backup.Finish(); finishErr != nil {
				rawErr = errors.Join(rawErr, fmt.Errorf("finish sqlite backup: %w", finishErr))
			}
		}()
		for {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("check backup context: %w", err)
			}
			more, err := backup.Step(backupPagesPerStep)
			if err != nil {
				if isBackupBusy(err) {
					select {
					case <-ctx.Done():
						return fmt.Errorf("wait to retry sqlite backup: %w", ctx.Err())
					case <-time.After(time.Millisecond):
						continue
					}
				}
				return fmt.Errorf("step sqlite backup: %w", err)
			}
			if progress != nil {
				progress(backup.Remaining(), backup.PageCount())
			}
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("check backup context after step: %w", err)
			}
			if !more {
				return nil
			}
			// Give active writers and cancellation a chance between bounded steps.
			runtime.Gosched()
		}
	})
	if err != nil {
		return fmt.Errorf("run online backup: %w", err)
	}
	return nil
}

func isBackupBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff
	return code == int(sqlite3.SQLITE_BUSY) || code == int(sqlite3.SQLITE_LOCKED)
}

func removeBackupTemporary(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove backup temporary file: %w", err)
	}
	return nil
}
