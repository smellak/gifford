// Package db implements offline database backup and restore commands.
package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/cli/runtimelock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/xdg"
)

const databaseName = "gifford.db"

// NewCmd returns the gifford db command group.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Group(
		"db",
		"Back up or restore the node database while the hub is stopped",
		newBackupCmd(),
		newRestoreCmd(),
	)
}

func newBackupCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "backup <destination>",
		Short: "Write a consistent offline backup without overwriting a file",
		Args:  onePathArg("gifford db backup"),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := backup(cmd.Context(), args[0]); err != nil {
				return commandError("backup", err)
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Backup written to %s\n", args[0]); err != nil {
				return commandError("backup", err)
			}
			return nil
		},
	}
}

func newRestoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restore <source>",
		Short: "Restore a backup offline and preserve the current database",
		Args:  onePathArg("gifford db restore"),
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := restore(cmd.Context(), args[0])
			if err != nil {
				return commandError("restore", err)
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(),
				"Database restored; recovery copy: %s\n", result.RecoveryPath); err != nil {
				return commandError("restore", err)
			}
			return nil
		},
	}
}

func onePathArg(command string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return &deps.ExitError{
				Code: exitcode.Usage,
				Err:  fmt.Errorf("%s: expected exactly one path", command),
			}
		}
		return nil
	}
}

func backup(ctx context.Context, destination string) (err error) {
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve dirs: %w", err)
	}
	lock, err := acquireRuntimeLock(dirs.Runtime)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, lock.Close())
	}()

	databasePath := filepath.Join(dirs.Data, databaseName)
	if err := eventstore.RecoverInterruptedRestore(ctx, databasePath); err != nil {
		return fmt.Errorf("recover interrupted restore: %w", err)
	}
	if err := requireDatabase(databasePath); err != nil {
		return err
	}
	store, err := eventstore.Open(ctx, databasePath)
	if err != nil {
		return fmt.Errorf("open event store: %w", err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err := store.Backup(ctx, destination); err != nil {
		return fmt.Errorf("write backup: %w", err)
	}
	return nil
}

func restore(ctx context.Context, source string) (result eventstore.RestoreResult, err error) {
	dirs, err := xdg.Resolve()
	if err != nil {
		return result, fmt.Errorf("resolve dirs: %w", err)
	}
	lock, err := acquireRuntimeLock(dirs.Runtime)
	if err != nil {
		return result, err
	}
	defer func() {
		err = errors.Join(err, lock.Close())
	}()
	result, err = eventstore.Restore(ctx, source, filepath.Join(dirs.Data, databaseName))
	if err != nil {
		if result.RecoveryPath != "" {
			return result, fmt.Errorf("restore event store (recovery copy preserved at %s): %w",
				result.RecoveryPath, err)
		}
		return result, fmt.Errorf("restore event store: %w", err)
	}
	return result, nil
}

func requireDatabase(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect event store: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("event store is not a regular file")
	}
	return nil
}

func acquireRuntimeLock(runtimeDir string) (*runtimelock.Lock, error) {
	// XDG runtime directories may be cleared across reboots while the durable
	// database remains. Recreate the private lock directory without creating a
	// database or config file.
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		return nil, fmt.Errorf("create runtime directory: %w", err)
	}
	lock, err := runtimelock.Acquire(runtimeDir)
	if err != nil {
		return nil, fmt.Errorf("acquire runtime lock: %w", err)
	}
	return lock, nil
}

func commandError(action string, err error) error {
	code := exitcode.Error
	if errors.Is(err, runtimelock.ErrHeld) {
		code = exitcode.Refused
	}
	return &deps.ExitError{
		Code: code,
		Err:  fmt.Errorf("gifford db %s: %w", action, err),
	}
}
