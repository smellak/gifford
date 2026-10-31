package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/cli/runtimelock"
	"github.com/smellak/gifford/internal/eventstore"
)

type testNode struct {
	databasePath string
	runtimeDir   string
}

func setupTestNode(t *testing.T, value string) testNode {
	t.Helper()
	base := t.TempDir()
	configHome := filepath.Join(base, "config")
	dataHome := filepath.Join(base, "data")
	runtimeHome := filepath.Join(base, "run")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	dataDir := filepath.Join(dataHome, "gifford")
	runtimeDir := filepath.Join(runtimeHome, "gifford")
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	databasePath := filepath.Join(dataDir, databaseName)
	store, err := eventstore.Open(t.Context(), databasePath)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), "CREATE TABLE maintenance_inventory (value TEXT NOT NULL)")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"INSERT INTO maintenance_inventory (value) VALUES (?)", value)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return testNode{databasePath: databasePath, runtimeDir: runtimeDir}
}

func executeDBCommand(ctx context.Context, args ...string) error {
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	return cmd.ExecuteContext(ctx)
}

func requireExitCode(t *testing.T, err error, code int) {
	t.Helper()
	var exitErr *deps.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, code, exitErr.Code)
}

func fileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(raw)
}

func createRestoreSource(t *testing.T, dir, value string) string {
	t.Helper()
	path := filepath.Join(dir, "source.db")
	store, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), "CREATE TABLE maintenance_inventory (value TEXT NOT NULL)")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"INSERT INTO maintenance_inventory (value) VALUES (?)", value)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return path
}

func TestCommandsRefuseWhileRuntimeLockIsHeld(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, node testNode)
	}{
		{
			name: "backup",
			run: func(t *testing.T, node testNode) {
				destination := filepath.Join(t.TempDir(), "backup.db")
				before := fileDigest(t, node.databasePath)
				err := executeDBCommand(t.Context(), "backup", destination)
				requireExitCode(t, err, exitcode.Refused)
				require.NoFileExists(t, destination)
				require.Equal(t, before, fileDigest(t, node.databasePath))
			},
		},
		{
			name: "restore",
			run: func(t *testing.T, node testNode) {
				source := createRestoreSource(t, t.TempDir(), "new")
				beforeDatabase := fileDigest(t, node.databasePath)
				beforeSource := fileDigest(t, source)
				err := executeDBCommand(t.Context(), "restore", source)
				requireExitCode(t, err, exitcode.Refused)
				require.Equal(t, beforeDatabase, fileDigest(t, node.databasePath))
				require.Equal(t, beforeSource, fileDigest(t, source))
				recovery, globErr := filepath.Glob(node.databasePath + ".recovery-*")
				require.NoError(t, globErr)
				require.Empty(t, recovery)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := setupTestNode(t, "old")
			lock, err := runtimelock.Acquire(node.runtimeDir)
			require.NoError(t, err)
			defer func() { require.NoError(t, lock.Close()) }()
			tc.run(t, node)
		})
	}
}

func TestBackupRefusesExistingDestination(t *testing.T) {
	setupTestNode(t, "kept")
	destination := filepath.Join(t.TempDir(), "backup.db")
	require.NoError(t, os.WriteFile(destination, []byte("do not replace"), 0o600))

	err := executeDBCommand(t.Context(), "backup", destination)
	requireExitCode(t, err, exitcode.Error)
	raw, readErr := os.ReadFile(destination)
	require.NoError(t, readErr)
	require.Equal(t, []byte("do not replace"), raw)
}

func TestBackupCreatesPrivateConsistentDatabase(t *testing.T) {
	node := setupTestNode(t, "kept")
	destination := filepath.Join(t.TempDir(), "backup.db")

	require.NoError(t, executeDBCommand(t.Context(), "backup", destination))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(destination)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	backup, err := eventstore.Open(t.Context(), destination)
	require.NoError(t, err)
	defer func() { require.NoError(t, backup.Close()) }()
	var value string
	require.NoError(t, backup.DB().QueryRowContext(t.Context(),
		"SELECT value FROM maintenance_inventory").Scan(&value))
	require.Equal(t, "kept", value)
	require.DirExists(t, node.runtimeDir)
}

func TestBackupRecreatesEphemeralRuntimeDirectory(t *testing.T) {
	node := setupTestNode(t, "kept")
	require.NoError(t, os.Remove(node.runtimeDir))
	destination := filepath.Join(t.TempDir(), "backup.db")

	require.NoError(t, executeDBCommand(t.Context(), "backup", destination))
	info, err := os.Stat(node.runtimeDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
}

func TestBackupCancellationLeavesNoDestination(t *testing.T) {
	setupTestNode(t, "kept")
	destination := filepath.Join(t.TempDir(), "backup.db")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := executeDBCommand(ctx, "backup", destination)
	require.ErrorIs(t, err, context.Canceled)
	require.NoFileExists(t, destination)
}

func TestRestoreCommandReplacesDatabaseAndReportsRecovery(t *testing.T) {
	node := setupTestNode(t, "old")
	source := createRestoreSource(t, t.TempDir(), "new")
	sourceDigest := fileDigest(t, source)
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"restore", source})

	require.NoError(t, cmd.ExecuteContext(t.Context()))
	require.Contains(t, output.String(), "recovery copy:")
	require.Equal(t, sourceDigest, fileDigest(t, source))
	recoveryPaths, err := filepath.Glob(node.databasePath + ".recovery-*")
	require.NoError(t, err)
	require.Len(t, recoveryPaths, 1)
	require.Contains(t, output.String(), recoveryPaths[0])

	restored, err := eventstore.Open(t.Context(), node.databasePath)
	require.NoError(t, err)
	var value string
	require.NoError(t, restored.DB().QueryRowContext(t.Context(),
		"SELECT value FROM maintenance_inventory").Scan(&value))
	require.Equal(t, "new", value)
	require.NoError(t, restored.Close())

	recovery, err := eventstore.Open(t.Context(), recoveryPaths[0])
	require.NoError(t, err)
	require.NoError(t, recovery.DB().QueryRowContext(t.Context(),
		"SELECT value FROM maintenance_inventory").Scan(&value))
	require.Equal(t, "old", value)
	require.NoError(t, recovery.Close())
}

func TestBackupArityIsUsage(t *testing.T) {
	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"backup"})
	err := cmd.Execute()
	requireExitCode(t, err, exitcode.Usage)
}
