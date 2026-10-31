package eventstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/migrations"
)

const restoreInventoryTable = "restore_inventory"

func createRestoreInventory(t *testing.T, path, value string) *Store {
	t.Helper()
	store, err := Open(t.Context(), path)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"CREATE TABLE "+restoreInventoryTable+" (value TEXT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"INSERT INTO "+restoreInventoryTable+" (value) VALUES (?)", value)
	require.NoError(t, err)
	return store
}

func readRestoreInventory(t *testing.T, database interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) string {
	t.Helper()
	var value string
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT value FROM "+restoreInventoryTable).Scan(&value))
	return value
}

func rawFileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(raw)
}

func schemaMigrationCount(t *testing.T, path string) int {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer func() { require.NoError(t, database.Close()) }()
	var count int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT count(*) FROM schema_migrations").Scan(&count))
	return count
}

func TestRestoreMigratesOnlyStagingAndPreservesRecovery(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workingSource := filepath.Join(dir, "source-working.db")
	sourcePath := filepath.Join(dir, "source-backup.db")
	source := createRestoreInventory(t, workingSource, "from-backup")
	require.NoError(t, source.MigrateDown(t.Context(), 1))
	require.NoError(t, source.Backup(t.Context(), sourcePath))
	require.NoError(t, source.Close())
	oldMigrationCount := schemaMigrationCount(t, sourcePath)
	embedded, err := parseMigrations(migrations.FS())
	require.NoError(t, err)
	require.Equal(t, len(embedded)-1, oldMigrationCount)
	sourceDigest := rawFileDigest(t, sourcePath)

	databasePath := filepath.Join(dir, "gifford.db")
	current := createRestoreInventory(t, databasePath, "before-restore")
	require.NoError(t, current.MigrateDown(t.Context(), 2))
	require.NoError(t, current.Close())
	currentMigrationCount := schemaMigrationCount(t, databasePath)

	result, err := Restore(t.Context(), sourcePath, databasePath)
	require.NoError(t, err)
	require.NotEmpty(t, result.RecoveryPath)
	require.FileExists(t, result.RecoveryPath)
	require.NoFileExists(t, restoreMarkerPath(databasePath))
	require.Equal(t, sourceDigest, rawFileDigest(t, sourcePath), "restore source changed")
	require.Equal(t, oldMigrationCount, schemaMigrationCount(t, sourcePath),
		"restore source was migrated")
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(result.RecoveryPath)
		require.NoError(t, statErr)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	restored, err := Open(t.Context(), databasePath)
	require.NoError(t, err)
	require.Equal(t, "from-backup", readRestoreInventory(t, restored.DB()))
	require.Equal(t, len(embedded), schemaRowCount(t.Context(), t, restored.DB()))
	require.NoError(t, restored.Close())

	recovery, err := sql.Open("sqlite", result.RecoveryPath)
	require.NoError(t, err)
	require.Equal(t, "before-restore", readRestoreInventory(t, recovery))
	require.NoError(t, recovery.Close())
	require.Equal(t, currentMigrationCount, schemaMigrationCount(t, result.RecoveryPath),
		"recovery copy was migrated")
	require.FileExists(t, result.RecoveryPath)
}

func TestRestoreRejectsCurrentDatabaseAsSource(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "gifford.db")
	store := createRestoreInventory(t, path, "kept")
	require.NoError(t, store.Close())
	before := rawFileDigest(t, path)

	_, err := Restore(t.Context(), path, path)
	require.ErrorIs(t, err, ErrInvalidArgument)
	require.Equal(t, before, rawFileDigest(t, path))
}

func TestRestoreRejectsInvalidSourceBeforeRecoveryCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	databasePath := filepath.Join(dir, "gifford.db")
	current := createRestoreInventory(t, databasePath, "kept")
	require.NoError(t, current.Close())
	before := rawFileDigest(t, databasePath)
	sourcePath := filepath.Join(dir, "invalid.db")
	require.NoError(t, os.WriteFile(sourcePath, []byte("not a sqlite database\n"), 0o600))

	_, err := Restore(t.Context(), sourcePath, databasePath)
	require.Error(t, err)
	require.Equal(t, before, rawFileDigest(t, databasePath))
	recoveryPaths, globErr := filepath.Glob(databasePath + ".recovery-*")
	require.NoError(t, globErr)
	require.Empty(t, recoveryPaths)
}

func TestRecoverInterruptedRestoreRefusesBeforeCreatingDatabase(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "gifford.db")
	require.NoError(t, os.WriteFile(restoreMarkerPath(databasePath), []byte("not-json\n"), 0o600))

	err := RecoverInterruptedRestore(t.Context(), databasePath)
	require.Error(t, err)
	require.NoFileExists(t, databasePath)
	require.FileExists(t, restoreMarkerPath(databasePath))
}

func TestRecoverInterruptedRestoreRepublishesPermanentCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	databasePath := filepath.Join(dir, "gifford.db")
	recoveryPath := filepath.Join(dir, "gifford.db.recovery-kept")
	store := createRestoreInventory(t, databasePath, "recover-me")
	require.NoError(t, store.Backup(t.Context(), recoveryPath))
	require.NoError(t, store.Close())
	databaseDigest := rawFileDigest(t, databasePath)
	recoveryDigest := rawFileDigest(t, recoveryPath)
	missingReplacementDigest := sha256.Sum256([]byte("replacement no longer available"))
	marker := restoreMarker{
		Version:        restoreMarkerVersion,
		Database:       filepath.Base(databasePath),
		Replacement:    ".gifford.db.restore-new-missing",
		Recovery:       filepath.Base(recoveryPath),
		BeforeSHA256:   fmt.Sprintf("%x", databaseDigest),
		AfterSHA256:    fmt.Sprintf("%x", missingReplacementDigest),
		RecoverySHA256: fmt.Sprintf("%x", recoveryDigest),
	}
	require.NoError(t, writeRestoreMarker(restoreMarkerPath(databasePath), marker))
	require.NoError(t, removeDatabaseSidecars(databasePath))
	require.NoError(t, os.Remove(databasePath))

	require.NoError(t, RecoverInterruptedRestore(t.Context(), databasePath))
	require.FileExists(t, databasePath)
	require.FileExists(t, recoveryPath)
	require.NoFileExists(t, restoreMarkerPath(databasePath))
	recovered, err := Open(t.Context(), databasePath)
	require.NoError(t, err)
	require.Equal(t, "recover-me", readRestoreInventory(t, recovered.DB()))
	require.NoError(t, recovered.Close())
}

func TestRecoverInterruptedRestoreCompletesMissingCanonicalGap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	databasePath := filepath.Join(dir, "gifford.db")
	recoveryPath := filepath.Join(dir, "gifford.db.recovery-kept")
	replacementPath := filepath.Join(dir, ".gifford.db.restore-new-kept")
	current := createRestoreInventory(t, databasePath, "before-gap")
	require.NoError(t, current.Backup(t.Context(), recoveryPath))
	require.NoError(t, current.Close())
	replacement := createRestoreInventory(t, replacementPath, "after-gap")
	require.NoError(t, replacement.Close())
	databaseDigest := rawFileDigest(t, databasePath)
	recoveryDigest := rawFileDigest(t, recoveryPath)
	replacementDigest := rawFileDigest(t, replacementPath)
	marker := restoreMarker{
		Version:        restoreMarkerVersion,
		Database:       filepath.Base(databasePath),
		Replacement:    filepath.Base(replacementPath),
		Recovery:       filepath.Base(recoveryPath),
		BeforeSHA256:   fmt.Sprintf("%x", databaseDigest),
		AfterSHA256:    fmt.Sprintf("%x", replacementDigest),
		RecoverySHA256: fmt.Sprintf("%x", recoveryDigest),
	}
	require.NoError(t, writeRestoreMarker(restoreMarkerPath(databasePath), marker))
	require.NoError(t, removeDatabaseSidecars(databasePath))
	require.NoError(t, os.Remove(databasePath))

	require.NoError(t, RecoverInterruptedRestore(t.Context(), databasePath))
	require.NoFileExists(t, replacementPath)
	require.FileExists(t, recoveryPath)
	require.NoFileExists(t, restoreMarkerPath(databasePath))
	recovered, err := Open(t.Context(), databasePath)
	require.NoError(t, err)
	require.Equal(t, "after-gap", readRestoreInventory(t, recovered.DB()))
	require.NoError(t, recovered.Close())
}
