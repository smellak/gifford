package eventstore

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupRestoresConsistentCopyWithActiveWriter(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	insertMessagingTestMessage(t, store.DB(), "message-1")
	stamp := FormatTime(testEventTime)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, "message-1", "agent-a", 0, "WHEN_SAFE", 1, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO acks
		(message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, ?, ?, ?)`, "message-1", "agent-a", 1, "ACKNOWLEDGED", stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"CREATE TABLE backup_activity (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	writer, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	writerDone := false
	defer func() {
		if !writerDone {
			require.NoError(t, writer.Rollback())
		}
	}()
	_, err = writer.ExecContext(t.Context(), "INSERT INTO backup_activity (id) VALUES (1)")
	require.NoError(t, err)

	backupPath := filepath.Join(t.TempDir(), "backup.db")
	require.NoError(t, store.Backup(t.Context(), backupPath))
	require.NoError(t, writer.Commit())
	writerDone = true

	backup, err := sql.Open("sqlite", backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backup.Close()) })
	var integrity string
	require.NoError(t, backup.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
	for _, table := range []string{tableMessages, tableMessageRecipients, tableAcks} {
		var sourceCount, backupCount int
		require.NoError(t, store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM "+table).Scan(&sourceCount))
		require.NoError(t, backup.QueryRowContext(t.Context(),
			"SELECT count(*) FROM "+table).Scan(&backupCount))
		require.Equal(t, sourceCount, backupCount, table)
	}
	var sourceActivity, backupActivity int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM backup_activity").Scan(&sourceActivity))
	require.NoError(t, backup.QueryRowContext(t.Context(),
		"SELECT count(*) FROM backup_activity").Scan(&backupActivity))
	require.Equal(t, 1, sourceActivity)
	require.Zero(t, backupActivity)
}

func TestBackupCancellationRemovesPartialCopy(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	const payloadBytes = 8 << 20
	_, err := store.DB().ExecContext(t.Context(), "CREATE TABLE backup_payload (data BLOB NOT NULL)")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(),
		"INSERT INTO backup_payload (data) VALUES (zeroblob(?))", payloadBytes)
	require.NoError(t, err)

	dir := t.TempDir()
	backupPath := filepath.Join(dir, "cancelled.db")
	ctx, cancel := context.WithCancel(t.Context())
	var remaining, total int
	err = store.backup(ctx, backupPath, func(stepRemaining, stepTotal int) {
		if stepRemaining > 0 {
			remaining = stepRemaining
			total = stepTotal
			cancel()
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Positive(t, remaining)
	require.Greater(t, total, remaining)

	_, err = os.Stat(backupPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	temporaryFiles, err := filepath.Glob(filepath.Join(dir, ".cancelled.db.backup-*"))
	require.NoError(t, err)
	require.Empty(t, temporaryFiles)
	var count, length int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*), length(data) FROM backup_payload").Scan(&count, &length))
	require.Equal(t, 1, count)
	require.Equal(t, payloadBytes, length)
	var integrity string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
}

func TestBackupRejectsExistingDestination(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	path := filepath.Join(t.TempDir(), "existing.db")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	err := store.Backup(t.Context(), path)
	require.ErrorIs(t, err, ErrInvalidArgument)
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("keep"), contents)
}

func TestBackupCancelledBeforeStart(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path := filepath.Join(t.TempDir(), "cancelled.db")
	err := store.Backup(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
