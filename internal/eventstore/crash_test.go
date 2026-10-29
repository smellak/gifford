//go:build unix

package eventstore

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	crashModeEnv     = "GIFFORD_EVENTSTORE_CRASH"
	crashDBEnv       = "GIFFORD_EVENTSTORE_DB"
	crashLifetimeEnv = "GIFFORD_EVENTSTORE_LIFETIME"
	crashModeOpenTx  = "open-tx"
	crashModeCommits = "commit-loop"
	crashModeOutput  = "output-burst"
	crashReady       = "READY"
	crashLineBuffer  = 16
	crashBurstLines  = crashLineBuffer * 4
)

// crashExecer is the part of sql.DB and sql.Tx needed by the synthetic row
// fixture.
type crashExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertCrashControlRows(ctx context.Context, execer crashExecer, id string) error {
	stamp := FormatTime(testEventTime)
	if _, err := execer.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "client-"+id, "node-a", "agent-a", "node-b", "thr-"+id,
		"NORMAL", "PENDING", stamp, stamp, stamp, 0, "sha256:"+id); err != nil {
		return fmt.Errorf("insert message %s: %w", id, err)
	}
	if _, err := execer.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "recipient-"+id, "agent-a", "thr-"+id, "QUEUED", 2,
		stamp, stamp, stamp, 0, "sha256:"+id); err != nil {
		return fmt.Errorf("insert delivery %s: %w", id, err)
	}
	return nil
}

func crashBatch(writer int, session string, start, count int64) []protocol.EventEnvelope {
	batch := make([]protocol.EventEnvelope, 0, count)
	for sequence := start; sequence < start+count; sequence++ {
		batch = append(batch, testEvent(writer, session, sequence))
	}
	return batch
}

func crashHelperErrorf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "eventstore crash helper: "+format+"\n", args...)
}

func crashHelperFailf(format string, args ...any) {
	crashHelperErrorf(format, args...)
	os.Exit(2)
}

func crashHelperOpenTransaction(path string) {
	ctx := context.Background()
	store, err := Open(ctx, path)
	if err != nil {
		crashHelperFailf("open database: %v", err)
	}
	tx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		crashHelperFailf("begin transaction: %v", err)
	}
	statement, err := tx.PrepareContext(ctx, insertEventsSQL)
	if err != nil {
		crashHelperFailf("prepare events: %v", err)
	}
	defer func() { _ = statement.Close() }()
	for _, event := range crashBatch(8, "crash", 0, 500) {
		if _, err := statement.ExecContext(ctx, bindEvent(&event)...); err != nil {
			crashHelperFailf("insert event: %v", err)
		}
	}
	if err := insertCrashControlRows(ctx, tx, "crash-message"); err != nil {
		crashHelperFailf("%v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, crashReady); err != nil {
		crashHelperFailf("write ready: %v", err)
	}
	select {}
}

func crashHelperCommitLoop(path string) {
	ctx := context.Background()
	store, err := Open(ctx, path)
	if err != nil {
		crashHelperFailf("open database: %v", err)
	}
	for batchNumber := int64(1); ; batchNumber++ {
		start := (batchNumber - 1) * 1000
		if err := store.AppendEvents(ctx, crashBatch(9, "loop", start, 1000)); err != nil {
			crashHelperFailf("append batch %d: %v", batchNumber, err)
		}
		if _, err := fmt.Fprintln(os.Stdout, batchNumber); err != nil {
			crashHelperFailf("write batch %d: %v", batchNumber, err)
		}
	}
}

func crashHelperOutputBurst() {
	for line := range crashBurstLines {
		if _, err := fmt.Fprintln(os.Stdout, line); err != nil {
			crashHelperFailf("write output line %d: %v", line, err)
		}
	}
	os.Exit(0)
}

// TestHelperCrash is the terminal child selected by the crash recovery tests.
func TestHelperCrash(t *testing.T) {
	mode := os.Getenv(crashModeEnv)
	if mode == "" {
		t.Skip("event store crash helper")
	}
	lifetimeText := os.Getenv(crashLifetimeEnv)
	lifetime, err := time.ParseDuration(lifetimeText)
	if err != nil || lifetime <= 0 || lifetime > 30*time.Second {
		crashHelperFailf("invalid lifetime %q", lifetimeText)
	}
	timer := time.AfterFunc(lifetime, func() { os.Exit(3) })
	defer timer.Stop()
	if mode != crashModeOpenTx && mode != crashModeCommits && mode != crashModeOutput {
		crashHelperFailf("unknown mode %q", mode)
	}
	if mode == crashModeOutput {
		crashHelperOutputBurst()
	}
	path := os.Getenv(crashDBEnv)
	if path == "" {
		crashHelperFailf("database path is empty")
	}
	switch mode {
	case crashModeOpenTx:
		crashHelperOpenTransaction(path)
	case crashModeCommits:
		crashHelperCommitLoop(path)
	}
	crashHelperFailf("mode %q returned", mode)
}

type crashProcess struct {
	cmd           *exec.Cmd
	lines         <-chan string
	stopLines     chan struct{}
	stopLinesOnce sync.Once
	readerDone    chan struct{}
	readerErr     error
	waitDone      chan struct{}
	waitErr       error
	stderr        bytes.Buffer
}

func startCrashProcess(t *testing.T, path, mode, lifetime string) *crashProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHelperCrash$")
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = append(os.Environ(),
		crashDBEnv+"="+path,
		crashModeEnv+"="+mode,
		crashLifetimeEnv+"="+lifetime)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	lines := make(chan string, crashLineBuffer)
	process := &crashProcess{
		cmd:        cmd,
		lines:      lines,
		stopLines:  make(chan struct{}),
		readerDone: make(chan struct{}),
		waitDone:   make(chan struct{}),
	}
	cmd.Stderr = &process.stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		cancel()
		require.NoError(t, err)
		return nil
	}
	t.Cleanup(func() { process.cleanup(t, stdout) })
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			// Keep draining the pipe after the caller has enough evidence. Wait
			// starts only after this reader reaches EOF.
			select {
			case <-process.stopLines:
				continue
			default:
			}
			select {
			case lines <- scanner.Text():
			default:
			}
		}
		process.readerErr = scanner.Err()
		close(lines)
		close(process.readerDone)
	}()
	go func() {
		<-process.readerDone
		process.waitErr = cmd.Wait()
		close(process.waitDone)
		cancel()
	}()
	return process
}

func (process *crashProcess) cleanup(t *testing.T, stdout io.Closer) {
	t.Helper()
	process.stopLinesOnce.Do(func() { close(process.stopLines) })
	_ = process.cmd.Process.Kill()
	if !process.readerWithin() {
		_ = stdout.Close()
		if !process.readerWithin() {
			t.Error("crash helper stdout reader did not exit during cleanup")
		}
	}
	if ok, _ := process.waitWithin(); !ok {
		t.Error("crash helper did not exit during cleanup")
	}
}

func (process *crashProcess) readLine(t *testing.T, timeout time.Duration) string {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case line, ok := <-process.lines:
		if !ok {
			require.True(t, process.readerWithin(), "waiting for crash helper stdout reader")
			require.NoError(t, process.readerErr)
			require.FailNow(t, "crash helper closed stdout")
		}
		return line
	case <-timer.C:
		require.FailNow(t, "crash helper output timeout")
		return ""
	}
}

func (process *crashProcess) readerWithin() bool {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-process.readerDone:
		return true
	case <-timer.C:
		return false
	}
}

func (process *crashProcess) waitWithin() (bool, error) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-process.waitDone:
		return true, process.waitErr
	case <-timer.C:
		return false, nil
	}
}

func requireExitCode(t *testing.T, err error, code int) {
	t.Helper()
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, code, exitError.ExitCode())
}

func requireKilledChild(t *testing.T, process *crashProcess) {
	t.Helper()
	require.NoError(t, process.cmd.Process.Signal(syscall.SIGKILL))
	ok, err := process.waitWithin()
	require.True(t, ok, "waiting for SIGKILL")
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	status, ok := exitError.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled(), "child exited instead of being signaled: %v", err)
	require.Equal(t, syscall.SIGKILL, status.Signal())
}

func appendCrashBatches(ctx context.Context, store *Store, writer int, session string, total, batchSize int64) error {
	for start := int64(0); start < total; start += batchSize {
		if err := store.AppendEvents(ctx, crashBatch(writer, session, start, batchSize)); err != nil {
			return err
		}
	}
	return nil
}

func requireDatabaseHealthy(t *testing.T, db *sql.DB) {
	t.Helper()
	var integrity string
	require.NoError(t, db.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
	rows, err := db.QueryContext(t.Context(), "PRAGMA foreign_key_check")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

type crashControlSnapshot struct {
	clientID       string
	messageState   string
	agentID        string
	deliveryState  string
	priority       int64
	generation     int64
	controlState   string
	controlReason  string
	controlVersion int64
}

func readCrashControlSnapshot(t *testing.T, db *sql.DB, id string) crashControlSnapshot {
	t.Helper()
	var snapshot crashControlSnapshot
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT client_msg_id, state,
		control_state, control_reason, control_version FROM messages WHERE message_id = ?`, id).
		Scan(&snapshot.clientID, &snapshot.messageState, &snapshot.controlState,
			&snapshot.controlReason, &snapshot.controlVersion))
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT agent_id, state,
		priority, delivery_generation FROM delivery_queue WHERE message_id = ?`, id).
		Scan(&snapshot.agentID, &snapshot.deliveryState, &snapshot.priority, &snapshot.generation))
	return snapshot
}

func requireSessionSequence(t *testing.T, db *sql.DB, session string, wantCount int64) {
	t.Helper()
	var count, distinct int64
	var minimum, maximum sql.NullInt64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*),
		count(DISTINCT sequence), min(sequence), max(sequence)
		FROM events WHERE session_id = ?`, session).
		Scan(&count, &distinct, &minimum, &maximum))
	require.Equal(t, wantCount, count)
	require.Equal(t, wantCount, distinct)
	if wantCount == 0 {
		require.False(t, minimum.Valid)
		require.False(t, maximum.Valid)
		return
	}
	require.Equal(t, int64(0), minimum.Int64)
	require.Equal(t, wantCount-1, maximum.Int64)
}

// TestKillNineMidTransaction proves an uncommitted transaction disappears
// while prior events and control rows remain byte-for-byte queryable.
func TestKillNineMidTransaction(t *testing.T) {
	t.Parallel()

	path := testStorePath(t)
	store, err := Open(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, appendCrashBatches(t.Context(), store, 7, "base", 1000, 250))
	require.NoError(t, insertCrashControlRows(t.Context(), store.DB(), "baseline-message"))
	baseline := readCrashControlSnapshot(t, store.DB(), "baseline-message")
	var migrationCount int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM schema_migrations").Scan(&migrationCount))
	require.NoError(t, store.Close())

	child := startCrashProcess(t, path, crashModeOpenTx, "30s")
	require.Equal(t, crashReady, child.readLine(t, 20*time.Second))
	requireKilledChild(t, child)

	store, err = Open(t.Context(), path)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	requireDatabaseHealthy(t, store.DB())
	var reopenedMigrations int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM schema_migrations").Scan(&reopenedMigrations))
	require.Equal(t, migrationCount, reopenedMigrations)
	requireSessionSequence(t, store.DB(), "base", 1000)
	requireSessionSequence(t, store.DB(), "crash", 0)
	require.Equal(t, baseline, readCrashControlSnapshot(t, store.DB(), "baseline-message"))
	for _, table := range []string{tableMessages, tableDeliveryQueue} {
		var count int64
		require.NoError(t, store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM "+table+" WHERE message_id = ?", "crash-message").Scan(&count))
		require.Zero(t, count)
	}
	require.NoError(t, store.AppendEvents(t.Context(), crashBatch(6, "post-crash", 0, 10)))
}

// TestKillNineDuringCommits proves every visible commit is complete and its
// sequences stay contiguous over three fresh crash cycles.
func TestKillNineDuringCommits(t *testing.T) {
	t.Parallel()

	for round := range 3 {
		t.Run(fmt.Sprintf("round-%d", round+1), func(t *testing.T) {
			t.Parallel()
			path := testStorePath(t)
			child := startCrashProcess(t, path, crashModeCommits, "30s")
			for batch := child.readLine(t, 20*time.Second); batch != "2"; batch = child.readLine(t, 20*time.Second) {
				require.NotEmpty(t, batch)
			}
			requireKilledChild(t, child)

			store, err := Open(t.Context(), path)
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			requireDatabaseHealthy(t, store.DB())
			var count int64
			require.NoError(t, store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM events WHERE session_id = 'loop'").Scan(&count))
			require.GreaterOrEqual(t, count, int64(2000))
			require.Zero(t, count%1000)
			requireSessionSequence(t, store.DB(), "loop", count)
		})
	}
}

// TestCrashHelperBounds checks terminal invalid modes and lifetimes before
// allowing the helper to participate in the crash tests.
func TestCrashHelperBounds(t *testing.T) {
	invalidPath := filepath.Join(t.TempDir(), "must-not-exist.db")
	cases := []struct {
		name     string
		mode     string
		lifetime string
	}{
		{"unknown mode", "unknown", "30s"},
		{"missing lifetime", crashModeOpenTx, ""},
		{"zero lifetime", crashModeOpenTx, "0s"},
		{"long lifetime", crashModeOpenTx, "31s"},
		{"bad lifetime", crashModeOpenTx, "later"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			process := startCrashProcess(t, invalidPath, tc.mode, tc.lifetime)
			ok, err := process.waitWithin()
			require.True(t, ok)
			requireExitCode(t, err, 2)
			require.Contains(t, process.stderr.String(), "eventstore crash helper:")
			require.NoFileExists(t, invalidPath)
		})
	}
	t.Run("lifetime expires", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lifetime.db")
		process := startCrashProcess(t, path, crashModeOpenTx, "1s")
		require.Equal(t, crashReady, process.readLine(t, 5*time.Second))
		ok, err := process.waitWithin()
		require.True(t, ok)
		requireExitCode(t, err, 3)
		require.True(t, process.readerWithin())
		require.NoError(t, process.readerErr)
	})
	t.Run("output beyond consumer capacity", func(t *testing.T) {
		process := startCrashProcess(t, invalidPath, crashModeOutput, "5s")
		require.True(t, process.readerWithin(), "waiting for output reader")
		require.NoError(t, process.readerErr)
		require.Len(t, process.lines, crashLineBuffer)
		ok, err := process.waitWithin()
		require.True(t, ok, "waiting for output helper")
		require.NoError(t, err)
	})
}
