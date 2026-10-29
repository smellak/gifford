package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/ctl"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/eventstore"
)

// envBase wires the XDG directories to scratch and returns them.
func envBase(t *testing.T) (configHome, dataHome, runtimeHome string) {
	t.Helper()
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "run"))
	return filepath.Join(base, "config"), filepath.Join(base, "data"), filepath.Join(base, "run")
}

// writeConfig drops a config.yaml with node_id under the config home.
func writeConfig(t *testing.T, configHome, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(configHome, "gifford"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"), []byte(body), 0o600))
}

// syncBuffer is a goroutine-safe stderr for the hub's background lines.
type syncBuffer struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

// startedHub runs the hub command until cancel and waits for both
// sockets. It returns the cancel func, the done channel and stderr.
func startedHub(ctx context.Context, t *testing.T) (context.CancelFunc, <-chan error, *syncBuffer) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	return cancel, done, stderr
}

// waitSockets polls for hub.sock and daemon.sock under runtimeHome.
func waitSockets(t *testing.T, runtimeHome string) {
	t.Helper()
	hubSock := filepath.Join(runtimeHome, "gifford", "hub.sock")
	daemonSock := filepath.Join(runtimeHome, "gifford", "daemon.sock")
	require.Eventually(t, func() bool {
		if _, err := os.Stat(hubSock); err != nil {
			return false
		}
		_, err := os.Stat(daemonSock)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "hub sockets never appeared")
	for _, sock := range []string{hubSock, daemonSock} {
		fi, err := os.Stat(sock)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), "%s mode", sock)
	}
}

// waitReady polls the hub stderr for the ready line: the sockets exist
// before the hub reports itself serving, so files alone are not enough.
func waitReady(t *testing.T, stderr *syncBuffer) {
	t.Helper()
	require.Eventually(t, func() bool {
		return strings.Contains(stderr.String(), "ready")
	}, 10*time.Second, 10*time.Millisecond, "hub never reported ready")
}

// ctlStatus runs gifford ctl channel status against the live hub.
func ctlStatus(t *testing.T) {
	t.Helper()
	cmd := ctl.NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"channel", "status"})
	require.NoError(t, cmd.ExecuteContext(t.Context()))
}

// TestHubServesBothSockets starts the hub on scratch XDG dirs: both
// sockets appear with their modes and the ready line names them.
func TestHubServesBothSockets(t *testing.T) {
	configHome, _, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	cancel()
	require.NoError(t, <-done)
}

// TestHubEnsuresLocalNode proves startup pins the node signing key before
// the Hub reports ready.
func TestHubEnsuresLocalNode(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	cancel()
	require.NoError(t, <-done)

	store, err := eventstore.Open(t.Context(), filepath.Join(dataHome, "gifford", dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	var local int64
	var status string
	var publicKey []byte
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT local, status, signing_public_key FROM nodes WHERE node_id = 'node-a'").
		Scan(&local, &status, &publicKey))
	require.Equal(t, int64(1), local)
	require.Equal(t, "ACTIVE", status)
	require.Len(t, publicKey, ed25519.PublicKeySize)
}

// TestHubAnswersCtl runs one control command against the live hub.
func TestHubAnswersCtl(t *testing.T) {
	configHome, _, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	ctlStatus(t)
	cancel()
	require.NoError(t, <-done)
}

// TestHubStopsCleanly cancels the hub: it returns nil and the control
// socket is removed.
func TestHubStopsCleanly(t *testing.T) {
	configHome, _, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	cancel()
	require.NoError(t, <-done)
	_, err := os.Stat(filepath.Join(runtimeHome, "gifford", "daemon.sock"))
	require.True(t, os.IsNotExist(err), "daemon.sock must be removed on stop")
}

// TestSecondHubRefuses starts a second hub on the same directories: it
// fails fast while the first keeps answering.
func TestSecondHubRefuses(t *testing.T) {
	configHome, _, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, _ := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)

	second := NewCmd(&deps.Deps{})
	second.SilenceErrors = true
	second.SilenceUsage = true
	second.SetOut(io.Discard)
	second.SetErr(io.Discard)
	second.SetArgs([]string{})
	err := second.ExecuteContext(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "another hub")

	ctlStatus(t)
	cancel()
	require.NoError(t, <-done)
}

// runSecondHub runs one hub command against the same XDG directories
// and returns its error: the caller must hold a live first hub.
func runSecondHub() error {
	second := NewCmd(&deps.Deps{})
	second.SilenceErrors = true
	second.SilenceUsage = true
	second.SetOut(io.Discard)
	second.SetErr(io.Discard)
	second.SetArgs([]string{})
	return second.ExecuteContext(context.Background())
}

// dbDigest returns the hex SHA-256 of the database file.
func dbDigest(t *testing.T, dbPath string) string {
	t.Helper()
	raw, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// TestSecondHubLeavesDatabaseAlone starts a second hub while one
// serves: with the database present it must be byte-identical after
// the refusal, and with the database path removed the refusal must
// not recreate it.
func TestSecondHubLeavesDatabaseAlone(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")

	cancel, done, _ := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	dbPath := filepath.Join(dataHome, "gifford", dbName)
	require.FileExists(t, dbPath)

	before := dbDigest(t, dbPath)
	err := runSecondHub()
	require.Error(t, err)
	require.Contains(t, err.Error(), "another hub")
	require.Equal(t, before, dbDigest(t, dbPath), "second hub must not modify gifford.db")

	require.NoError(t, os.Remove(dbPath))
	err = runSecondHub()
	require.Error(t, err)
	require.Contains(t, err.Error(), "another hub")
	_, statErr := os.Stat(dbPath)
	require.True(t, os.IsNotExist(statErr), "second hub must not recreate gifford.db")

	cancel()
	require.NoError(t, <-done)
}

// TestHubWithoutNodeID refuses before creating anything: a missing
// file, then a config without node_id, leave the directories untouched.
func TestHubWithoutNodeID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setup     func(t *testing.T, configHome string)
		absentAll bool
	}{
		{"missing file", func(*testing.T, string) {}, true},
		{"no node_id", func(t *testing.T, configHome string) {
			t.Helper()
			writeConfig(t, configHome, "schema_version: 1\n")
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configHome, dataHome, runtimeHome := envBase(t)
			tc.setup(t, configHome)

			cmd := NewCmd(&deps.Deps{})
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{})
			err := cmd.ExecuteContext(t.Context())
			require.Error(t, err)
			require.Contains(t, err.Error(), "node_id")

			absent := []string{
				filepath.Join(dataHome, "gifford"),
				filepath.Join(runtimeHome, "gifford"),
			}
			if tc.absentAll {
				absent = append(absent, filepath.Join(configHome, "gifford"))
			}
			for _, dir := range absent {
				_, statErr := os.Stat(dir)
				require.True(t, os.IsNotExist(statErr), "%s must not be created", dir)
			}
		})
	}
}
