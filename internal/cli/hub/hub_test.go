package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
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
	"github.com/smellak/gifford/internal/policy"
)

const unixNetwork = "unix"

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

type directoryLoopClock struct {
	mu    sync.Mutex
	now   time.Time
	reads chan struct{}
}

func (c *directoryLoopClock) Now() time.Time {
	c.mu.Lock()
	now := c.now
	c.mu.Unlock()
	select {
	case c.reads <- struct{}{}:
	default:
	}
	return now
}

func (c *directoryLoopClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type recordingDirectorySynchronizer struct {
	mu    sync.Mutex
	calls int
}

type recordingFederationReplicator struct {
	calls int
	work  int
	err   error
}

func (r *recordingFederationReplicator) ReplicateOnce(context.Context) (bool, error) {
	r.calls++
	if r.err != nil {
		return false, r.err
	}
	if r.work > 0 {
		r.work--
		return true, nil
	}
	return false, nil
}

func (s *recordingDirectorySynchronizer) SyncDirectories(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (s *recordingDirectorySynchronizer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
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

func TestReclaimStaleControlSocket(t *testing.T) {
	t.Run("stale socket", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "daemon.sock")
		listener, err := net.ListenUnix(unixNetwork, &net.UnixAddr{Name: path, Net: unixNetwork})
		require.NoError(t, err)
		listener.SetUnlinkOnClose(false)
		require.NoError(t, listener.Close())
		require.FileExists(t, path)

		require.NoError(t, reclaimStaleControlSocket(path))
		require.NoFileExists(t, path)
	})

	t.Run("live socket", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "daemon.sock")
		listener, err := net.ListenUnix(unixNetwork, &net.UnixAddr{Name: path, Net: unixNetwork})
		require.NoError(t, err)
		defer func() { require.NoError(t, listener.Close()) }()

		err = reclaimStaleControlSocket(path)
		require.ErrorContains(t, err, "live control server")
		require.FileExists(t, path)
	})

	t.Run("foreign file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "daemon.sock")
		require.NoError(t, os.WriteFile(path, []byte("foreign"), 0o600))

		err := reclaimStaleControlSocket(path)
		require.ErrorContains(t, err, "is not a socket")
		require.FileExists(t, path)
	})
}

func TestWaitSocketRequiresLiveListener(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.ListenUnix(unixNetwork, &net.UnixAddr{Name: path, Net: unixNetwork})
	require.NoError(t, err)
	listener.SetUnlinkOnClose(false)
	require.NoError(t, listener.Close())
	t.Cleanup(func() { _ = os.Remove(path) })

	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer cancel()
	require.ErrorContains(t, waitSocket(ctx, path), "wait control socket")
}

func TestDirectorySyncLoopRunsAtStartupAndSixtySeconds(t *testing.T) {
	start := time.Date(2026, time.October, 29, 22, 0, 0, 0, time.UTC)
	clk := &directoryLoopClock{now: start, reads: make(chan struct{}, 8)}
	synchronizer := &recordingDirectorySynchronizer{}
	stderr := &syncBuffer{}
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runDirectorySyncLoop(ctx, stderr, synchronizer, clk, ticks)
		close(done)
	}()

	require.Eventually(t, func() bool { return synchronizer.count() == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return strings.Contains(stderr.String(), "sync peer directories: unexpected EOF")
	}, time.Second, time.Millisecond)
	for len(clk.reads) > 0 {
		<-clk.reads
	}

	clk.set(start.Add(59 * time.Second))
	ticks <- time.Time{}
	<-clk.reads
	require.Equal(t, 1, synchronizer.count())

	clk.set(start.Add(directorySyncEvery))
	ticks <- time.Time{}
	<-clk.reads
	require.Eventually(t, func() bool { return synchronizer.count() == 2 }, time.Second, time.Millisecond)

	cancel()
	<-done
}

func TestReplicationPassDrainsDueRowsAndReportsFailure(t *testing.T) {
	t.Run("drain", func(t *testing.T) {
		replicator := &recordingFederationReplicator{work: 3}
		runReplicationPass(t.Context(), io.Discard, replicator)
		require.Equal(t, 4, replicator.calls)
	})

	t.Run("failure", func(t *testing.T) {
		replicator := &recordingFederationReplicator{err: io.ErrUnexpectedEOF}
		stderr := &syncBuffer{}
		runReplicationPass(t.Context(), stderr, replicator)
		require.Equal(t, 1, replicator.calls)
		require.Contains(t, stderr.String(), "replicate federation outbox: unexpected EOF")
	})
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

func TestHubStartsAndStopsFederationListener(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	trustBundle := filepath.Join(t.TempDir(), "trust")
	require.NoError(t, os.Mkdir(trustBundle, 0o700))
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\norganization_id: org-a\nfederation:\n  listen: 127.0.0.1:0\n  trust_bundle: "+trustBundle+"\n  advertise: https://node-a.example.invalid:7443\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	require.Contains(t, stderr.String(), "federation 127.0.0.1:")
	require.FileExists(t, filepath.Join(dataHome, "gifford", "keys", "node-tls.pem"))
	cancel()
	require.NoError(t, <-done)
}

func TestUnenrolledHubDefersFederationListener(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	trustBundle := filepath.Join(t.TempDir(), "trust")
	require.NoError(t, os.Mkdir(trustBundle, 0o700))
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\nfederation:\n  listen: 127.0.0.1:0\n  trust_bundle: "+trustBundle+"\n  advertise: https://node-a.example.invalid:7443\n")

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	require.NotContains(t, stderr.String(), "federation 127.0.0.1:")
	require.FileExists(t, filepath.Join(dataHome, "gifford", "keys", "node-tls.pem"))
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

func TestHubPersistsCompiledPolicyVersion(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")
	policyDir := filepath.Join(configHome, "gifford", "policies")
	require.NoError(t, os.MkdirAll(policyDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "peers.yaml"), []byte(`schema_version: 1
rules:
  - id: allow-peer
    effect: allow
    peer: node-b
    action: deliver_message
    agent: "agent-*"
`), 0o600))
	expected, err := policy.Load(policyDir)
	require.NoError(t, err)

	cancel, done, stderr := startedHub(t.Context(), t)
	waitSockets(t, runtimeHome)
	waitReady(t, stderr)
	cancel()
	require.NoError(t, <-done)

	store, err := eventstore.Open(t.Context(), filepath.Join(dataHome, "gifford", dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	var nodeVersion, digest, canonical string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT policy_version FROM nodes WHERE node_id = 'node-a'").Scan(&nodeVersion))
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT digest, compiled_json FROM policy_versions WHERE version = ?", expected.Version).
		Scan(&digest, &canonical))
	require.Equal(t, expected.Version, nodeVersion)
	require.Equal(t, expected.Digest, digest)
	require.Equal(t, expected.CompiledJSON, canonical)
}

func TestHubRefusesInvalidPolicyBeforeStartup(t *testing.T) {
	configHome, dataHome, runtimeHome := envBase(t)
	writeConfig(t, configHome, "schema_version: 1\nnode_id: node-a\n")
	policyDir := filepath.Join(configHome, "gifford", "policies")
	require.NoError(t, os.MkdirAll(policyDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "invalid.yaml"), []byte(`schema_version: 1
rules:
  - id: invalid
    effect: maybe
    peer: "*"
    action: ack
    agent: "agent-*"
`), 0o600))

	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})
	err := cmd.ExecuteContext(t.Context())
	require.ErrorIs(t, err, policy.ErrInvalidPolicy)
	for _, path := range []string{
		filepath.Join(dataHome, "gifford"), filepath.Join(runtimeHome, "gifford"),
	} {
		_, statErr := os.Stat(path)
		require.True(t, os.IsNotExist(statErr), "%s must not be created", path)
	}
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
