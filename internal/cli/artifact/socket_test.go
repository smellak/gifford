package artifact

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/xdg"

	"github.com/smellak/gifford/internal/cli/deps"
)

const (
	testSocketFlag    = "--socket"
	testSocketSend    = "send"
	testSocketList    = "list"
	testSocketReceive = "receive"
	testSocketStatus  = "status"
	testSocketBad     = "relative.sock"
)

// testSocketDirs names a runtime directory no hub serves and a fresh data
// directory with no recorded hub address.
func testSocketDirs(t *testing.T) xdg.Dirs {
	t.Helper()
	return xdg.Dirs{Runtime: "/run/user/1000/gifford", Data: t.TempDir()}
}

func TestResolveArtifactSocket(t *testing.T) {
	t.Parallel()

	resolved, err := resolveArtifactSocket("", testSocketDirs(t))
	require.NoError(t, err)
	require.Equal(t, "/run/user/1000/gifford/hub.sock", resolved)

	resolved, err = resolveArtifactSocket("/run/gifford/node.sock", testSocketDirs(t))
	require.NoError(t, err)
	require.Equal(t, "/run/gifford/node.sock", resolved)

	for _, bad := range []string{"relative/hub.sock", "hub.sock", "/tmp/../node.sock", "/run/gifford/./node.sock"} {
		_, err := resolveArtifactSocket(bad, testSocketDirs(t))
		require.ErrorContains(t, err, "socket path must be absolute and clean", "flag %q", bad)
	}
}

func TestArtifactSocketFlagReachesEverySubcommand(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "note.txt")
	require.NoError(t, os.WriteFile(file, []byte("note"), 0o600))
	validID := "art-0123456789abcdef0123456789abcdef"
	cases := map[string][]string{
		testSocketSend:    {testSocketSend, file, "--to", "agent-recipient", testSocketFlag, testSocketBad},
		testSocketList:    {testSocketList, testSocketFlag, testSocketBad},
		testSocketReceive: {testSocketReceive, validID, testSocketFlag, testSocketBad},
		testSocketStatus:  {testSocketStatus, validID, testSocketFlag, testSocketBad},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(args)
			require.EqualError(t, cmd.Execute(), "socket path must be absolute and clean")
		})
	}
}

// TestArtifactSocketFlagSkipsDefaultHub serves the runtime hub.sock and points
// --socket at a missing path: the command must fail on the chosen socket and
// never dial the live default.
func TestArtifactSocketFlagSkipsDefaultHub(t *testing.T) {
	// Touches process-wide XDG variables: never parallel.
	runtime := filepath.Join(t.TempDir(), "runtime", "gifford")
	require.NoError(t, os.MkdirAll(runtime, 0o700))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", filepath.Dir(runtime))

	defaultSocket := filepath.Join(runtime, "hub.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", defaultSocket)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	var accepted atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = conn.Close()
		}
	}()

	missing := filepath.Join(t.TempDir(), "nowhere", "node.sock")
	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--socket", missing, "list"})
	err = cmd.Execute()
	require.ErrorContains(t, err, missing)
	require.Zero(t, accepted.Load(), "a --socket failure must not touch hub.sock")
}
