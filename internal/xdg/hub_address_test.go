package xdg

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// The hub records its socket under the data directory; a client whose
// environment names another runtime directory still finds it there.
func TestHubAddressRoundTrip(t *testing.T) {
	t.Parallel()
	dirs := Dirs{Data: filepath.Join(t.TempDir(), "data"), Runtime: filepath.Join(t.TempDir(), "run")}
	require.Empty(t, ReadHubAddress(dirs), "no record before the hub writes one")
	require.Error(t, WriteHubAddress(dirs, "relative/hub.sock"))

	socket := filepath.Join(dirs.Runtime, HubSocketName)
	require.NoError(t, WriteHubAddress(dirs, socket))
	info, err := os.Stat(filepath.Join(dirs.Data, HubAddressFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, socket, ReadHubAddress(dirs))

	require.NoError(t, WriteHubAddress(dirs, socket+".2"), "a later record replaces the earlier one")
	require.Equal(t, socket+".2", ReadHubAddress(dirs))

	require.NoError(t, os.WriteFile(filepath.Join(dirs.Data, HubAddressFile), []byte("  \n"), 0o600))
	require.Empty(t, ReadHubAddress(dirs), "a blank record is no record")
}

func TestResolveMarksANamedRuntimeDirectory(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	dirs, err := Resolve()
	require.NoError(t, err)
	require.True(t, dirs.RuntimeNamed)

	t.Setenv("XDG_RUNTIME_DIR", "")
	dirs, err = Resolve()
	require.NoError(t, err)
	require.False(t, dirs.RuntimeNamed)
	require.Equal(t, filepath.Join(dirs.Data, "run"), dirs.Runtime)
}

func TestHubSocketCandidatesOrderAndResolve(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	hubRuntime := filepath.Join(base, "hub-run")
	clientRuntime := filepath.Join(base, "client-run")
	dirs := Dirs{Data: filepath.Join(base, "data"), Runtime: clientRuntime}
	served := filepath.Join(hubRuntime, HubSocketName)
	require.NoError(t, WriteHubAddress(dirs, served))

	require.Equal(t, []string{
		filepath.Join(clientRuntime, HubSocketName),
		served,
		filepath.Join("/run/user", strconv.Itoa(os.Getuid()), "gifford", HubSocketName),
		filepath.Join(dirs.Data, "run", HubSocketName),
	}, HubSocketCandidates(dirs))

	// Nothing listens: the environment's own path is returned, so the dial
	// error names what the caller expected.
	require.Equal(t, filepath.Join(clientRuntime, HubSocketName), ResolveHubSocket(dirs))

	// The hub's recorded socket exists: it wins over the absent own path.
	require.NoError(t, os.MkdirAll(hubRuntime, 0o700))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", served)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	require.Equal(t, served, ResolveHubSocket(dirs))

	// The environment names a runtime directory: that path and nothing
	// else, even with no socket there yet and a live hub recorded.
	named := dirs
	named.RuntimeNamed = true
	require.Equal(t, []string{filepath.Join(clientRuntime, HubSocketName)}, HubSocketCandidates(named))
	require.Equal(t, filepath.Join(clientRuntime, HubSocketName), ResolveHubSocket(named))

	// A plain file at the recorded path is not a socket.
	require.NoError(t, os.WriteFile(clientRuntime+"-x", nil, 0o600))
	other := Dirs{Data: filepath.Join(base, "data2"), Runtime: clientRuntime}
	require.NoError(t, WriteHubAddress(other, clientRuntime+"-x"))
	require.Equal(t, filepath.Join(clientRuntime, HubSocketName), ResolveHubSocket(other))
}
