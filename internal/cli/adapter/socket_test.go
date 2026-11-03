package adapter

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
)

func TestSocketPathPrefersTheEnvironment(t *testing.T) {
	t.Setenv(SocketEnvironment, "/run/per-run.sock")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	require.Equal(t, "/run/per-run.sock", socketPath())
}

func TestSocketPathFallsBackToTheNodeEntry(t *testing.T) {
	t.Setenv(SocketEnvironment, "")
	runtime := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtime)
	nodeDir := filepath.Join(runtime, "gifford")
	require.NoError(t, os.MkdirAll(nodeDir, 0o700))
	entry := filepath.Join(nodeDir, nativeevents.NodeSocketName)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", entry)
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	require.Equal(t, entry, socketPath())
}

func TestSocketPathStaysSilentWithoutANodeEntry(t *testing.T) {
	isolateSystemNativeSocket(t)
	t.Setenv(SocketEnvironment, "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	require.Empty(t, socketPath())
}

func TestSocketPathIgnoresANonSocketEntry(t *testing.T) {
	isolateSystemNativeSocket(t)
	t.Setenv(SocketEnvironment, "")
	runtime := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtime)
	nodeDir := filepath.Join(runtime, "gifford")
	require.NoError(t, os.MkdirAll(nodeDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(nodeDir, nativeevents.NodeSocketName), []byte("x"), 0o600))
	require.Empty(t, socketPath())
}
