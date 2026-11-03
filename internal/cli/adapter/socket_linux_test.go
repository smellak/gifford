//go:build linux

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

// useSystemDir injects a scratch system node directory. Hook tests never
// touch the real /run/gifford.
func useSystemDir(t *testing.T, dir string) {
	t.Helper()
	previous := systemNativeSocketDir
	systemNativeSocketDir = dir
	t.Cleanup(func() { systemNativeSocketDir = previous })
}

func listenSystemEntry(t *testing.T, dir string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.Chmod(dir, 0o700))
	entry := filepath.Join(dir, nativeevents.NodeSocketName)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", entry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return entry
}

func TestSocketPathFallsBackToTheSystemEntry(t *testing.T) {
	t.Setenv(SocketEnvironment, "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	systemDir := filepath.Join(t.TempDir(), "node")
	useSystemDir(t, systemDir)
	require.Equal(t, listenSystemEntry(t, systemDir), socketPath())
}

func TestSocketPathPrefersTheEnvironmentOverTheSystemEntry(t *testing.T) {
	t.Setenv(SocketEnvironment, "/run/per-run.sock")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	systemDir := filepath.Join(t.TempDir(), "node")
	useSystemDir(t, systemDir)
	listenSystemEntry(t, systemDir)
	require.Equal(t, "/run/per-run.sock", socketPath())
}

func TestSystemNativeSocketRefusesAnUnsafeDirectory(t *testing.T) {
	t.Setenv(SocketEnvironment, "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	for _, mode := range []os.FileMode{0o777, 0o770, 0o707} {
		t.Run(mode.String(), func(t *testing.T) {
			systemDir := filepath.Join(t.TempDir(), "node")
			useSystemDir(t, systemDir)
			entry := listenSystemEntry(t, systemDir)
			require.NotEmpty(t, entry)
			require.NoError(t, os.Chmod(systemDir, mode))
			require.Empty(t, systemNativeSocket())
			require.Empty(t, socketPath())
		})
	}
}

func TestSystemNativeSocketRefusesSymlinks(t *testing.T) {
	t.Run("linked directory", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "target")
		entry := listenSystemEntry(t, target)
		require.NotEmpty(t, entry)
		link := filepath.Join(t.TempDir(), "link")
		require.NoError(t, os.Symlink(target, link))
		useSystemDir(t, link)
		require.Empty(t, systemNativeSocket())
	})
	t.Run("linked leaf", func(t *testing.T) {
		systemDir := filepath.Join(t.TempDir(), "node")
		entry := listenSystemEntry(t, systemDir)
		require.NoError(t, os.Remove(entry))
		require.NoError(t, os.Symlink(entry, entry))
		useSystemDir(t, systemDir)
		require.Empty(t, systemNativeSocket())
	})
}

func TestSystemNativeSocketRefusesANonSocketLeaf(t *testing.T) {
	systemDir := filepath.Join(t.TempDir(), "node")
	require.NoError(t, os.MkdirAll(systemDir, 0o700))
	require.NoError(t, os.Chmod(systemDir, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(systemDir, nativeevents.NodeSocketName), []byte("x"), 0o600))
	useSystemDir(t, systemDir)
	require.Empty(t, systemNativeSocket())
}

func TestSystemNativeSocketNeedsTheConfiguredDirectory(t *testing.T) {
	useSystemDir(t, filepath.Join(t.TempDir(), "missing"))
	require.Empty(t, systemNativeSocket())
}
