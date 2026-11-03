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

func isolateSystemNativeSocket(t *testing.T) {
	t.Helper()
	useSystemDir(t, filepath.Join(t.TempDir(), "missing-system-entry"))
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

func writeNodeSocketConfig(t *testing.T, configHome, socketPath string) {
	t.Helper()
	// xdg resolves the config file under $XDG_CONFIG_HOME/gifford.
	dir := filepath.Join(configHome, "gifford")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	content := "schema_version: 1\nnode_socket:\n  path: " + socketPath + "\n  bridge_uid: 998\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(content), 0o600))
}

func nodeSocketConfigPath(t *testing.T, configHome string) string {
	t.Helper()
	return filepath.Join(configHome, "gifford", "config.yaml")
}

// The hook resolves like status: the caller-owned node_socket directory
// wins over the documented system entry. Removing that step resolves to
// the system entry instead, so this test kills the mutant.
func TestSocketPathPrefersNodeSocketDirOverSystemEntry(t *testing.T) {
	t.Setenv(SocketEnvironment, "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	configDir := filepath.Join(t.TempDir(), "config")
	nodeDir := t.TempDir()
	writeNodeSocketConfig(t, configDir, filepath.Join(nodeDir, "node.sock"))
	t.Setenv("XDG_CONFIG_HOME", configDir)
	nodeEntry := listenSystemEntry(t, nodeDir)
	systemDir := filepath.Join(t.TempDir(), "system")
	useSystemDir(t, systemDir)
	systemEntry := listenSystemEntry(t, systemDir)
	require.NotEqual(t, nodeEntry, systemEntry)
	require.Equal(t, nodeEntry, socketPath())
}

// Without a readable node_socket config, or when its directory holds no
// entry, the hook still falls through to the documented system entry.
func TestSocketPathFallsBackToSystemEntryWithoutNodeSocket(t *testing.T) {
	t.Setenv(SocketEnvironment, "")
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Run("unreadable config", func(t *testing.T) {
		configDir := filepath.Join(t.TempDir(), "config")
		writeNodeSocketConfig(t, configDir, filepath.Join(t.TempDir(), "node.sock"))
		require.NoError(t, os.Chmod(nodeSocketConfigPath(t, configDir), 0o000))
		t.Setenv("XDG_CONFIG_HOME", configDir)
		systemDir := filepath.Join(t.TempDir(), "system")
		useSystemDir(t, systemDir)
		require.Equal(t, listenSystemEntry(t, systemDir), socketPath())
	})
	t.Run("silent node directory", func(t *testing.T) {
		configDir := filepath.Join(t.TempDir(), "config")
		writeNodeSocketConfig(t, configDir, filepath.Join(t.TempDir(), "node.sock"))
		t.Setenv("XDG_CONFIG_HOME", configDir)
		systemDir := filepath.Join(t.TempDir(), "system")
		useSystemDir(t, systemDir)
		require.Equal(t, listenSystemEntry(t, systemDir), socketPath())
	})
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
