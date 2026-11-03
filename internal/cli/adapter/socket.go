package adapter

import (
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/cli/status"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/xdg"
)

// socketPath resolves where one hook emission goes. The per-run socket
// from the environment wins when set; otherwise the hook falls back to
// the node-shared native entry so resumed sessions without a per-run
// path still deliver. After the per-user runtime entry, the hook resolves
// like status through the caller-owned node_socket directory, when its
// own config exists and is readable; a system-node client that cannot
// read it still falls through to the documented system node entry on
// Linux. An empty result keeps the emission silent.
func socketPath() string {
	if path := os.Getenv(SocketEnvironment); path != "" {
		return path
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return ""
	}
	candidate := filepath.Join(dirs.Runtime, nativeevents.NodeSocketName)
	if info, err := os.Stat(candidate); err == nil && info.Mode().Type() == os.ModeSocket {
		return candidate
	}
	if configured := status.NativeSocketDir(dirs); configured != dirs.Runtime {
		candidate = filepath.Join(configured, nativeevents.NodeSocketName)
		if info, err := os.Stat(candidate); err == nil && info.Mode().Type() == os.ModeSocket {
			return candidate
		}
	}
	return systemNativeSocket()
}
