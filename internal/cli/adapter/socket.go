package adapter

import (
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/xdg"
)

// socketPath resolves where one hook emission goes. The per-run socket
// from the environment wins when set; otherwise the hook falls back to
// the node-shared native entry so resumed sessions without a per-run
// path still deliver. An empty result keeps the emission silent.
func socketPath() string {
	if path := os.Getenv(SocketEnvironment); path != "" {
		return path
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return ""
	}
	candidate := filepath.Join(dirs.Runtime, nativeevents.NodeSocketName)
	info, err := os.Stat(candidate)
	if err != nil || info.Mode().Type() != os.ModeSocket {
		return ""
	}
	return candidate
}
