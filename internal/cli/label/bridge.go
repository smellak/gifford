package label

import (
	_ "embed"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	openCodeBridgeDir  = "plugins"
	openCodeBridgeFile = "gifford-label-bridge.js"
)

var (
	//go:embed opencode_bridge.js
	openCodeBridge []byte

	errBridgeDefaultDir = errors.New("default OpenCode config dir is forbidden")
	errBridgeNotEmpty   = errors.New("config dir must be empty")
	errBridgeUnsafe     = errors.New("config dir must be a real directory with an existing real parent")
	errBridgeWrite      = errors.New("could not write isolated OpenCode bridge")
)

// installOpenCodeBridge writes the embedded plugin into one isolated config
// directory. It never follows a link, merges with an existing configuration or
// replaces a file.
func installOpenCodeBridge(target string) error {
	return installOpenCodeBridgeWithOpenFile(target, os.OpenFile)
}

func installOpenCodeBridgeWithOpenFile(
	target string,
	openFile func(string, int, os.FileMode) (*os.File, error),
) error {
	abs, err := filepath.Abs(target)
	if err != nil || target == "" || isDefaultOpenCodeConfig(abs) {
		if err == nil && target != "" {
			return errBridgeDefaultDir
		}
		return errBridgeUnsafe
	}
	abs = filepath.Clean(abs)
	created, err := prepareEmptyBridgeDir(abs)
	if err != nil {
		return err
	}
	plugins := filepath.Join(abs, openCodeBridgeDir)
	plugin := filepath.Join(plugins, openCodeBridgeFile)
	pluginsCreated := false
	pluginCreated := false
	cleanup := func() {
		if pluginCreated {
			_ = os.Remove(plugin)
		}
		if pluginsCreated {
			_ = os.Remove(plugins)
		}
		if created {
			_ = os.Remove(abs)
		}
	}
	if err := os.Mkdir(plugins, 0o700); err != nil {
		cleanup()
		return errBridgeWrite
	}
	pluginsCreated = true
	f, err := openFile(plugin, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		cleanup()
		return errBridgeWrite
	}
	pluginCreated = true
	_, writeErr := f.Write(openCodeBridge)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		cleanup()
		return errBridgeWrite
	}
	return nil
}

func prepareEmptyBridgeDir(target string) (bool, error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		parentPath := filepath.Dir(target)
		parent, parentErr := os.Lstat(parentPath)
		if parentErr != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
			return false, errBridgeUnsafe
		}
		resolved, resolveErr := filepath.EvalSymlinks(parentPath)
		if resolveErr != nil || filepath.Clean(resolved) != filepath.Clean(parentPath) {
			return false, errBridgeUnsafe
		}
		if err := os.Mkdir(target, 0o700); err != nil {
			return false, errBridgeWrite
		}
		return true, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errBridgeUnsafe
	}
	resolved, resolveErr := filepath.EvalSymlinks(target)
	if resolveErr != nil || filepath.Clean(resolved) != filepath.Clean(target) {
		return false, errBridgeUnsafe
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return false, errBridgeUnsafe
	}
	if len(entries) != 0 {
		return false, errBridgeNotEmpty
	}
	if err := os.Chmod(target, 0o700); err != nil { //nolint:gosec // target is a private directory, not a file
		return false, errBridgeWrite
	}
	return false, nil
}

func isDefaultOpenCodeConfig(target string) bool {
	var base string
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		base = xdg
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return false
		}
		base = filepath.Join(home, ".config")
	}
	usual, err := filepath.Abs(filepath.Join(base, "opencode"))
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(usual), filepath.Clean(target))
	if err != nil {
		return false
	}
	return rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
