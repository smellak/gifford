package label

import (
	_ "embed"
	"errors"
	"fmt"
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
	if target == "" {
		return errBridgeUnsafe
	}
	abs, err := filepath.Abs(target)
	if err != nil {
		return errBridgeUnsafe
	}
	abs = filepath.Clean(abs)
	forbidden, err := isDefaultOpenCodeConfig(abs)
	if err != nil || forbidden {
		return errBridgeDefaultDir
	}
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

func isDefaultOpenCodeConfig(target string) (bool, error) {
	resolvedTarget, err := resolveBridgePath(target)
	if err != nil {
		return false, err
	}
	if filepath.Base(resolvedTarget) == ".opencode" {
		return true, nil
	}

	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return false, errBridgeDefaultDir
	}
	resolvedHome, err := resolveBridgePath(home)
	if err != nil {
		return false, err
	}
	candidates := []string{
		filepath.Join(resolvedHome, ".config", "opencode"),
		filepath.Join(resolvedHome, ".opencode"),
	}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		resolvedXDG, resolveErr := resolveBridgePath(xdg)
		if resolveErr != nil {
			return false, resolveErr
		}
		candidates = append(candidates, filepath.Join(resolvedXDG, "opencode"))
	}
	for _, candidate := range candidates {
		resolvedCandidate, resolveErr := resolveBridgePath(candidate)
		if resolveErr != nil {
			return false, resolveErr
		}
		if bridgePathWithin(resolvedCandidate, resolvedTarget) {
			return true, nil
		}
	}
	return false, nil
}

// resolveBridgePath resolves every existing symlink prefix while retaining any
// missing suffix. Installation permits a missing target, so EvalSymlinks on the
// full path alone cannot establish whether two spellings name the same place.
func resolveBridgePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute bridge path: %w", err)
	}
	probe := filepath.Clean(abs)
	var missing []string
	for {
		// The user-selected path is intentionally inspected component by
		// component here; this guard never opens or writes it.
		if _, statErr := os.Lstat(probe); statErr == nil { //nolint:gosec // path inspection is the security boundary
			resolved, resolveErr := filepath.EvalSymlinks(probe)
			if resolveErr != nil {
				return "", fmt.Errorf("resolve bridge path: %w", resolveErr)
			}
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return filepath.Clean(resolved), nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf("inspect bridge path: %w", statErr)
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", os.ErrNotExist
		}
		missing = append(missing, filepath.Base(probe))
		probe = parent
	}
}

func bridgePathWithin(root, target string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(target))
	if err != nil {
		return false
	}
	return rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
