// Package xdg resolves and prepares gifford's three base directories
// (config, data, runtime) following the XDG Base Directory layout, with
// fallbacks when the environment variables are unset.
package xdg

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Dirs holds the three resolved gifford directories.
type Dirs struct {
	Config  string // $XDG_CONFIG_HOME/gifford, default $HOME/.config/gifford
	Data    string // $XDG_DATA_HOME/gifford, default $HOME/.local/share/gifford
	Runtime string // $XDG_RUNTIME_DIR/gifford, default <Data>/run when unset
}

// Resolve computes the three directories from the environment. It does not
// touch the filesystem.
func Resolve() (Dirs, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Dirs{}, fmt.Errorf("resolve home directory: %w", err)
	}

	configHome := os.Getenv("XDG_CONFIG_HOME")
	if configHome == "" {
		configHome = filepath.Join(home, ".config")
	}
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		dataHome = filepath.Join(home, ".local", "share")
	}

	dirs := Dirs{
		Config: filepath.Join(configHome, "gifford"),
		Data:   filepath.Join(dataHome, "gifford"),
	}
	if runtimeHome := os.Getenv("XDG_RUNTIME_DIR"); runtimeHome != "" {
		dirs.Runtime = filepath.Join(runtimeHome, "gifford")
	} else {
		dirs.Runtime = filepath.Join(dirs.Data, "run")
	}
	return dirs, nil
}

const requiredMode = 0o700

// DirCheck reports the state of one resolved directory after EnsureAll.
type DirCheck struct {
	Name    string      // "config", "data" or "runtime"
	Path    string      // resolved path that was checked
	Created bool        // true if it did not exist and EnsureAll created it
	Mode    os.FileMode // permission bits observed (zero if stat failed)
	OK      bool        // the path is a directory and Mode.Perm() == 0700
}

// EnsureAll creates each missing directory with mode 0700 and reports the
// state of all three, in the order config, data, runtime. It never changes
// the mode of a directory that already exists: a pre-existing directory
// with the wrong mode is reported in the returned slice, not fixed. A path
// that exists but is not a directory (a regular file, say) is left as it
// is, reported with OK false, and also returned as an error. The returned
// slice always has exactly three entries, even when err != nil.
func EnsureAll(d Dirs) ([]DirCheck, error) {
	order := []struct{ name, path string }{
		{"config", d.Config},
		{"data", d.Data},
		{"runtime", d.Runtime},
	}

	checks := make([]DirCheck, 0, len(order))
	var errs error
	for _, e := range order {
		check := DirCheck{Name: e.name, Path: e.path}
		info, statErr := os.Stat(e.path)
		if errors.Is(statErr, os.ErrNotExist) {
			if mkErr := os.MkdirAll(e.path, requiredMode); mkErr != nil {
				errs = errors.Join(errs, fmt.Errorf("create %s dir %s: %w", e.name, e.path, mkErr))
				checks = append(checks, check)
				continue
			}
			check.Created = true
			info, statErr = os.Stat(e.path)
		}
		if statErr != nil {
			errs = errors.Join(errs, fmt.Errorf("stat %s dir %s: %w", e.name, e.path, statErr))
			checks = append(checks, check)
			continue
		}
		check.Mode = info.Mode().Perm()
		if !info.IsDir() {
			errs = errors.Join(errs, fmt.Errorf("%s dir %s: not a directory", e.name, e.path))
			checks = append(checks, check)
			continue
		}
		check.OK = check.Mode == requiredMode
		checks = append(checks, check)
	}
	return checks, errs
}
