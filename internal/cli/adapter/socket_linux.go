//go:build linux

package adapter

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/smellak/gifford/internal/nativeevents"
)

// systemNativeSocketDir is the documented system node directory holding the
// shared native entry. Tests override it; production code never looks
// anywhere else, and hook tests never touch the real /run/gifford.
var systemNativeSocketDir = "/run/gifford"

// systemNativeSocket is the last hook candidate, Linux only, after the
// environment and the per-user runtime entry. It is valid only when no
// component of the path is a symlink, the system directory itself is a
// directory writable by neither group nor others, and the leaf is a
// socket. GIFFORD_ADAPTER_SOCKET still wins: socketPath returns it
// untouched before reaching here, and nothing beyond this fixed path is
// ever tried.
func systemNativeSocket() string {
	dir := filepath.Clean(systemNativeSocketDir)
	candidate := filepath.Join(dir, nativeevents.NodeSocketName)
	if !filepath.IsAbs(candidate) {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(filepath.Clean(candidate), "/"), "/")
	current := "/"
	dirChecked := false
	for index, part := range parts {
		if part == "" || part == "." || part == ".." {
			return ""
		}
		next := filepath.Join(current, part)
		info, err := os.Lstat(next)
		if err != nil {
			return ""
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return ""
		}
		last := index == len(parts)-1
		if !last && !info.IsDir() {
			return ""
		}
		if next == dir {
			if !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
				return ""
			}
			dirChecked = true
		}
		if last && info.Mode().Type() != os.ModeSocket {
			return ""
		}
		current = next
	}
	if !dirChecked {
		return ""
	}
	return candidate
}
