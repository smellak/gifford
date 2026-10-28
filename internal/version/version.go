// Package version reports the build identity of the binary. Version, Commit
// and Date are injected with -ldflags at release time; a plain go build or go
// install falls back to the module build information.
package version

import (
	"fmt"
	"runtime/debug"
)

// Injected by the linker; see Taskfile.yml and .goreleaser.yaml.
var (
	Version = ""
	Commit  = ""
	Date    = ""
)

// String returns a one-line build identity, e.g. "gifford v0.1.0 (abc1234, 2026-11-04)".
func String() string {
	v, c, d := Version, Commit, Date
	if info, ok := debug.ReadBuildInfo(); ok {
		if v == "" {
			v = info.Main.Version
		}
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if c == "" {
					c = s.Value
				}
			case "vcs.time":
				if d == "" {
					d = s.Value
				}
			}
		}
	}
	if v == "" {
		v = "devel"
	}
	if len(c) > 7 {
		c = c[:7]
	}
	return fmt.Sprintf("gifford %s (%s, %s)", v, orUnknown(c), orUnknown(d))
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
