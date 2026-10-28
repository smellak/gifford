package redact

import (
	"fmt"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxSensitivePathBytes bounds one configured sensitive path.
const maxSensitivePathBytes = 4096

// Options configures a Redactor. SensitivePaths holds the node paths whose
// text the sensitive_path rule masks, longest match first at rule time.
// There is no config key in this subphase: both consumers construct the
// Redactor themselves. The zero value selects no paths.
type Options struct {
	SensitivePaths []string `json:"sensitive_paths,omitempty" yaml:"sensitive_paths,omitempty"`
}

// Validate checks the sensitive paths without changing them: duplicates
// stay for New to drop. Every refusal wraps ErrInvalidOptions and names
// the entry index, never the entry.
func (o Options) Validate() error {
	if len(o.SensitivePaths) > MaxSensitivePaths {
		return fmt.Errorf(
			"sensitive_paths: %d entries (max %d): %w",
			len(o.SensitivePaths), MaxSensitivePaths, ErrInvalidOptions,
		)
	}
	for i, p := range o.SensitivePaths {
		if err := checkSensitivePath(i, p); err != nil {
			return err
		}
	}
	return nil
}

// checkSensitivePath validates one entry by index.
func checkSensitivePath(i int, p string) error {
	refuse := func(reason string) error {
		return fmt.Errorf("sensitive_paths[%d]: %s: %w", i, reason, ErrInvalidOptions)
	}
	switch {
	case p == "":
		return refuse("empty entry")
	case len(p) > maxSensitivePathBytes:
		return refuse("entry over 4096 bytes")
	case !utf8.ValidString(p):
		return refuse("entry not UTF-8")
	case strings.IndexFunc(p, unicode.IsControl) >= 0:
		return refuse("entry holds a control rune")
	case p[0] == ' ' || p[len(p)-1] == ' ':
		return refuse("entry has surrounding spaces")
	case !strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "~/"):
		return refuse("not absolute or home-relative")
	case p == "/" || p == "~/":
		return refuse("root path not allowed")
	case path.Clean(p) != p:
		return refuse("entry not clean")
	default:
		return nil
	}
}
