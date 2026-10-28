package redact_test

import (
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/width"
)

// TestUnicodeVersions pins the Unicode tables the v1 width model reads:
// Go's unicode categories and the x/text width table. A change of either
// needs a compatibility review and may bump PolicyVersion.
func TestUnicodeVersions(t *testing.T) {
	t.Parallel()
	require.Equal(t, "17.0.0", unicode.Version,
		"unicode tables changed: review compatibility before deciding PolicyVersion")
	require.Equal(t, "15.0.0", width.UnicodeVersion,
		"width tables changed: review compatibility before deciding PolicyVersion")
}
