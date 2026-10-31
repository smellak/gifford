package redact_test

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/width"

	"github.com/smellak/gifford/internal/redact"
)

// TestUnicodeVersions pins the Unicode tables the v2 width model reads:
// Go's unicode categories and the x/text width table. A change of either
// needs a compatibility review and may bump PolicyVersion.
func TestUnicodeVersions(t *testing.T) {
	t.Parallel()
	require.Equal(t, "17.0.0", unicode.Version,
		"unicode tables changed: review compatibility before deciding PolicyVersion")
	require.Equal(t, "17.0.0", width.UnicodeVersion,
		"width tables changed: review compatibility before deciding PolicyVersion")
}

// TestUnicode17WideRuneRedaction pins one of the Unicode 15 to 17 width
// changes through the same text and screen redaction paths production uses.
func TestUnicode17WideRuneRedaction(t *testing.T) {
	t.Parallel()

	const (
		wideRune      = "\u2630"
		sensitivePath = "/synthetic/" + wideRune
		wantCells     = 13
	)
	require.Equal(t, 2, redact.CellWidth(wideRune))
	require.Equal(t, wantCells, redact.CellWidth(sensitivePath))

	r, err := redact.New(redact.Options{SensitivePaths: []string{sensitivePath}})
	require.NoError(t, err)
	want := strings.Repeat(string(redact.Placeholder), wantCells)
	wantCounts := redact.Counts{
		Total:  1,
		ByKind: map[redact.Kind]int64{redact.KindSensitivePath: 1},
	}

	text, err := r.RedactText(sensitivePath)
	require.NoError(t, err)
	require.Equal(t, want, text.Text)
	require.Equal(t, "2", text.PolicyVersion)
	require.Equal(t, wantCounts, text.Counts)

	screen, err := r.RedactScreen(redact.Screen{Cols: wantCells, Rows: []string{sensitivePath}})
	require.NoError(t, err)
	require.Equal(t, []string{want}, screen.Rows)
	require.Equal(t, "2", screen.PolicyVersion)
	require.Equal(t, wantCounts, screen.Counts)
}
