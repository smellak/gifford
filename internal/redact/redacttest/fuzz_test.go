package redacttest_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// mustFixtureOptionsF returns the fixture options, failing on any error.
func mustFixtureOptionsF(f *testing.F) redact.Options {
	f.Helper()
	opts := redacttest.Options()
	if err := opts.Validate(); err != nil {
		f.Fatal(err)
	}
	return opts
}

// mustScreensF loads the seeded screens, failing on any error.
func mustScreensF(f *testing.F) []redacttest.SeededScreen {
	f.Helper()
	screens, err := redacttest.Screens()
	if err != nil {
		f.Fatal(err)
	}
	return screens
}

// mustTextsF loads the seeded texts, failing on any error.
func mustTextsF(f *testing.F) []redacttest.SeededText {
	f.Helper()
	texts, err := redacttest.Texts()
	if err != nil {
		f.Fatal(err)
	}
	return texts
}

// FuzzRedactText throws arbitrary text at the v1 rules: line count and
// per-line width hold, a second pass is identical with zero counts, the
// same lines as a 40-column screen keep their widths, and only the three
// input errors may fail. Seeds are every fixture input.
func FuzzRedactText(f *testing.F) {
	r, err := redact.New(mustFixtureOptionsF(f))
	if err != nil {
		f.Fatal(err)
	}
	for _, s := range mustScreensF(f) {
		f.Add(strings.Join(s.Input.Rows, "\n"))
	}
	for _, x := range mustTextsF(f) {
		f.Add(x.Input)
	}
	f.Add("")
	f.Add("x")
	f.Fuzz(func(t *testing.T, s string) {
		out, err := r.RedactText(s)
		if err != nil {
			allowed := errors.Is(err, redact.ErrInvalidInput) ||
				errors.Is(err, redact.ErrInputTooLarge) ||
				errors.Is(err, redact.ErrUnsupportedInput)
			require.True(t, allowed, "unexpected error: %v", err)
			return
		}
		inLines := strings.Split(s, "\n")
		outLines := strings.Split(out.Text, "\n")
		require.Len(t, outLines, len(inLines))
		for i := range inLines {
			require.Equal(t, redact.CellWidth(inLines[i]), redact.CellWidth(outLines[i]), "line %d", i)
		}
		again, err := r.RedactText(out.Text)
		require.NoError(t, err)
		require.Equal(t, out.Text, again.Text)
		require.Equal(t, redact.Counts{}, again.Counts)
		screen, err := r.RedactScreen(redact.Screen{Rows: inLines, Cols: 40})
		if err != nil {
			allowed := errors.Is(err, redact.ErrInvalidInput) ||
				errors.Is(err, redact.ErrInputTooLarge) ||
				errors.Is(err, redact.ErrUnsupportedInput)
			require.True(t, allowed, "unexpected screen error: %v", err)
			return
		}
		require.Len(t, screen.Rows, len(inLines))
		for i := range inLines {
			require.Equal(t, redact.CellWidth(inLines[i]), redact.CellWidth(screen.Rows[i]), "row %d", i)
		}
	})
}
