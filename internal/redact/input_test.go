package redact_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestInputValidation pins the §5.2 checks on a rule-less Redactor: bad
// input fails before any rule with ErrInvalidInput or ErrInputTooLarge,
// valid input reaches the missing rules and fails with ErrInternal, and a
// nil receiver behaves like the zero value.
func TestInputValidation(t *testing.T) {
	t.Parallel()

	var zero redact.Redactor
	var nilRedactor *redact.Redactor

	screens := []struct {
		name   string
		screen redact.Screen
		want   error
	}{
		{"escape in row", redact.Screen{Rows: []string{"ok\x1b[2K"}}, redact.ErrInvalidInput},
		{"tab in row", redact.Screen{Rows: []string{"a\tb"}}, redact.ErrInvalidInput},
		{"newline in row", redact.Screen{Rows: []string{"a\nb"}}, redact.ErrInvalidInput},
		{"c1 in row", redact.Screen{Rows: []string{"a\u0085b"}}, redact.ErrInvalidInput},
		{"invalid utf8", redact.Screen{Rows: []string{"a\xffb"}}, redact.ErrInvalidInput},
		{"negative cols", redact.Screen{Rows: []string{"ok"}, Cols: -1}, redact.ErrInvalidInput},
		{"cols over max", redact.Screen{Rows: []string{"ok"}, Cols: 10001}, redact.ErrInvalidInput},
		{"too many rows", redact.Screen{Rows: make([]string, 10001)}, redact.ErrInputTooLarge},
		{
			"too many bytes",
			redact.Screen{Rows: []string{strings.Repeat("a", 1<<19), strings.Repeat("b", 1<<19), "c"}},
			redact.ErrInputTooLarge,
		},
		{"valid", redact.Screen{Rows: []string{"hello"}, Cols: 80}, redact.ErrInternal},
	}
	for _, tc := range screens {
		t.Run("screen/"+tc.name, func(t *testing.T) {
			t.Parallel()
			for _, r := range []*redact.Redactor{&zero, nilRedactor} {
				got, err := r.RedactScreen(tc.screen)
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, redact.ScreenResult{}, got)
			}
		})
	}

	texts := []struct {
		name string
		text string
		want error
	}{
		{"escape", "a\x1bb", redact.ErrInvalidInput},
		{"nul", "a\x00b", redact.ErrInvalidInput},
		{"invalid utf8", "a\xffb", redact.ErrInvalidInput},
		{"too many bytes", strings.Repeat("a", (1<<20)+1), redact.ErrInputTooLarge},
		{"valid", "hello world", redact.ErrInternal},
	}
	for _, tc := range texts {
		t.Run("text/"+tc.name, func(t *testing.T) {
			t.Parallel()
			for _, r := range []*redact.Redactor{&zero, nilRedactor} {
				got, err := r.RedactText(tc.text)
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, redact.TextResult{}, got)
			}
		})
	}
}

// TestErrorsCarryNoInput builds the refusals around a runtime-assembled
// secret and requires that no 4-rune substring of it leaks into the error
// text. The secret is assembled from a bare prefix on purpose: no committed
// line holds a secret shape.
func TestErrorsCarryNoInput(t *testing.T) {
	t.Parallel()

	secret := "ghp_" + strings.Repeat("a", 36)
	require.Len(t, secret, 40)

	var windows []string
	runes := []rune(secret)
	for i := 0; i+4 <= len(runes); i++ {
		windows = append(windows, string(runes[i:i+4]))
	}
	require.NotEmpty(t, windows)

	var zero redact.Redactor
	errs := make([]error, 0, 3)
	_, err := zero.RedactScreen(redact.Screen{Rows: []string{secret + "\x1b"}})
	errs = append(errs, err)
	_, err = zero.RedactText(secret + "\x00" + strings.Repeat(secret, 1<<15))
	errs = append(errs, err)
	_, err = zero.RedactText(strings.Repeat(secret, 1<<15))
	errs = append(errs, err)

	for i, err := range errs {
		require.Error(t, err, "case %d", i)
		for _, w := range windows {
			require.NotContains(t, err.Error(), w, "case %d", i)
		}
	}
}
