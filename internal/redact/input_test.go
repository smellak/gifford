package redact_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
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

// TestErrorsCarryNoInput builds every refusal of TestOptions and
// TestInputValidation around one runtime-assembled redacttest value and
// requires each to fail with its listed class while no 4-rune substring
// of the secret leaks into the error text. Every input stays within the
// size limits, except the too-large cases, so the intended check fires;
// the zero Redactor's ErrInternal on valid input is included.
func TestErrorsCarryNoInput(t *testing.T) {
	t.Parallel()

	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	var windows []string
	runes := []rune(secret)
	for i := 0; i+4 <= len(runes); i++ {
		windows = append(windows, string(runes[i:i+4]))
	}
	require.NotEmpty(t, windows)

	var zero redact.Redactor
	many := make([]string, 257)
	for i := range many {
		many[i] = "/" + secret
	}
	rows := make([]string, 10001)
	for i := range rows {
		rows[i] = secret
	}
	opts := func(paths []string) error {
		return redact.Options{SensitivePaths: paths}.Validate()
	}
	screenErr := func(s redact.Screen) error {
		_, err := zero.RedactScreen(s)
		return err
	}
	textErr := func(s string) error {
		_, err := zero.RedactText(s)
		return err
	}
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"options/too many", opts(many), redact.ErrInvalidOptions},
		{"options/empty", opts([]string{"/" + secret, ""}), redact.ErrInvalidOptions},
		{"options/oversized", opts([]string{"/" + secret + strings.Repeat("b", 4096)}), redact.ErrInvalidOptions},
		{"options/not utf8", opts([]string{"/" + secret + "\xff"}), redact.ErrInvalidOptions},
		{"options/control", opts([]string{"/" + secret + "\x07"}), redact.ErrInvalidOptions},
		{"options/spaces", opts([]string{"/" + secret + " "}), redact.ErrInvalidOptions},
		{"options/relative", opts([]string{secret}), redact.ErrInvalidOptions},
		{"options/root", opts([]string{"/"}), redact.ErrInvalidOptions},
		{"options/home root", opts([]string{"~/"}), redact.ErrInvalidOptions},
		{"options/unclean", opts([]string{"/" + secret + "/../x"}), redact.ErrInvalidOptions},
		{"screen/escape", screenErr(redact.Screen{Rows: []string{secret + "\x1b[2K"}}), redact.ErrInvalidInput},
		{"screen/tab", screenErr(redact.Screen{Rows: []string{secret + "\ta"}}), redact.ErrInvalidInput},
		{"screen/newline", screenErr(redact.Screen{Rows: []string{secret + "\nb"}}), redact.ErrInvalidInput},
		{"screen/c1", screenErr(redact.Screen{Rows: []string{"a\u0085" + secret}}), redact.ErrInvalidInput},
		{"screen/invalid utf8", screenErr(redact.Screen{Rows: []string{"a\xff" + secret}}), redact.ErrInvalidInput},
		{"screen/negative cols", screenErr(redact.Screen{Rows: []string{secret}, Cols: -1}), redact.ErrInvalidInput},
		{"screen/cols over max", screenErr(redact.Screen{Rows: []string{secret}, Cols: 10001}), redact.ErrInvalidInput},
		{"screen/too many rows", screenErr(redact.Screen{Rows: rows}), redact.ErrInputTooLarge},
		{"screen/too many bytes", screenErr(redact.Screen{Rows: []string{strings.Repeat(secret, 1<<15), strings.Repeat(secret, 1<<15)}}), redact.ErrInputTooLarge},
		{"screen/valid zero", screenErr(redact.Screen{Rows: []string{secret}, Cols: 80}), redact.ErrInternal},
		{"text/escape", textErr("a\x1b" + secret), redact.ErrInvalidInput},
		{"text/nul", textErr(secret + "\x00"), redact.ErrInvalidInput},
		{"text/invalid utf8", textErr("a\xff" + secret), redact.ErrInvalidInput},
		{"text/too many bytes", textErr(strings.Repeat(secret, (1<<20)/len(secret)+1)), redact.ErrInputTooLarge},
		{"text/valid zero", textErr(secret), redact.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tc.err, tc.want)
			for _, w := range windows {
				require.NotContains(t, tc.err.Error(), w)
			}
		})
	}
}
