package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// clusterRunes holds both ends of every §5.1 cluster-forming range.
func clusterRunes() []rune {
	return []rune{
		0x200d,
		0xfe00, 0xfe0f,
		0xe0100, 0xe01ef,
		0x1f1e6, 0x1f1ff,
		0x1f3fb, 0x1f3ff,
		0xe0020, 0xe007f,
		0x20e3,
	}
}

// windows returns every 4-rune window of s for the no-leak check.
func windows(s string) []string {
	runes := []rune(s)
	var out []string
	for i := 0; i+4 <= len(runes); i++ {
		out = append(out, string(runes[i:i+4]))
	}
	return out
}

// TestUnsupportedWidth pins the fail-closed width domain: a cluster-forming
// rune inside a masked span, a joiner right before it, or an extender right
// after it fails both methods with the zero result and no input in the
// error. Elsewhere the same runes pass through untouched.
func TestUnsupportedWidth(t *testing.T) {
	t.Parallel()
	r := mustNew(t)
	hex := mustValue(t, "hex64")

	t.Run("cluster inside", func(t *testing.T) {
		t.Parallel()
		for _, cr := range clusterRunes() {
			secret := hex[:32] + string(cr) + hex[32:]
			input := "PASSWORD=" + secret
			text, err := r.RedactText(input)
			require.ErrorIs(t, err, redact.ErrUnsupportedInput, "rune U+%04X", cr)
			require.Equal(t, redact.TextResult{}, text, "rune U+%04X", cr)
			for _, w := range windows(hex) {
				require.NotContains(t, err.Error(), w, "rune U+%04X", cr)
			}
			screen, err := r.RedactScreen(redact.Screen{Rows: []string{input}})
			require.ErrorIs(t, err, redact.ErrUnsupportedInput, "rune U+%04X", cr)
			require.Equal(t, redact.ScreenResult{}, screen, "rune U+%04X", cr)
		}
	})

	t.Run("cluster outside", func(t *testing.T) {
		t.Parallel()
		for _, cr := range clusterRunes() {
			input := "note " + string(cr) + " end"
			got, err := r.RedactText(input)
			require.NoError(t, err, "rune U+%04X", cr)
			require.Equal(t, input, got.Text, "rune U+%04X", cr)
			require.Equal(t, redact.Counts{}, got.Counts, "rune U+%04X", cr)
		}
	})

	t.Run("token edges", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "github_pat")
		t.Run("extenders after", func(t *testing.T) {
			t.Parallel()
			for _, tail := range []string{"\uFE0F\u20E3", "\U0001F3FB", "\u200D"} {
				got, err := r.RedactText(token + tail)
				require.ErrorIs(t, err, redact.ErrUnsupportedInput, "tail %q", tail)
				require.Equal(t, redact.TextResult{}, got, "tail %q", tail)
			}
		})
		t.Run("joiner before", func(t *testing.T) {
			t.Parallel()
			got, err := r.RedactText("\u200d" + token)
			require.ErrorIs(t, err, redact.ErrUnsupportedInput)
			require.Equal(t, redact.TextResult{}, got)
		})
		t.Run("space before keycap", func(t *testing.T) {
			t.Parallel()
			input := token + " \u20E3"
			got, err := r.RedactText(input)
			require.NoError(t, err)
			require.Equal(t, dots(token)+" \u20E3", got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}, got.Counts)
		})
		t.Run("flag kept", func(t *testing.T) {
			t.Parallel()
			flag := string([]rune{0x1f1e6, 0x1f1f8})
			input := flag + token + flag
			got, err := r.RedactText(input)
			require.NoError(t, err)
			require.Equal(t, flag+dots(token)+flag, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}, got.Counts)
		})
	})

	t.Run("mark absorbed", func(t *testing.T) {
		t.Parallel()
		input := "PASSWORD=a\u0301bc"
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, "PASSWORD="+dots("a\u0301bc"), got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
		require.Equal(t, redact.CellWidth(input), redact.CellWidth(got.Text))
	})
}
