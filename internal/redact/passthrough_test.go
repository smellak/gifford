package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestPassThrough pins the empty and clean inputs: empty results with
// version "2", and clean text passing through byte-identical.
func TestPassThrough(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	t.Run("empty text", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText("")
		require.NoError(t, err)
		require.Equal(t, redact.TextResult{Text: "", PolicyVersion: "2", Counts: redact.Counts{}}, got)
	})

	t.Run("empty screen", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactScreen(redact.Screen{})
		require.NoError(t, err)
		require.Empty(t, got.Rows)
		require.Equal(t, "2", got.PolicyVersion)
		require.Equal(t, redact.Counts{}, got.Counts)
	})

	t.Run("clean text", func(t *testing.T) {
		t.Parallel()
		for _, input := range []string{
			"a\tb",
			"a\r\nb",
			"密码强度很好",
			"deploy 📦 done 🎉",
			"flags 🇪🇸🇯🇵 ok",
			"plain ASCII with no secrets",
		} {
			got, err := r.RedactText(input)
			require.NoError(t, err, "input %q", input)
			require.Equal(t, input, got.Text, "input %q", input)
			require.Equal(t, "2", got.PolicyVersion, "input %q", input)
			require.Equal(t, redact.Counts{}, got.Counts, "input %q", input)
		}
	})

	t.Run("clean screen", func(t *testing.T) {
		t.Parallel()
		rows := []string{"plain row", "密码 row 📦"}
		got, err := r.RedactScreen(redact.Screen{Rows: rows, Cols: 80})
		require.NoError(t, err)
		require.Equal(t, rows, got.Rows)
		require.Equal(t, "2", got.PolicyVersion)
		require.Equal(t, redact.Counts{}, got.Counts)
	})
}
