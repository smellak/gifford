package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestWraps pins the §6.6 continuation across TUI hard breaks: a finding
// ending at its line's last non-blank rune joins the next line's usable
// head, and a cut literal prefix joins its head against the token
// patterns. Free text never joins.
func TestWraps(t *testing.T) {
	t.Parallel()

	t.Run("jwt soft join", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		token := mustValue(t, "jwt")
		rows := []string{">> " + token[:50], token[50:]}
		for _, cols := range []int{54, 0} {
			got, err := r.RedactScreen(redact.Screen{Rows: rows, Cols: cols})
			require.NoError(t, err, "cols %d", cols)
			require.Equal(t, []string{">> " + dots(token[:50]), dots(token[50:])}, got.Rows, "cols %d", cols)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindJWT: 1}}, got.Counts, "cols %d", cols)
		}
	})

	t.Run("continuation stops", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		secret := mustValue(t, "hex64")
		head := "PASSWORD=" + secret
		for _, next := range []string{"DEBUG=true", "Starting server on :8080"} {
			got, err := r.RedactScreen(redact.Screen{Rows: []string{head, next}})
			require.NoError(t, err, "next %q", next)
			require.Equal(t, []string{"PASSWORD=" + dots(secret), next}, got.Rows, "next %q", next)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts, "next %q", next)
		}
	})

	t.Run("continuation masks lone", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		secret := mustValue(t, "hex64")
		got, err := r.RedactScreen(redact.Screen{Rows: []string{"PASSWORD=" + secret, "Done"}})
		require.NoError(t, err)
		require.Equal(t, []string{"PASSWORD=" + dots(secret), "••••"}, got.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("userinfo wrap", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		user := mustValue(t, "db_userinfo")
		rows := []string{"postgres://app_rw:", user[7:] + "@db.example.invalid:5432/app"}
		got, err := r.RedactScreen(redact.Screen{Rows: rows, Cols: 18})
		require.NoError(t, err)
		require.Equal(t, []string{"postgres://" + dots("app_rw:"), dots(user[7:]) + "@db.example.invalid:5432/app"}, got.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindConnectionString: 1}}, got.Counts)
	})

	t.Run("frame keeps apart", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		user := mustValue(t, "db_userinfo")
		rows := []string{"postgres://app_rw:│", user[7:] + "@db.example.invalid:5432/app"}
		got, err := r.RedactScreen(redact.Screen{Rows: rows, Cols: 19})
		require.NoError(t, err)
		require.Equal(t, rows, got.Rows)
		require.Equal(t, redact.Counts{}, got.Counts)
	})

	t.Run("prefix join token", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		token := mustValue(t, "github_pat")
		rows := []string{"tok " + token[:20], "│ " + token[20:]}
		got, err := r.RedactScreen(redact.Screen{Rows: rows})
		require.NoError(t, err)
		require.Equal(t, []string{"tok " + dots(token[:20]), "│ " + dots(token[20:])}, got.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}, got.Counts)
	})

	t.Run("prefix join jwt", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		token := mustValue(t, "jwt")
		rows := []string{"tok " + token[:12], "│ " + token[12:]}
		got, err := r.RedactScreen(redact.Screen{Rows: rows})
		require.NoError(t, err)
		require.Equal(t, []string{"tok " + dots(token[:12]), "│ " + dots(token[12:])}, got.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindJWT: 1}}, got.Counts)
	})

	t.Run("short prefix unchanged", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		got, err := r.RedactScreen(redact.Screen{Rows: []string{"see sk-learn", "docs online"}})
		require.NoError(t, err)
		require.Equal(t, []string{"see sk-learn", "docs online"}, got.Rows)
		require.Equal(t, redact.Counts{}, got.Counts)
	})

	t.Run("text never joins", func(t *testing.T) {
		t.Parallel()
		r := mustNew(t)
		token := mustValue(t, "github_pat")
		input := "tok " + token[:20] + "\n" + token[20:]
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, input, got.Text)
		require.Equal(t, redact.Counts{}, got.Counts)
	})
}
