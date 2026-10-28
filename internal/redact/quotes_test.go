package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestQuotes pins the §6.4 quote rules: a double quote closes only after
// an even run of backslashes, single quotes join on doubling without
// escapes, and an unterminated value runs to the line end.
func TestQuotes(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	t.Run("escaped", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`"password": "abc\"def tail",`)
		require.NoError(t, err)
		require.Equal(t, `"password": "`+dots(`abc\"def tail`)+`",`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("even closes", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`"password": "abc\\" tail",`)
		require.NoError(t, err)
		require.Equal(t, `"password": "`+dots(`abc\\`)+`" tail",`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("odd escapes", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`"password": "abc\\\"def"`)
		require.NoError(t, err)
		require.Equal(t, `"password": "`+dots(`abc\\\"def`)+`"`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("unterminated", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`"password": "abc\"def`)
		require.NoError(t, err)
		require.Equal(t, `"password": "`+dots(`abc\"def`), got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("single doubled", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`'password': 'it''s-a-secret',`)
		require.NoError(t, err)
		require.Equal(t, `'password': '`+dots(`it''s-a-secret`)+`',`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("single backslash", func(t *testing.T) {
		t.Parallel()
		got, err := r.RedactText(`'password': 'abc\',`)
		require.NoError(t, err)
		require.Equal(t, `'password': '`+dots(`abc\`)+`',`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSecretAssignment: 1}}, got.Counts)
	})

	t.Run("auth escaped", func(t *testing.T) {
		t.Parallel()
		creds := mustValue(t, "basic_credentials")
		input := `{"Authorization": "Bearer ` + creds + `\"def"}`
		value := creds + `\"def`
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, `{"Authorization": "Bearer `+dots(value)+`"}`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, got.Counts)
	})
}
