package redact_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// requireAuth redacts input, expecting the scheme visible, the credential
// masked, one authorization substitution and a no-op second pass.
func requireAuth(t *testing.T, r *redact.Redactor, input, credential string) {
	t.Helper()
	got, err := r.RedactText(input)
	require.NoError(t, err)
	require.Equal(t, strings.Replace(input, credential, dots(credential), 1), got.Text)
	require.Equal(t, redact.PolicyVersion, got.PolicyVersion)
	require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, got.Counts)
	again, err := r.RedactText(got.Text)
	require.NoError(t, err)
	require.Equal(t, got.Text, again.Text)
	require.Equal(t, redact.Counts{}, again.Counts)
}

// TestAuthorization covers the credential-to-value-end rule with the
// scheme left visible: digest plain and JSON-escaped, OAuth, Signature,
// shell-quoted basics, proxy names, expired notes, unknown schemes,
// equals separators, unterminated values and bare words.
func TestAuthorization(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	t.Run("digest", func(t *testing.T) {
		t.Parallel()
		params := mustValue(t, "digest_params")
		requireAuth(t, r,
			"Authorization: Digest "+params,
			params)
	})

	t.Run("digest json", func(t *testing.T) {
		t.Parallel()
		params := mustValue(t, "digest_params_json")
		requireAuth(t, r,
			`{"request": {"headers": {"Authorization": "Digest `+params+`"}}}`,
			params)
	})

	t.Run("oauth", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		requireAuth(t, r, "Authorization: OAuth "+token, token)
	})

	t.Run("signature", func(t *testing.T) {
		t.Parallel()
		sig := mustValue(t, "hex64")
		credential := `keyId="app",signature="` + sig + `"`
		requireAuth(t, r, "Authorization: Signature "+credential, credential)
	})

	t.Run("basic single quoted", func(t *testing.T) {
		t.Parallel()
		creds := mustValue(t, "basic_credentials")
		requireAuth(t, r, "-H 'Authorization: Basic "+creds+"'", creds)
	})

	t.Run("bearer double quoted", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "jwt")
		requireAuth(t, r, `-H "Authorization: Bearer `+token+`"`, token)
	})

	t.Run("token json", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		requireAuth(t, r, `{"Authorization": "token `+token+`"}`, token)
	})

	t.Run("proxy basic", func(t *testing.T) {
		t.Parallel()
		creds := mustValue(t, "basic_credentials")
		requireAuth(t, r, "proxy-authorization: basic "+creds, creds)
	})

	t.Run("expired note", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "jwt")
		credential := token + " (expired)"
		requireAuth(t, r, "Authorization: Bearer "+credential, credential)
	})

	t.Run("unknown scheme", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		credential := "Frobnicate " + token
		got, err := r.RedactText("Authorization: " + credential)
		require.NoError(t, err)
		require.Equal(t, "Authorization: "+dots(credential), got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, got.Counts)
	})

	t.Run("equals separator", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		requireAuth(t, r, "authorization="+token, token)
	})

	t.Run("unterminated", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		requireAuth(t, r, "Authorization: 'Bearer "+token, token)
	})

	t.Run("tab after scheme", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		cases := []struct {
			name  string
			input string
		}{
			{"bare tab", "Authorization: Bearer\t" + token},
			{"bare space tab", "Authorization: Bearer \t" + token},
			{"bare tab space tab", "Authorization: Bearer\t \t" + token},
			{"quoted tab", "Authorization: \"Bearer\t" + token + "\""},
			{"quoted space tab", "Authorization: \"Bearer \t" + token + "\""},
			{"quoted tab space tab", "Authorization: \"Bearer\t \t" + token + "\""},
			{"enclosed tab", "x \"Authorization: Bearer\t" + token + "\""},
			{"enclosed space tab", "x \"Authorization: Bearer \t" + token + "\""},
			{"enclosed tab space tab", "x \"Authorization: Bearer\t \t" + token + "\""},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireAuth(t, r, tc.input, token)
			})
		}
	})

	t.Run("tab ends quoted credential", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		for _, tc := range []struct {
			name  string
			input string
		}{
			{"quoted", `Authorization: "Bearer ` + token + "\tvisible" + `"`},
			{"enclosed", `x "Authorization: Bearer ` + token + "\tvisible" + `"`},
			{"quoted without scheme", `Authorization: "` + token + "\tvisible" + `"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireAuth(t, r, tc.input, token)
			})
		}
	})

	t.Run("enclosed different quote opens value", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		input := `'Authorization: "Bearer ` + token + `"'`
		requireAuth(t, r, input, token)
		screen, err := r.RedactScreen(redact.Screen{Rows: []string{input}})
		require.NoError(t, err)
		require.Equal(t, []string{`'Authorization: "Bearer ` + dots(token) + `"'`}, screen.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, screen.Counts)
		second, err := r.RedactScreen(redact.Screen{Rows: screen.Rows})
		require.NoError(t, err)
		require.Equal(t, screen.Rows, second.Rows)
		require.Equal(t, redact.Counts{}, second.Counts)
	})

	t.Run("enclosed escaped quote runs on", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		credential := `\"` + token + `\"`
		input := `x "Authorization: ` + credential + `" y`
		requireAuth(t, r, input, credential)
		screen, err := r.RedactScreen(redact.Screen{Rows: []string{input}})
		require.NoError(t, err)
		require.Equal(t, []string{`x "Authorization: ` + dots(credential) + `" y`}, screen.Rows)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, screen.Counts)
		second, err := r.RedactScreen(redact.Screen{Rows: screen.Rows})
		require.NoError(t, err)
		require.Equal(t, screen.Rows, second.Rows)
		require.Equal(t, redact.Counts{}, second.Counts)
	})

	t.Run("bare quote", func(t *testing.T) {
		t.Parallel()
		requireAuth(t, r, `Authorization: Digest"`, `"`)
	})

	t.Run("enclosed value starting with backslash", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		credential := `\"` + token + `\" end`
		requireAuth(t, r, `x "Authorization: `+credential+`"`, credential)
	})

	t.Run("enclosed value starting with frame", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "hex64")
		input := "'Authorization: \u2502" + token
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, input, got.Text)
		require.Equal(t, redact.Counts{}, got.Counts)
		again, err := r.RedactText(got.Text)
		require.NoError(t, err)
		require.Equal(t, got.Text, again.Text)
		require.Equal(t, redact.Counts{}, again.Counts)
	})

	t.Run("enclosed empty value then CR", func(t *testing.T) {
		t.Parallel()
		input := "'Authorization: \r\nnext"
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, input, got.Text)
		require.Equal(t, redact.Counts{}, got.Counts)
		again, err := r.RedactText(got.Text)
		require.NoError(t, err)
		require.Equal(t, got.Text, again.Text)
		require.Equal(t, redact.Counts{}, again.Counts)
	})
}
