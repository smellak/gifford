package redact_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// mustNew builds the v1 redactor over the fixture paths, failing the test
// on any error.
func mustNew(t *testing.T) *redact.Redactor {
	t.Helper()
	r, err := redact.New(redacttest.Options())
	require.NoError(t, err)
	return r
}

// mustValue assembles one fixture value, failing the test on any error.
func mustValue(t *testing.T, name string) string {
	t.Helper()
	v, err := redacttest.Value(name)
	require.NoError(t, err)
	return v
}

// dots returns one placeholder per input cell.
func dots(secret string) string {
	return strings.Repeat("•", redact.CellWidth(secret))
}

// requireOne redacts input, expecting exactly the secret masked with the
// policy version and one substitution of kind, and a no-op second pass.
func requireOne(t *testing.T, r *redact.Redactor, input, secret string, kind redact.Kind) {
	t.Helper()
	got, err := r.RedactText(input)
	require.NoError(t, err)
	require.Equal(t, strings.Replace(input, secret, dots(secret), 1), got.Text)
	require.Equal(t, redact.PolicyVersion, got.PolicyVersion)
	require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{kind: 1}}, got.Counts)
	again, err := r.RedactText(got.Text)
	require.NoError(t, err)
	require.Equal(t, got.Text, again.Text)
	require.Equal(t, redact.Counts{}, again.Counts)
}

// TestRules covers one positive per §6 pattern: the twelve vendor tokens,
// two credential URLs, the assignment forms, three path forms and the key
// block forms.
func TestRules(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	t.Run("api keys", func(t *testing.T) {
		t.Parallel()
		for _, name := range []string{
			"aws_access_key_id",
			"github_pat",
			"github_fine_grained",
			"gitlab_pat",
			"slack_bot",
			"stripe_live",
			"openai_project",
			"anthropic",
			"google_api",
			"huggingface",
			"npm",
			"pypi",
			"sendgrid",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				secret := mustValue(t, name)
				requireOne(t, r, "v="+secret, secret, redact.KindAPIKey)
			})
		}
	})

	t.Run("connection strings", func(t *testing.T) {
		t.Parallel()
		user := mustValue(t, "db_userinfo")
		requireOne(t, r,
			"MONGODB_URI=mongodb+srv://"+user+"@db.example.invalid/app",
			user, redact.KindConnectionString)
		hex := mustValue(t, "hex64")
		requireOne(t, r,
			"REDIS_URL=redis://:"+hex+"@cache.example.invalid:6379/0",
			":"+hex, redact.KindConnectionString)
	})

	t.Run("assignments", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name   string
			input  string
			secret string
		}{
			{"export", "export DB_PASSWORD=" + mustValue(t, "hex64"), mustValue(t, "hex64")},
			{"yaml", `client_secret: "` + mustValue(t, "password_spaces") + `"`, mustValue(t, "password_spaces")},
			{"semicolons", ";Password=" + mustValue(t, "hex64") + ";", mustValue(t, "hex64")},
			{"query", "https://example.invalid/search?api_key=" + mustValue(t, "hex64") + "&page=2", mustValue(t, "hex64")},
			{"flag", "--auth-token=" + mustValue(t, "hex64"), mustValue(t, "hex64")},
			{"header", "X-Api-Key: " + mustValue(t, "hex64"), mustValue(t, "hex64")},
			{"backtick", "`DB_PASSWORD=" + mustValue(t, "hex64") + "` x", mustValue(t, "hex64")},
			{"camel", "accessToken: " + mustValue(t, "cookie_session"), mustValue(t, "cookie_session")},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				requireOne(t, r, tc.input, tc.secret, redact.KindSecretAssignment)
			})
		}
	})

	t.Run("paths", func(t *testing.T) {
		t.Parallel()
		contract := mustValue(t, "sensitive_contract")
		requireOne(t, r, "Read("+contract+")", contract, redact.KindSensitivePath)
		requireOne(t, r,
			"cat /srv/clients/northwind/a.md:12:5",
			"/srv/clients/northwind/a.md:12:5", redact.KindSensitivePath)
		requireOne(t, r,
			"Read("+mustValue(t, "acme_env")+")",
			mustValue(t, "acme_env"), redact.KindSensitivePath)
		requireOne(t, r,
			"`"+mustValue(t, "sensitive_contract")+"/a.md` x",
			mustValue(t, "sensitive_contract")+"/a.md", redact.KindSensitivePath)
	})

	t.Run("private keys", func(t *testing.T) {
		t.Parallel()
		t.Run("inline", func(t *testing.T) {
			t.Parallel()
			key := mustValue(t, "pem_pkcs8_inline")
			requireOne(t, r, `{"private_key": "`+key+`"}`, key, redact.KindPrivateKey)
		})
		t.Run("block", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "RSA PRIVATE KEY"),
				body,
				body,
				testMarker("END", "RSA PRIVATE KEY"),
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{
				dots(lines[0]),
				dots(body),
				dots(body),
				dots(lines[3]),
			}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("line numbered", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "EC PRIVATE KEY"),
				"1 " + body,
				"2 " + body,
				testMarker("END", "EC PRIVATE KEY"),
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{
				dots(lines[0]),
				"• " + dots(body),
				"• " + dots(body),
				dots(lines[3]),
			}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("pgp block", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "PGP PRIVATE KEY BLOCK"),
				body,
				testMarker("END", "PGP PRIVATE KEY BLOCK"),
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{dots(lines[0]), dots(body), dots(lines[2])}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("encrypted headers", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "RSA PRIVATE KEY"),
				"Proc-Type: 4,ENCRYPTED",
				"DEK-Info: AES-128-CBC,0123456789ABCDEF",
				"",
				body,
				testMarker("END", "RSA PRIVATE KEY"),
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{
				dots(lines[0]),
				"••••-••••: •,•••••••••",
				"•••-••••: •••-•••-•••,••••••••••••••••",
				"",
				dots(body),
				dots(lines[5]),
			}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("truncated", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "RSA PRIVATE KEY"),
				body,
				body,
				"… +3 lines",
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{dots(lines[0]), dots(body), dots(body), "… +3 lines"}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("scrolled end", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				body,
				body,
				testMarker("END", "RSA PRIVATE KEY"),
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{dots(body), dots(body), dots(lines[2])}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
	})
}

// testMarker builds a PEM marker from parts, so no test line holds a
// whole marker.
func testMarker(word, label string) string {
	return strings.Repeat("-", 5) + word + " " + label + strings.Repeat("-", 5)
}
