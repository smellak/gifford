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
			{"yaml client", `client_secret: "` + mustValue(t, "hex64") + `"`, mustValue(t, "hex64")},
			{"semicolons", ";Password=" + mustValue(t, "hex64") + ";", mustValue(t, "hex64")},
			{"query", "https://example.invalid/search?api_key=" + mustValue(t, "hex64") + "&page=2", mustValue(t, "hex64")},
			{"flag", "--auth-token=" + mustValue(t, "hex64"), mustValue(t, "hex64")},
			{"header", "X-Api-Key: " + mustValue(t, "hex64"), mustValue(t, "hex64")},
			{"quoted spaces", `"api_key": "alpha beta gamma"`, "alpha beta gamma"},
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
			mustValue(t, "sensitive_contract"),
			mustValue(t, "sensitive_contract"), redact.KindSensitivePath)
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
				truncNote,
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{dots(lines[0]), dots(body), dots(body), truncNote}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
		})
		t.Run("inline twice on one line", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			inl := testMarker("BEGIN", "PRIVATE KEY") + `\n` + body + `\n` + body + `\n` + testMarker("END", "PRIVATE KEY")
			input := `{"keys": ["` + inl + `", "` + inl + `"]}`
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := `{"keys": ["` + dots(inl) + `", "` + dots(inl) + `"]}`
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 2}}, got.Counts)
			again, err := r.RedactText(got.Text)
			require.NoError(t, err)
			require.Equal(t, got.Text, again.Text)
			require.Equal(t, redact.Counts{}, again.Counts)
		})
		t.Run("end and begin on one line", func(t *testing.T) {
			t.Parallel()
			body := mustValue(t, "hex64")
			lines := []string{
				testMarker("BEGIN", "RSA PRIVATE KEY"),
				body,
				testMarker("END", "RSA PRIVATE KEY") + testMarker("BEGIN", "RSA PRIVATE KEY"),
				body,
				body,
				truncNote,
			}
			input := strings.Join(lines, "\n")
			got, err := r.RedactText(input)
			require.NoError(t, err)
			want := strings.Join([]string{
				dots(lines[0]),
				dots(body),
				dots(lines[2]),
				dots(body),
				dots(body),
				truncNote,
			}, "\n")
			require.Equal(t, want, got.Text)
			require.Equal(t, redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 2}}, got.Counts)
			again, err := r.RedactText(got.Text)
			require.NoError(t, err)
			require.Equal(t, got.Text, again.Text)
			require.Equal(t, redact.Counts{}, again.Counts)
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
		t.Run("orphan end stops at prior block", func(t *testing.T) {
			t.Parallel()
			end := testMarker("END", "RSA PRIVATE KEY")
			begin := testMarker("BEGIN", "RSA PRIVATE KEY")
			for _, tc := range []struct {
				name string
				rows []string
				want []string
			}{
				{
					"same-line prefix before paired block",
					[]string{"visiblePrefix " + begin + end + " body " + end},
					[]string{"visiblePrefix " + dots(begin+end) + " " + dots("body") + " " + dots(end)},
				},
				{
					"earlier row before same-line paired block",
					[]string{"visibleEarlierRow", begin + end + " " + end},
					[]string{"visibleEarlierRow", dots(begin+end) + " " + dots(end)},
				},
			} {
				wantCounts := redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 2}}
				got, err := r.RedactText(strings.Join(tc.rows, "\n"))
				require.NoError(t, err, tc.name)
				require.Equal(t, strings.Join(tc.want, "\n"), got.Text, tc.name)
				require.Equal(t, wantCounts, got.Counts, tc.name)
				again, err := r.RedactText(got.Text)
				require.NoError(t, err, tc.name)
				require.Equal(t, got.Text, again.Text, tc.name)
				require.Equal(t, redact.Counts{}, again.Counts, tc.name)
				screen, err := r.RedactScreen(redact.Screen{Rows: tc.rows})
				require.NoError(t, err, tc.name)
				require.Equal(t, tc.want, screen.Rows, tc.name)
				require.Equal(t, wantCounts, screen.Counts, tc.name)
				second, err := r.RedactScreen(redact.Screen{Rows: screen.Rows})
				require.NoError(t, err, tc.name)
				require.Equal(t, screen.Rows, second.Rows, tc.name)
				require.Equal(t, redact.Counts{}, second.Counts, tc.name)
			}
		})
		t.Run("orphan controls", func(t *testing.T) {
			t.Parallel()
			end := testMarker("END", "RSA PRIVATE KEY")
			begin := testMarker("BEGIN", "RSA PRIVATE KEY")
			for _, tc := range []struct {
				name   string
				rows   []string
				want   []string
				counts redact.Counts
			}{
				{
					"orphan after orphan",
					[]string{end + " " + end + " " + end},
					[]string{dots(end) + " " + dots(end) + " " + dots(end)},
					redact.Counts{Total: 3, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 3}},
				},
				{
					"walk-up to a block on an earlier line",
					[]string{begin + end, "body " + end},
					[]string{dots(begin + end), dots("body") + " " + dots(end)},
					redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 2}},
				},
			} {
				got, err := r.RedactText(strings.Join(tc.rows, "\n"))
				require.NoError(t, err, tc.name)
				require.Equal(t, strings.Join(tc.want, "\n"), got.Text, tc.name)
				require.Equal(t, tc.counts, got.Counts, tc.name)
				again, err := r.RedactText(got.Text)
				require.NoError(t, err, tc.name)
				require.Equal(t, got.Text, again.Text, tc.name)
				require.Equal(t, redact.Counts{}, again.Counts, tc.name)
				screen, err := r.RedactScreen(redact.Screen{Rows: tc.rows})
				require.NoError(t, err, tc.name)
				require.Equal(t, tc.want, screen.Rows, tc.name)
				require.Equal(t, tc.counts, screen.Counts, tc.name)
				second, err := r.RedactScreen(redact.Screen{Rows: screen.Rows})
				require.NoError(t, err, tc.name)
				require.Equal(t, screen.Rows, second.Rows, tc.name)
				require.Equal(t, redact.Counts{}, second.Counts, tc.name)
			}
		})
		t.Run("orphan ends on one line", func(t *testing.T) {
			t.Parallel()
			end := testMarker("END", "RSA PRIVATE KEY")
			begin := testMarker("BEGIN", "RSA PRIVATE KEY")
			for _, tc := range []struct{ input, want string }{
				{end + " " + end, dots(end) + " " + dots(end)},
				{begin + end + " " + end, dots(begin+end) + " " + dots(end)},
			} {
				wantCounts := redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 2}}
				got, err := r.RedactText(tc.input)
				require.NoError(t, err, "input %q", tc.input)
				require.Equal(t, tc.want, got.Text, "input %q", tc.input)
				require.Equal(t, wantCounts, got.Counts, "input %q", tc.input)
				again, err := r.RedactText(got.Text)
				require.NoError(t, err, "input %q", tc.input)
				require.Equal(t, got.Text, again.Text, "input %q", tc.input)
				require.Equal(t, redact.Counts{}, again.Counts, "input %q", tc.input)
				screen, err := r.RedactScreen(redact.Screen{Rows: []string{tc.input}})
				require.NoError(t, err, "input %q", tc.input)
				require.Equal(t, []string{tc.want}, screen.Rows, "input %q", tc.input)
				require.Equal(t, wantCounts, screen.Counts, "input %q", tc.input)
				second, err := r.RedactScreen(redact.Screen{Rows: screen.Rows})
				require.NoError(t, err, "input %q", tc.input)
				require.Equal(t, screen.Rows, second.Rows, "input %q", tc.input)
				require.Equal(t, redact.Counts{}, second.Counts, "input %q", tc.input)
			}
		})
	})
}

// truncNote is the TUI truncation line surviving an open block.
const truncNote = "… +3 lines"

// testMarker builds a PEM marker from parts, so no test line holds a
// whole marker.
func testMarker(word, label string) string {
	return strings.Repeat("-", 5) + word + " " + label + strings.Repeat("-", 5)
}
