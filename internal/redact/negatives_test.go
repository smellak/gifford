package redact_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestNegatives pins the §6.7 false-positive policy: readable shapes pass
// through identical with zero counts.
func TestNegatives(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	sha := mustValue(t, "git_sha")
	uuid := mustValue(t, "uuid")
	hex := mustValue(t, "sha256_hex")
	b64 := mustValue(t, "sha256_b64")
	lines := strings.Split(mustValue(t, "cert_body"), "\n")
	require.Len(t, lines, 3)
	aws := mustValue(t, "aws_access_key_id")

	inputs := []string{
		"commit " + sha,
		"session " + uuid,
		"image digest sha256:" + hex,
		"integrity sha512-" + b64,
		strings.Join([]string{testMarker("BEGIN", "CERTIFICATE"), lines[0], lines[1], lines[2], testMarker("END", "CERTIFICATE")}, "\n"),
		"ssh-ed25519 " + b64 + " dev@example.invalid",
		`"public_key": "ssh-ed25519 ` + b64 + `"`,
		"NODE_ENV=production",
		"AWS_REGION=eu-west-1",
		"TOKENIZERS_PARALLELISM=false",
		"max_tokens: 4096",
		"PWD=/home/user/src",
		"OLDPWD=/tmp",
		"- key: app",
		"primary_key = id",
		"PASSWORD=********",
		`token := os.Getenv("GITHUB_TOKEN")`,
		`if password == "" {`,
		"apiKey: string;",
		"token: str | None = None",
		`api_key = os.environ["API_KEY"]`,
		"token = get_token()",
		"password: ${DB_PASSWORD}",
		"password: <redacted>",
		"https://example.invalid:8443/v1/models",
		"ssh://git@example.invalid/acme/app.git",
		"git@example.invalid:acme/app.git",
		"postgres://app@db.example.invalid/app",
		"pip install sk" + "-learn-contrib-extension-tools",
		"the Authorization header is missing",
		"Authorization: Bearer",
		"proxy-authorization: basic",
		`cfg = 'Authorization: ' + token`,
		`cfg = "Authorization: " + token`,
		`"Authorization": "Bearer " + token`,
		`{'Authorization': 'Bearer ' + token}`,
		`"Authorization": ""`,
		`req.Header.Set("Authorization", "Bearer "+tok)`,
		"eyJhbGciOiJIUzI1NiJ9 is only a header",
		"X" + aws,
		"/srv/clients/northwind-archive/README.md",
		"/mnt/srv/clients/northwind/x",
		"/srv/clients/northwindx",
		"• Ran go test ./...",
	}
	for i, input := range inputs {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := r.RedactText(input)
			require.NoError(t, err, "case %d", i)
			require.Equal(t, input, got.Text, "case %d", i)
			require.Equal(t, redact.Counts{}, got.Counts, "case %d", i)
		})
	}

	// Empty bare values report no finding: an operator with no value run
	// passes through identical with zero counts instead of failing on an
	// empty span.
	t.Run("empty values", func(t *testing.T) {
		t.Parallel()
		for _, input := range []string{
			";Password=;",
			"Server=db;Database=app;Password=;",
			"PASSWORD=)",
			"TOKEN=]",
			"token: }",
			"PASSWORD=,x",
			"PASSWORD=\r\nnext",
			"PASSWORD=\u00a0x",
			"PASSWORD=\u3000x",
			"PASSWORD=",
			"PASSWORD= ",
		} {
			t.Run(fmt.Sprintf("text %q", input), func(t *testing.T) {
				t.Parallel()
				got, err := r.RedactText(input)
				require.NoError(t, err)
				require.Equal(t, input, got.Text)
				require.Equal(t, redact.Counts{}, got.Counts)
			})
		}
		t.Run("framed screen", func(t *testing.T) {
			t.Parallel()
			rows := []string{"\u2502 export DB_PASSWORD=          \u2502"}
			got, err := r.RedactScreen(redact.Screen{Rows: rows})
			require.NoError(t, err)
			require.Equal(t, rows, got.Rows)
			require.Equal(t, redact.Counts{}, got.Counts)
		})
	})
}
