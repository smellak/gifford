package redacttest_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact/redacttest"
)

// marker builds a PEM marker from parts, the same way values.go does, so
// no test line holds a whole marker either.
func marker(word, label string) string {
	return strings.Repeat("-", 5) + word + " " + label + strings.Repeat("-", 5)
}

// TestValues pins the synthetic values: deterministic per name, shaped as
// the §9.2 table documents, with unknown names refused.
func TestValues(t *testing.T) {
	t.Parallel()

	lengths := []struct {
		name string
		want int
	}{
		{"aws_access_key_id", 20},
		{"aws_secret_access_key", 40},
		{"github_pat", 40},
		{"github_fine_grained", 93},
		{"gitlab_pat", 26},
		{"slack_bot", 56},
		{"stripe_live", 32},
		{"openai_project", 164},
		{"anthropic", 108},
		{"google_api", 39},
		{"huggingface", 37},
		{"npm", 40},
		{"pypi", 80},
		{"sendgrid", 69},
		{"jwt", 147},
		{"db_userinfo", 27},
		{"x_access_userinfo", 55},
		{"basic_credentials", 32},
		{"hex64", 64},
		{"cookie_session", 40},
		{"digest_params", 84},
		{"digest_params_json", 90},
		{"password_spaces", 24},
		{"git_sha", 40},
		{"sha256_hex", 64},
		{"sha256_b64", 44},
	}
	multi := []string{"pem_openssh", "pem_pkcs8_inline", "uuid", "cert_body"}

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()
		for _, tc := range lengths {
			first, err := redacttest.Value(tc.name)
			require.NoError(t, err, "value %s", tc.name)
			second, err := redacttest.Value(tc.name)
			require.NoError(t, err, "value %s", tc.name)
			require.Equal(t, first, second, "value %s", tc.name)
		}
		for _, name := range multi {
			first, err := redacttest.Value(name)
			require.NoError(t, err, "value %s", name)
			second, err := redacttest.Value(name)
			require.NoError(t, err, "value %s", name)
			require.Equal(t, first, second, "value %s", name)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		t.Parallel()
		_, err := redacttest.Value("no_such_value")
		require.ErrorIs(t, err, redacttest.ErrUnknownValue)
		require.NotContains(t, err.Error(), "srv")
	})

	t.Run("lengths", func(t *testing.T) {
		t.Parallel()
		for _, tc := range lengths {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				v, err := redacttest.Value(tc.name)
				require.NoError(t, err)
				require.Len(t, v, tc.want, "value %s", tc.name)
			})
		}
	})

	t.Run("shapes", func(t *testing.T) {
		t.Parallel()
		prefixed := map[string]string{
			"aws_access_key_id":   "AKIA",
			"github_pat":          "ghp_",
			"github_fine_grained": "github_pat_",
			"gitlab_pat":          "glpat-",
			"slack_bot":           "xoxb-",
			"stripe_live":         "sk_live_",
			"openai_project":      "sk-proj-",
			"anthropic":           "sk-ant-api03-",
			"google_api":          "AIza",
			"huggingface":         "hf_",
			"npm":                 "npm_",
			"pypi":                "pypi-AgEIcHlwaS5vcmc",
			"sendgrid":            "SG.",
			"jwt":                 "eyJ",
		}
		for name, prefix := range prefixed {
			v, err := redacttest.Value(name)
			require.NoError(t, err, "value %s", name)
			require.True(t, strings.HasPrefix(v, prefix), "value %s", name)
		}
	})

	t.Run("jwt parts", func(t *testing.T) {
		t.Parallel()
		v, err := redacttest.Value("jwt")
		require.NoError(t, err)
		parts := strings.Split(v, ".")
		require.Len(t, parts, 3)
		head, err := base64.RawURLEncoding.DecodeString(parts[0])
		require.NoError(t, err)
		require.JSONEq(t, `{"alg":"HS256","typ":"JWT"}`, string(head))
		require.True(t, strings.HasPrefix(parts[1], "eyJ"))
	})

	t.Run("digest pair", func(t *testing.T) {
		t.Parallel()
		plain, err := redacttest.Value("digest_params")
		require.NoError(t, err)
		escaped, err := redacttest.Value("digest_params_json")
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(plain, `"`, `\"`), escaped)
	})

	t.Run("pem openssh", func(t *testing.T) {
		t.Parallel()
		v, err := redacttest.Value("pem_openssh")
		require.NoError(t, err)
		lines := strings.Split(v, "\n")
		require.Len(t, lines, 7)
		require.Equal(t, marker("BEGIN", "OPENSSH PRIVATE KEY"), lines[0])
		require.Equal(t, marker("END", "OPENSSH PRIVATE KEY"), lines[6])
		for i, want := range []int{70, 70, 70, 70, 34} {
			require.Len(t, lines[i+1], want, "body line %d", i)
		}
	})

	t.Run("pem pkcs8 inline", func(t *testing.T) {
		t.Parallel()
		v, err := redacttest.Value("pem_pkcs8_inline")
		require.NoError(t, err)
		require.NotContains(t, v, "\n")
		require.Equal(t, 4, strings.Count(v, `\n`))
		require.True(t, strings.HasPrefix(v, marker("BEGIN", "PRIVATE KEY")))
	})

	t.Run("password spaces", func(t *testing.T) {
		t.Parallel()
		v, err := redacttest.Value("password_spaces")
		require.NoError(t, err)
		words := strings.Split(v, " ")
		require.Len(t, words, 3)
		require.Len(t, words[0], 7)
		require.Len(t, words[1], 6)
		require.Len(t, words[2], 9)
	})

	t.Run("keep values", func(t *testing.T) {
		t.Parallel()
		uuid, err := redacttest.Value("uuid")
		require.NoError(t, err)
		require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, uuid)
		cert, err := redacttest.Value("cert_body")
		require.NoError(t, err)
		require.Len(t, strings.Split(cert, "\n"), 3)
		require.True(t, strings.HasSuffix(cert, "=="))
	})

	t.Run("options", func(t *testing.T) {
		t.Parallel()
		opts := redacttest.Options()
		require.Equal(t, []string{"/srv/clients/northwind", "~/work/clients/acme"}, opts.SensitivePaths)
		require.NoError(t, opts.Validate())
	})
}
