package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestOverlap pins the kind-priority counting: every byte of a finding is
// masked, but a finding counts for its kind only when no earlier finding
// masked any of its bytes.
func TestOverlap(t *testing.T) {
	t.Parallel()
	r := mustNew(t)

	t.Run("token in assignment", func(t *testing.T) {
		t.Parallel()
		token := mustValue(t, "github_pat")
		input := "GITHUB_TOKEN=" + token
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, "GITHUB_TOKEN="+dots(token), got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}, got.Counts)
	})

	t.Run("key in json", func(t *testing.T) {
		t.Parallel()
		key := mustValue(t, "pem_pkcs8_inline")
		input := `"private_key": "` + key + `"`
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, `"private_key": "`+dots(key)+`"`, got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1}}, got.Counts)
	})

	t.Run("two tokens", func(t *testing.T) {
		t.Parallel()
		first := mustValue(t, "github_pat")
		second := mustValue(t, "gitlab_pat")
		input := first + " " + second
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, dots(first)+" "+dots(second), got.Text)
		require.Equal(t, redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 2}}, got.Counts)
	})

	t.Run("aws signature", func(t *testing.T) {
		t.Parallel()
		key := mustValue(t, "aws_access_key_id")
		input := "Authorization: AWS4-HMAC-SHA256 Credential=" + key + "/20130524/us-east-1/s3/aws4_request"
		credential := "Credential=" + key + "/20130524/us-east-1/s3/aws4_request"
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, "Authorization: AWS4-HMAC-SHA256 "+dots(credential), got.Text)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAuthorization: 1}}, got.Counts)
	})

	t.Run("counts add", func(t *testing.T) {
		t.Parallel()
		a := redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}
		b := redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1, redact.KindJWT: 1}}
		sum := a.Add(b)
		require.Equal(t, redact.Counts{
			Total: 3,
			ByKind: map[redact.Kind]int64{
				redact.KindAPIKey: 2,
				redact.KindJWT:    1,
			},
		}, sum)
		require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1}}, a)
		require.Equal(t, redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{
			redact.KindAPIKey: 1,
			redact.KindJWT:    1,
		}}, b)
	})

	t.Run("unicode outside spans masks like ascii", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name  string
			paths []string
			input string
			want  string
		}{
			{"cluster outside", []string{"/a"}, "\u2714\uFE0F /a/a", "\u2714\uFE0F " + dots("/a/a")},
			{"marks absorbed", []string{"/a"}, "/a\u0301", dots("/a\u0301")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				rp := mustNewPaths(t, tc.paths...)
				got, err := rp.RedactText(tc.input)
				require.NoError(t, err)
				require.Equal(t, tc.want, got.Text)
				require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSensitivePath: 1}}, got.Counts)
				again, err := rp.RedactText(got.Text)
				require.NoError(t, err)
				require.Equal(t, got.Text, again.Text)
				require.Equal(t, redact.Counts{}, again.Counts)
				screen, err := rp.RedactScreen(redact.Screen{Rows: []string{tc.input}})
				require.NoError(t, err)
				require.Equal(t, []string{tc.want}, screen.Rows)
				require.Equal(t, redact.Counts{Total: 1, ByKind: map[redact.Kind]int64{redact.KindSensitivePath: 1}}, screen.Counts)
				second, err := rp.RedactScreen(redact.Screen{Rows: screen.Rows})
				require.NoError(t, err)
				require.Equal(t, screen.Rows, second.Rows)
				require.Equal(t, redact.Counts{}, second.Counts)
			})
		}
	})

	// Path boundaries yield to covered runes: a vendor token or a PEM
	// marker glued to a configured path lets the path count, and two
	// paths covering each other count both. Each row runs on text and on
	// a screen row, with a no-op second pass.
	n := mustValue(t, "sensitive_contract")
	kn := mustValue(t, "github_pat")
	kb := testMarker("BEGIN", "RSA PRIVATE KEY")
	ktok := "sk-" + mustValue(t, "hex64")[:24]
	rp := mustNewPaths(t, n, "/one)", "~/two", "/left)", "~/"+ktok)
	for _, tc := range []struct {
		name   string
		input  string
		want   string
		counts redact.Counts
	}{
		{
			name:   "token glued before path",
			input:  kn + n + "/x",
			want:   dots(kn) + dots(n+"/x"),
			counts: redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1, redact.KindSensitivePath: 1}},
		},
		{
			name:   "marker glued before path",
			input:  kb + n + "/x",
			want:   dots(kb) + dots(n+"/x"),
			counts: redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1, redact.KindSensitivePath: 1}},
		},
		{
			name:   "marker glued after path",
			input:  n + kb,
			want:   dots(n) + dots(kb),
			counts: redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindPrivateKey: 1, redact.KindSensitivePath: 1}},
		},
		{
			name:   "token and paths covering each other",
			input:  "/left)~/" + ktok,
			want:   dots("/left)~/" + ktok),
			counts: redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindAPIKey: 1, redact.KindSensitivePath: 1}},
		},
		{
			name:   "paths covering each other",
			input:  "/one)~/two",
			want:   dots("/one)~/two"),
			counts: redact.Counts{Total: 2, ByKind: map[redact.Kind]int64{redact.KindSensitivePath: 2}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := rp.RedactText(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Text)
			require.Equal(t, "2", got.PolicyVersion)
			require.Equal(t, tc.counts, got.Counts)
			again, err := rp.RedactText(got.Text)
			require.NoError(t, err)
			require.Equal(t, got.Text, again.Text)
			require.Equal(t, redact.Counts{}, again.Counts)
			screen, err := rp.RedactScreen(redact.Screen{Rows: []string{tc.input}})
			require.NoError(t, err)
			require.Equal(t, []string{tc.want}, screen.Rows)
			require.Equal(t, "2", screen.PolicyVersion)
			require.Equal(t, tc.counts, screen.Counts)
			againScreen, err := rp.RedactScreen(redact.Screen{Rows: screen.Rows})
			require.NoError(t, err)
			require.Equal(t, screen.Rows, againScreen.Rows)
			require.Equal(t, redact.Counts{}, againScreen.Counts)
		})
	}
}

// mustNewPaths builds a Redactor over the given sensitive paths.
func mustNewPaths(t *testing.T, paths ...string) *redact.Redactor {
	t.Helper()
	r, err := redact.New(redact.Options{SensitivePaths: paths})
	require.NoError(t, err)
	return r
}
