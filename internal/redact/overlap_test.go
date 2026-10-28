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
}
