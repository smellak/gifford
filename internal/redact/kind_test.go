package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestKinds pins the kind priority order, the freshness of EnumValues and
// the exact matching of Valid.
func TestKinds(t *testing.T) {
	t.Parallel()

	t.Run("priority order", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, []string{
			"private_key",
			"connection_string",
			"authorization",
			"api_key",
			"jwt",
			"secret_assignment",
			"sensitive_path",
		}, redact.Kind("").EnumValues())
	})

	t.Run("fresh slice", func(t *testing.T) {
		t.Parallel()
		first := redact.Kind("").EnumValues()
		first[0] = "mutated"
		require.Equal(t, "private_key", redact.Kind("").EnumValues()[0])
	})

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		for _, k := range redact.Kind("").EnumValues() {
			require.True(t, redact.Kind(k).Valid(), "kind %q", k)
		}
		for _, k := range []string{"", "API_KEY", "Private_Key", "private_key ", "token"} {
			require.False(t, redact.Kind(k).Valid(), "kind %q", k)
		}
	})
}
