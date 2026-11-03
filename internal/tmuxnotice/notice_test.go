package tmuxnotice

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidUsesClosedShellSafeAlphabet(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		": notice", ": [GIFFORD] 1 pending message", ": A-Z_a.z,0: [ok]",
		Prefix + strings.Repeat("x", 510),
	} {
		require.True(t, Valid(value), value)
	}
	for _, value := range []string{
		"", Prefix, "notice", ":[GIFFORD]", ": [GIFFORD] $(id)", ": [GIFFORD] `id`",
		": [GIFFORD] a|b", ": [GIFFORD] a > b", ": [GIFFORD] a;b",
		`: [GIFFORD] "quoted"`, `: [GIFFORD] slash\\`, ": café", string([]byte{':', ' ', 0xff}),
		Prefix + strings.Repeat("x", 511),
	} {
		require.False(t, Valid(value), value)
	}
}
