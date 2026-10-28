package channel

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// forgeCase binds one negative-compilation program to its expected vet
// output.
type forgeCase struct {
	dir     string
	succeed bool
	message string
}

// TestDecisionCannotBeForged proves no code outside the package can build
// a Decision: the zero value compiles and stays inert, while literals,
// assignments, conversions and constructors do not compile.
func TestDecisionCannotBeForged(t *testing.T) {
	t.Parallel()

	goBin, err := exec.LookPath("go")
	require.NoError(t, err, "the go binary must exist for the forge test")

	cases := []forgeCase{
		{dir: "forge_zero", succeed: true},
		{dir: "forge_literal", message: "unexported field d"},
		{dir: "forge_assign", message: "unexported field d"},
		{dir: "forge_convert", message: "cannot convert"},
		{dir: "forge_ctor", message: "undefined: channel.NewDecision"},
	}
	for _, tc := range cases {
		t.Run(tc.dir, func(t *testing.T) {
			t.Parallel()

			cmd := exec.CommandContext(t.Context(), goBin, "vet", "./testdata/"+tc.dir)
			out, err := cmd.CombinedOutput()
			t.Logf("%s: %s", tc.dir, firstVetLine(out))
			if tc.succeed {
				require.NoError(t, err, "vet output: %s", out)
				return
			}
			require.Error(t, err, "expected vet to fail")
			require.Contains(t, string(out), tc.message)
		})
	}
}

// firstVetLine returns the first message line of vet output for the
// evidence log, skipping the package header; empty when vet is quiet.
func firstVetLine(out []byte) string {
	for line := range strings.Lines(string(out)) {
		trimmed := strings.TrimRight(line, "\n")
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			return trimmed
		}
	}
	return ""
}
