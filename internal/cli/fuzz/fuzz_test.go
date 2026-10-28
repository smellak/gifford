package fuzz

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestFuzzNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{"fuzz", "fuzz --scenarios 10 --baseline"} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}
