package initcmd

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestInitNotImplemented(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})

	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.NotImplemented, ec.Code)
}
