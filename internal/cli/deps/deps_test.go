package deps

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestLeafReturnsNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{{}, {"anything", "else", "x"}} {
		t.Run("args", func(t *testing.T) {
			t.Parallel()

			cmd := Leaf("x", "y")
			cmd.SetOut(io.Discard)
			cmd.SetArgs(args)

			var ec *ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestGroupDispatchesToChildren(t *testing.T) {
	t.Parallel()

	t.Run("bare group exits 0", func(t *testing.T) {
		t.Parallel()

		cmd := Group("x", "y", Leaf("z", "w"))
		cmd.SetOut(io.Discard)
		cmd.SetArgs([]string{})
		require.NoError(t, cmd.Execute())
	})

	t.Run("child returns not implemented", func(t *testing.T) {
		t.Parallel()

		cmd := Group("x", "y", Leaf("z", "w"))
		cmd.SetOut(io.Discard)
		cmd.SetArgs([]string{"z"})

		var ec *ExitError
		require.ErrorAs(t, cmd.Execute(), &ec)
		require.Equal(t, exitcode.NotImplemented, ec.Code)
	})
}

func TestExitErrorUnwraps(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	ec := &ExitError{Code: exitcode.Error, Err: sentinel}
	require.ErrorIs(t, ec, sentinel)
	require.Equal(t, "boom", ec.Error())
}
