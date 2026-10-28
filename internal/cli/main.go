package cli

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

// Main builds the root command, runs it with the process arguments and
// returns the exit code for cmd/gifford.
func Main() int {
	root := NewRootCmd(&deps.Deps{})
	root.SetArgs(os.Args[1:])
	if err := root.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		var ec *deps.ExitError
		if errors.As(err, &ec) {
			return ec.Code
		}
		return exitcode.Error
	}
	return exitcode.OK
}
