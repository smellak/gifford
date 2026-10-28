package cli

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/smellak/gifford/internal/cli/exitcode"
)

// Main builds the root command, runs it with the process arguments and returns
// the exit code for cmd/gifford.
func Main() int {
	root := NewRootCmd(&Deps{})
	root.SetArgs(os.Args[1:])
	if err := root.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		var ec *ExitError
		if errors.As(err, &ec) {
			return ec.Code
		}
		return exitcode.Error
	}
	return exitcode.OK
}

// ExitError carries a specific exit code up to Main.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }

func (e *ExitError) Unwrap() error { return e.Err }
