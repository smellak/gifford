// Package exitcode defines the process exit codes of the gifford binary.
// They are part of the CLI contract and are documented in docs/CLI.md.
package exitcode

const (
	// OK means the command did what was asked.
	OK = 0
	// Error is any failure without a more specific code.
	Error = 1
	// Usage means the command line was wrong.
	Usage = 2
	// NotImplemented means the subcommand exists but does nothing yet.
	NotImplemented = 3
	// Refused means a guard or policy refused the operation on purpose.
	Refused = 4
)
