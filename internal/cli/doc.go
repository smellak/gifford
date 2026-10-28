// Package cli builds the cobra command tree of the gifford binary.
//
// Each first-level subcommand lives in its own package under internal/cli
// (internal/cli/run, internal/cli/capture, ...) with a NewCmd(d *deps.Deps)
// constructor. Subcommands parse flags, build dependencies and call a
// package under internal/; they hold no business logic (REPO-RULES R6).
// Deps and ExitError live in internal/cli/deps, not here, because this
// package imports every subcommand package to assemble the root command,
// and the reverse import would be a cycle.
//
// This package does not exit the process: Main returns an exit code from
// internal/cli/exitcode and only cmd/gifford calls os.Exit (R15).
package cli
