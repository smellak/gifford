// Package cli builds the cobra command tree of the gifford binary.
//
// Each first-level subcommand lives in its own package under internal/cli
// (internal/cli/run, internal/cli/capture, ...) with a NewCmd(deps *Deps)
// constructor. Subcommands parse flags, build dependencies and call a package
// under internal/; they hold no business logic (REPO-RULES R6).
//
// This package does not exit the process: Main returns an exit code from
// internal/cli/exitcode and only cmd/gifford calls os.Exit (R15).
package cli
