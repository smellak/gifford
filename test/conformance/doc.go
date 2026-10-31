//go:build conformance

// Package conformance exercises the real PTY delivery path against one
// operator-selected harness. It is excluded from normal tests because it owns
// a private tmux server and, for real harnesses, uses the operator's local
// installation and measured profile.
//
// GIFFORD_CONFORMANCE_HARNESS selects the harness. Real harness runs also use
// GIFFORD_CONFORMANCE_COMMAND (a JSON argv) and
// GIFFORD_CONFORMANCE_PROFILE (an exact-version measured YAML). Optional CWD
// and report paths come from GIFFORD_CONFORMANCE_CWD and
// GIFFORD_CONFORMANCE_REPORT. GIFFORD_CONFORMANCE_PREPARE=opencode-ok enables
// the explicit prepared OpenCode run; without it every harness starts cold.
// The synthetic harness supplies its own command, profile and working
// directory.
//
// S0 deliberately exempts no descendants: a harness-owned MCP server or other
// otherwise legitimate child still fails the process-tree guard and is
// documented in the sanitized report.
package conformance
