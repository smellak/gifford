// Package tui is gifford panel, built with Bubble Tea (PRD §19). See AGENTS.md
// in this directory.
//
// Invariants: No I/O in Update; ANSI-aware string handling only through
// x/ansi; styles come from styles/ tokens (R18).
//
// What it does not do: It does not hold state that the daemon owns.
package tui
