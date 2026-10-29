//go:build tmux

// Package capture_test is the synthetic integration suite for Bootstrap
// Capture Mode (§13.2): a private tmux server per test, synthetic pane
// programs only, never the default server or a real harness. The test
// binary re-executes itself in helper and ingest modes; TestMain fails
// when tmux is missing or older than 3.4 and never skips.
package capturetest
