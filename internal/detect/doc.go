// Package detect classifies one tmux pane from the outside: a
// `capture-pane -p -e` dump plus the pane's process tree. It answers the
// three questions the tmux_attach path asks before writing: is the agent
// working, does the input box hold anything but the placeholder, and does
// the pane really run the registered harness CLI.
//
// The classifier is pure: no tmux, no ps, no I/O. Reading the screen and
// the tree is the caller's job. Anything uncertain fails closed: Certain
// is false and the caller must not write.
//
// Harness screen markers (working indicators, input-line expressions, CLI
// binary names) are data in Table, ported from the CapyCom notifier. Until
// the bench markers land, only the shell profile classifies; every agent
// harness stays uncertain rather than guessing.
package detect
