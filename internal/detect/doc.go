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
// Harness screen markers (working indicators, composer forms, placeholder
// texts, CLI binary names) are data in Table, measured on real screens.
// The shell, Codex, Claude, Muse and OpenCode profiles classify; every
// profile stays uncertain rather than guessing where its own markers do
// not reach. Markers that only one harness shows live in its profile
// (Chrome, SkipRules, composer lead); the shared helpers stay neutral.
// OpenCode boxes span several rows around one content row, so its profile
// reads the run as one box and checks hints by their fixed gray in the
// raw bytes; the single-row harnesses never take that path.
package detect
