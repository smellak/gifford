// Package tmuxhelper implements the closed privileged interface used to
// inspect and notify registered tmux panes. It accepts seven fixed verbs,
// resolves every target from a revision-pinned, root-owned canonical JSON
// registry, and never accepts a tmux option, socket, notice, or format string
// from its caller. Notice values remain private deployment data.
//
// The helper does not acquire the pane lock. Its parent keeps the same
// close-on-exec lock description across observe, durable state transitions,
// notice, verification, Enter, and final recording. This package revalidates
// the registered operating-system identity immediately before every tmux
// effect; it does not turn a successful helper exit into delivery authority.
package tmuxhelper
