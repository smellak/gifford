// Package capture is Bootstrap Capture Mode: attaching to panes, ingesting the
// byte stream into chunks with wall and monotonic time, supervising, rotating
// and reporting status (PRD §9.15).
//
// Invariants: Panes are observed only with `tmux pipe-pane -O`; a pane that
// already has a pipe is refused unless --replace; observed agents are
// deliverable=false. See AGENTS.md in this directory.
//
// What it does not do: It never writes to a pane.
package capture
