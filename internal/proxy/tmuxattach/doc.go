// Package tmuxattach injects content-free notices into already-running tmux
// panes. It is a separate transport from the PTY proxy: it never claims proxy
// ownership of the keyboard and fails closed whenever pane state is uncertain.
package tmuxattach
