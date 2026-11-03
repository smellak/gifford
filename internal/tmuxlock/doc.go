// Package tmuxlock serializes one tmux pane's out-of-process writers.
//
// Lock files are provisioned ahead of time. This package only derives their
// names, opens and validates the existing inode without following symlinks,
// and retains an exclusive flock for the caller's critical section. It never
// creates, repairs, renames, or removes lock state and it never calls tmux.
package tmuxlock
