// Package tmuxinstance is the node's registry of agents that already run in a
// tmux pane on this machine (capture mode tmux_attach): which tmux server and
// pane each one runs in, which process is its CLI, and whether that is still
// true.
//
// Invariants:
//   - Only the node and the administrator write the registry; agents never
//     reach it. A row is never deleted: an instance that changes becomes
//     INVALID, and registering it again creates a new row.
//   - At most one ACTIVE instance per agent, and one per tmux pane.
//   - An instance's identity is its CLI process: PID, start time and uid.
//     The owner of the tmux server only decides the sudo user and the
//     socket, and never stands in for the agent.
//   - The CLI is the pane's own process, or the only direct child of the
//     pane's process with the name stated at registration: a launcher such
//     as node, or the shell the CLI was started from. The CLI itself is
//     never a shell. A notice cannot run as a command in that shell: Verify
//     invalidates the instance once the CLI exits or changes, the writer
//     types only while the CLI holds the pane's foreground, and Enter
//     follows only once the typed line is verified.
//   - Verify runs before every write, against a pane observation taken under
//     the per-pane lock, and invalidates the instance on any mismatch.
//
// It does not run tmux: callers pass what tmux printed (PaneLister,
// PaneObservation). It does not take the per-pane lock, and it never reads
// /proc/<pid>/exe, which another user's process does not expose.
package tmuxinstance
