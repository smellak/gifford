// Package capture is Bootstrap Capture Mode: attaching to panes, ingesting the
// byte stream into chunks with wall and monotonic time, supervising, rotating
// and reporting status (PRD §9.15), and importing captures recorded without
// the binary.
//
// Invariants: Panes are observed only with `tmux pipe-pane -O`; a pane that
// already has a pipe is refused unless --replace; observed agents are
// deliverable=false. See AGENTS.md in this directory.
//
// Import reads an asciicast v2 file into the pair a `gifford run` session
// leaves in recordings/, written with the session's own Recorder and
// EventLog, and its provenance beside it in <id>.import.json: the source's
// format, digest and size, the agent, harness and environment domain it is
// attributed to, and what was counted. Only the header's version, size and
// timestamp are read; every output is one recorded chunk with the screen
// hash a live vt gives, every resize an input.resize. Typed input and
// markers are counted and never written, the source's path is never
// recorded, and no keyboard or draft event is made up, so a replay leaves
// the draft unknown and the screen projection blanks the screen.
//
// What it does not do: It never writes to a pane.
package capture
