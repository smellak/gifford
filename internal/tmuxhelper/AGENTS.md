# internal/tmuxhelper — rules for agents

- Keep the command grammar closed to the eight documented verbs. Never accept
  caller-supplied tmux options, formats, sockets, executable paths or text.
- Re-read and revision-check the root-owned registry before every tmux call.
  Reject root execution, UID mismatches and unregistered panes.
- Validate the socket filesystem identity and the pane PID, process start time
  and boot ID before observing or changing a registered pane.
- Notice text and Enter remain separate verbs. The parent owns the pane lock,
  verifies the staged notice between them and maps any post-write ambiguity to
  `UNCERTAIN`; the helper never claims delivery authority.
- Buffer bounded output and emit only the versioned frame. Stderr and errors
  never contain registry data, paths, command diagnostics or pane content.
