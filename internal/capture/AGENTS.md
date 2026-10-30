# internal/capture — rules for agents

- Panes are observed only with `tmux pipe-pane -O`. Never `-I`: a test inspects
  the generated command.
- A pane that already has a pipe is refused unless `--replace` is given.
- Capture never writes to a pane. Observed agents are `deliverable=false`.
- Bytes are stored intact (ANSI included), with wall time, monotonic time and a
  sequence number per chunk.
- Captures never enter the repository (`captures/` is ignored).
- An import never writes typed input (`i`) or markers (`m`) anywhere: they are
  counted only. It never records the source's path, nor when it ran, and never
  makes up a keyboard or draft event.
- An import writes under temporary names and renames once whole, the events
  file last; on failure it removes only its own temporary files, by exact path.
