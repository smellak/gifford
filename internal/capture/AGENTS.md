# internal/capture — rules for agents

- Panes are observed only with `tmux pipe-pane -O`. Never `-I`: a test inspects
  the generated command.
- A pane that already has a pipe is refused unless `--replace` is given.
- Capture never writes to a pane. Observed agents are `deliverable=false`.
- Bytes are stored intact (ANSI included), with wall time, monotonic time and a
  sequence number per chunk.
- Captures never enter the repository (`captures/` is ignored).
- tmux is reached only through the typed runner in `tmux.go`, one method per
  allowed operation: `display-message -p`, `list-panes -a -F`, and the guarded
  conditional open built only by the fixed builders (branches hold only
  `display-message -p`, `pipe-pane -O` with a command, and `capture-pane`).
  Every call carries `-S <socket>` except socket discovery. Anything else is
  a bug: no send-keys, set-option, run-shell, `if-shell` without `-F` or with
  `-b`, free branch text, kill-*, respawn-*, resize-* or new-*, and no
  `pipe-pane -O` without a command.
- Every code line that names `pipe-pane` also contains `-O`, comments and
  tests included.
- A pipe is opened only by the guarded conditional. Normal attach and every
  automatic reattach refuse a present pipe. Only `attach --replace`, under
  the operator's authority, replaces a pipe that no live Gifford session
  claims. Nothing else closes or replaces a pipe through tmux; stopping means
  the ingest closes its stdin and exits.
- No signals to observed panes, harnesses, pipe commands or any process the
  package did not start. Only its own tmux client calls end, under
  `exec.CommandContext` with a deadline.
- Lock order: `supervise.lock` → `claims.lock` → a session's `ingest.lock` →
  `quota.lock`, every acquisition bounded, `stop.lock` never nested.
  `AppendEvents` is never called inside another SQLite transaction.
- Files are reached only through the capture root handle and the checks of
  the layout rules. Nothing is chmod'ed, chowned or followed to repair.
- Files are published only through `publish.go`: rename without replace,
  never hard links.
- Logs (`log/slog`) never carry pane content, screens, titles, session names
  or command lines: ids, counts and causes only.
