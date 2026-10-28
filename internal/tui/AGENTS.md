# internal/tui — rules for agents

- No I/O inside `Update`. I/O happens in `tea.Cmd`s that return messages.
- State changes only inside `Update`, never inside a `tea.Cmd`.
- Strings that may contain ANSI are handled only with `x/ansi`
  (`ansi.StringWidth`, `ansi.Truncate`, `ansi.Strip`).
- Styles are centralised in `internal/tui/styles` as tokens.
- One root model; components expose imperative methods.
- The panel shows what the daemon owns; it never keeps its own copy of truth.
