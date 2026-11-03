# Project layout

| Path | What lives there |
|---|---|
| `cmd/gifford/` | `main.go`, ≤ 30 lines: `os.Exit(cli.Main())` |
| `cmd/gifford-tmux/` | closed privileged tmux entry point, with no parsing or policy |
| `internal/cli/` | cobra root, shared `Deps`, exit codes, one package per subcommand |
| `internal/tmuxhelper/` | seven fixed tmux operations over a revision-pinned, root-owned registry |
| `internal/<component>/` | one package per component; read its `doc.go` first |
| `adapters/<harness>/` | observe-only emitters that run inside a harness |
| `schemas/` | generated frozen protocol contracts under `v1/`, plus explicitly documented, unfrozen deployment schemas |
| `profiles/` | harness keyboard profiles (YAML), embedded by `internal/profiles` |
| `migrations/` | SQL migrations (`<YYYYMMDDHHMMSS>_<name>.sql`), embedded by their own package and applied by `internal/eventstore` |
| `policies/` | example policies |
| `ml/` | offline Python: dataset preparation, LoRA training and evaluation |
| `test/` | suites that need real things: conformance, safety, fuzz, e2e |
| `scripts/` | project guards and helpers run by `task` |
| `docs/` | design, rules, ADRs |

## How a command runs

`gifford <cmd>` → `cmd/gifford/main.go` → `cli.Main()` builds the root →
`internal/cli/<cmd>.NewCmd(deps)` parses flags and builds dependencies → calls the
component package in `internal/` → returns an error → `Main` maps it to an exit
code from `internal/cli/exitcode`.

`gifford-tmux <verb> <revision> <bank> [pane]` →
`cmd/gifford-tmux/main.go` → `tmuxhelper.Main()` validates the closed grammar,
registry revision, operating-system ownership and pane identity → executes one
fixed tmux operation → returns a versioned binary-safe frame.
