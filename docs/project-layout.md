# Project layout

| Path | What lives there |
|---|---|
| `cmd/gifford/` | `main.go`, ≤ 30 lines: `os.Exit(cli.Main())` |
| `internal/cli/` | cobra root, shared `Deps`, exit codes, one package per subcommand |
| `internal/<component>/` | one package per component; read its `doc.go` first |
| `adapters/<harness>/` | observe-only emitters that run inside a harness |
| `schemas/` | JSON Schema v1 of envelopes, events, manifests, config |
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
