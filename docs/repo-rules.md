# Repository rules

Numbered so reviews can cite them ("breaks R12"). **[gate]** rules must be green
before the `contracts-v1` tag.

## Module and layout
- **R1 [gate].** One Go module at the root, no `go.work`, full `go` version plus a `toolchain` directive.
- **R2 [gate].** `cmd/gifford/main.go` and the operator-only
  `cmd/fedprobe/main.go` are the only `package main` files. Each is ≤ 30 lines
  with no logic, flags or `init()`; it only delegates and exits.
- **R3 [gate].** Product code lives in `internal/`. There is no `pkg/`. Public packages, when they come, go to `sdk/`.
- **R4.** One package per component, short lowercase names (`proxy`, `vt`, `channel`, …). At most three levels under `internal/`.
- **R5.** Forbidden package names: `util`, `utils`, `common`, `helpers`, `shared`, `lib`, `misc`, `base`.
- **R6.** Subcommands live in `internal/cli/<cmd>` with `NewCmd(deps *cli.Deps) *cobra.Command` and no business logic.
- **R7.** Platform files by suffix (`_linux.go`, `_other.go`); `go build ./...` passes on macOS and Windows with stubs returning `errors.ErrUnsupported`.
- **R8.** Non-code assets live where they belong and are embedded by their owner: `profiles/`, `schemas/`, `migrations/`, `internal/analyst/prompts/`, `internal/channel/budgets/`, `policies/`.
- **R9.** Generated files are committed; CI checks there is no drift (`task generate:check`).
- **R10.** Allowed root directories: `cmd/ internal/ sdk/ schemas/ profiles/ migrations/ policies/ ml/ docs/ test/ scripts/ .github/`. A new one needs an ADR.

## Code
- **R11.** `context.Context` first for anything that does I/O, waits or can be cancelled; never stored in structs.
- **R12.** Interfaces are defined by the consumer and stay small (1–3 methods).
- **R13.** No mutable globals, no `init()` with side effects. Only sentinel errors and constants at package level.
- **R14.** Wrap errors with `fmt.Errorf("...: %w", err)`, no "failed to"; `ErrX` sentinels, `XError` types; `errors.Is/As`; handle each error once.
- **R15.** `os.Exit` only in `cmd/gifford/main.go` and
  `cmd/fedprobe/main.go`; Gifford exit codes live in `internal/cli/exitcode`,
  while fedprobe's status is decided by `internal/federation`.
- **R16.** `log/slog`; capitalized messages, snake_case keys; never a secret, message body or unredacted screen in a log.
- **R17.** JSON/YAML snake_case; file modes `0o600`/`0o700`; comments in English explain why.
- **R18.** TUI code follows `internal/tui/AGENTS.md`.
- **R19.** Rules no linter covers become scripts in `scripts/` run by `task lint`.

## Tools
- **R20 [gate].** `Taskfile.yml` is the only build/test/lint interface.
- **R21 [gate].** `.golangci.yml` v2 with depguard and forbidigo; tests are linted.
- **R22.** gofumpt + goimports with local prefix; `task fmt` before every PR.
- **R23.** New dependencies need a clean `go mod tidy`, an Apache-2.0-compatible license and one line explaining why the stdlib was not enough.
- **R24.** `.goreleaser.yaml` from day 1: `CGO_ENABLED=0`, `-trimpath`, version ldflags, linux amd64/arm64.

## Tests
- **R25.** Table tests, `t.Parallel()` unless a real shared resource, `t.TempDir()`, `t.Setenv()`, testify `require`.
- **R26.** Golden files for screens, TUI output, rendered prompts, manifests and explanations; regenerate with `task test:update` and explain it in the PR.
- **R27.** Model calls in tests replay cassettes; harnesses are faked by `test/fakeharness`.
- **R28.** Suites needing real things live in `test/` with build tags and do not run in `task test`.
- **R29.** `-race` always in CI; tests over 10 s move to `test/`.
- **R30.** Seeded-secret fixtures are named `testdata/redact/seeded_*` and allowlisted in `.gitleaks.toml`; any other secret is an incident.

## Git
- **R31 [gate].** `main` is protected: PR and green CI required; merge commits only; squash and rebase merges disabled; history is never rewritten. PRs are integrated one at a time in dependency order, each with the maintainer's explicit approval; there is no merge schedule.
- **R32.** Branches `<role>/<subphase>-<slug>`, one per subphase, deleted after merge.
- **R33.** Conventional Commits with package scope; `!` for contract changes.
- **R34.** Imperative lowercase subject ≤ 72 chars; body at 76 columns; `Refs #n`; the agent's harness identity and trailers (`AUTHORS.md`). No `fixup!`/`wip`.
- **R35.** One small PR per subphase with the template filled in, observed evidence and declared authorship. PRs touching another owner's package are closed.
- **R36.** Labels by path (`.github/labeler.yml`); `CODEOWNERS` documents the owner of every package.
- **R37.** SemVer tags; `contracts-v1`, `ml-contract-v1`, `feature-freeze` are milestone tags. `CHANGELOG.md` is generated.

## CI and security
- **R38 [gate].** `ci.yml`: tidy, generate:check, lint (incl. guards), race build and tests, govulncheck, gitleaks, darwin/windows cross-build; actions pinned by SHA.
- **R39.** `snapshot.yml` on `main`, `release.yml` on `v*` tags.
- **R40.** Weekly grouped Dependabot; `SECURITY.md`; gitleaks allowlist only for seeded fixtures.
- **R41.** `.gitignore` blocks recordings, captures, datasets, adapter weights, keys, databases and `.env`.

## Documentation
- **R42.** Short English README.
- **R43.** `docs/`: DESIGN, project-layout, testing, commit-messages, CLI (generated), ML_CONTRACT, RESULTS, FREEZE, and numbered ADRs.
- **R44.** Root `AGENTS.md` plus nested ones where a package has its own rules.
- **R45.** Every package has a `doc.go`: what it does, which invariants it guarantees, what it does not do.

## Channel Control Plane
- **R46 [gate].** Every delivery takes a `channel.Decision` with `Allow` as a typed parameter and revalidates it right before acting. Nobody outside `internal/channel` can build a `Decision`.
- **R47 [gate].** Only `internal/channel` writes the four control tables and the control fields of messages and artifacts.
- **R48 [gate].** The Agent Endpoint exposes no MCP tool that changes a control.
- **R49.** `internal/channel` never imports a model client or the Proxy; loop counters are reserved in the message's own transaction.
