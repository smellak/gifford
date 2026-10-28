# Gifford — guide for implementing agents

Read this file, then the AGENTS.md of the package you work in, then the issue of
your subphase. Nothing else is required: every subphase issue is self-contained.
If it is not, ask in the issue instead of guessing.

## What this is
Gifford lets interactive coding agents (Muse Code, Claude Code, Codex, OpenCode,
Antigravity) message each other without ever typing over the human. A PTY proxy
owns the keyboard; Muse Glimmer reads screens; deterministic guards decide. See
docs/DESIGN.md once it lands.

## Layout
cmd/gifford/main.go  → internal/cli (cobra)  → internal/<package> (one per component)
See docs/project-layout.md. Rules: docs/repo-rules.md R1–R10 (one owner per package,
no pkg/, no util/, depth ≤ 3 under internal/). internal/channel is the Channel
Control Plane: every Hub admission, Orchestrator stage 0 and Relay FILE_READY goes
through it.

## Build, test, lint — always through Taskfile
task build · task test · task test:race · task test:update (golden) · task lint ·
task lint:fix · task fmt · task generate:check · task fuzz -- --scenarios N

## Invariants you never break
- Only internal/proxy/writer.go writes to a child PTY. Never `tmux send-keys`.
- `tmux pipe-pane` only with -O. Observed agents are deliverable=false.
- Glimmer never writes anywhere and never bypasses a guard. No timeout closes a draft.
- Nothing is delivered without a `channel.Decision{Allow}` passed as a parameter and
  revalidated right before acting. Only internal/channel writes channel_controls,
  communication_loops, channel_control_decisions, approval_requests and control_state.
  Agents get request tools (`request_loop_extension`), never control tools. Glimmer
  never changes a control; the Proxy never learns one (docs/repo-rules.md R46–R49).
- Nothing leaves the node for Glimmer without internal/redact. No secrets in logs.
- Nothing from .gitignore is ever committed.
- Nothing private enters this repository: no recordings, captures, raw datasets,
  keys, hostnames, IP addresses or references to anyone's infrastructure. Use
  placeholders such as `<org>` and `example.invalid`. This repository is public.

## Code style
gofumpt + goimports; context.Context first; interfaces in the consuming package;
errors wrapped with %w, sentinels ErrX; os.Exit only in main; slog with capitalized
messages and snake_case keys; JSON/YAML snake_case; 0o600/0o700; comments say why.

## Git
Branch <role>/<subphase>-<slug>. Conventional Commits with package scope
(`feat(proxy): ...`), imperative, lowercase, ≤ 72 chars, Co-authored-by trailer.
One PR per subphase using the template; observed test evidence; you never merge.
PRs are integrated one at a time, in dependency order, once the required checks
are green and the maintainer has explicitly approved that PR (or a block of PRs
named by their commits). Nobody merges on their own initiative.

## Roles and packages
| Role | Packages |
|---|---|
| A-CORE | cmd/gifford, internal/cli, internal/protocol, schemas/, internal/eventstore, migrations/, internal/controlapi, internal/notifications, internal/channel, internal/clock, internal/version |
| A-PROXY | internal/proxy, internal/vt, internal/profiles, profiles/, keycap, fuzz |
| A-MCP | internal/endpoint, internal/hub, internal/crypto |
| A-GLIMMER | internal/gateway, internal/analyst, internal/orchestrator |
| A-FED | internal/federation, internal/registry, internal/policy |
| A-RELAY | internal/artifacts |
| A-TUI | internal/tui |
| A-ML | internal/capture, internal/dataset, internal/redact, ml/ |
| A-ADAPTERS | internal/adapters, sdk/, test/conformance |
