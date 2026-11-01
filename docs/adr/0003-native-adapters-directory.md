# ADR 0003 — Native adapters live under adapters/

- **Status:** accepted
- **Owner:** A-ADAPTERS

## Context

Gifford observes harnesses through the PTY by default. Some harnesses also expose native extension points: OpenCode plugins, hooks in Claude Code, Codex and Muse Code, and Claude Code mods. Code that runs inside a harness is not Go, ships with that harness's own runtime, and must never reach the PTY. The Go side of the native path (the native-events ingress in `gifford run`, its wire and the durable native profile) stays in `internal/`.

R10 limits the root directories, and none fits harness-side code: `internal/` holds Go product code, `sdk/` is reserved for public Go packages, and `profiles/` holds measured harness profiles.

## Decision

Harness-side emitters live under `adapters/<harness>/`, one directory per harness, starting with `adapters/opencode/`. Each directory has a short README that says what it observes, how it is installed and what it never does.

Emitters only observe. They never write to the PTY, submit prompts, press keys, answer questions, approve tools or change permissions. They talk to Gifford only through the native-events socket named by `GIFFORD_ADAPTER_SOCKET`, using the closed wire described in `docs/native-adapters.md`.

## Consequences

- Harness-side tests run with the harness's own toolchain (`node --test` for OpenCode) through a `task` target, and CI runs that target.
- A failing or missing emitter never blocks the harness: Gifford falls back to `generic_pty`.
- Adding a harness adds a directory, not a new root.
