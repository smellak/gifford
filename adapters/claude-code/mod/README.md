# Claude Code native observer mod

This observe-only mod supports exactly Claude Code 2.1.316. It is packaged as
a Claude Code plugin and emits content-free native lifecycle events by running
`gifford adapter emit` through the mods process API. Each invocation uses an
argument vector, never a shell.

Run Claude Code under `gifford run`, with this directory loaded as a plugin:

```console
gifford run --agent claude --harness claude-code -- \
  claude --plugin-dir ./adapters/claude-code/mod
```

The `gifford` executable must be on `PATH`. A missing executable, rejected
native-event connection, full queue or process failure is silent and never
changes Claude Code's control flow. The queue holds at most 64 observations.

## Projection

| Claude Code event | Native event |
| --- | --- |
| `session.start` | `session.started`, once per session across reloads |
| `session.end` | `session.ended` |
| `turn.start` | `turn.started` |
| `turn.step` | `turn.streaming`, without reading stream chunks |
| `turn.complete` | `turn.completed`; `aborted` is `cancelled`, `answer` is `ok`, and other reasons are `failed` |
| `tool.check` → `allow` | `tool.started` |
| `tool.check` → `ask` | `approval.requested` with a random request reference |
| a later `tool.check` resolving that ask | `approval.resolved`; an allow also starts the tool, and a deny fails it |
| `tool.call` result | `tool.completed` or `tool.failed` |
| `agent.spawn` result | `subagent.started` |
| `ui.render` for `AskUserQuestion` | `question.requested`, once for the tool-use reference |
| `AskUserQuestion` result | `question.resolved` |
| `session.append` | no native event |

`tool.call` is deliberately not treated as a tool start: it runs before the
permission decision. A call closes only when core returns its result reference;
an early hook-provided result is not reported as execution. An `ask` that has
no later structured decision is not given an invented close; Gifford's native
reducer bounds that approval veto.

## Boundaries

Every hook calls `next(e)` and returns its result unchanged. The streaming hook
delegates with `yield* next(e)`. The mod never submits prompts, calls tools,
changes permissions, writes messages or uses Node.js APIs. It never reads or
exports prompt text, `session.append` content, tool arguments, tool output,
answers or refusal text. Only native identifiers, closed event/result enums,
coarse tool kinds and timestamps reach `gifford adapter emit`.

The mod deliberately does not register `classic.*` hooks. Their settings-hook
events would duplicate the authoritative modern session, tool and subagent
events, and some carry free text that this observer must not read.

## Exact-build checks

The development dependency pins the official Claude Code package to 2.1.316.
Install it locally, not globally:

```console
npm install --prefix adapters/claude-code/mod
```

Loading the mod once with that build writes its exact TypeScript declarations
under `.claude-plugin/types/`; the directory is ignored because it is generated
by Claude Code. Validate and run the fixture tests with the same local binary:

```console
task adapters:claude-code-mod:test
```
