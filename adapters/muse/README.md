# Muse Code hooks

This observe-only plugin supports exactly Muse Code 1.4.13 build
`1.4.13-R6996.1`. It translates that build's hook payloads into Gifford's
content-free native lifecycle events. It retains only identifiers and the
closed `source` and `trigger` enums; prompts, tool inputs and outputs, model
text, paths, errors, reasons and notifications are discarded while JSON is
decoded.

| Muse Code hook | Native event |
| --- | --- |
| `SessionStart` with `startup`, `resume` or `clear` | `session.started` |
| `SessionStart` with `compact` | no event |
| `SessionEnd` | `session.ended` |
| `UserPromptSubmit` | `prompt.submitted` |
| `PreLLMCall` | `turn.started` |
| `PostLLMCall` | `turn.streaming` |
| `PreToolUse` | `tool.started` |
| `PermissionRequest` | `approval.requested` |
| `PostToolUse` | `tool.completed` with `ok` |
| `PostToolUseFailure` | `tool.failed` with `failed` |
| `SubagentStart` | `subagent.started` |
| `PostCompact` with `manual` or `auto` | `context.compaction_completed` |
| `Interrupt` | `turn.completed` with `cancelled` |
| `ToolUseStart`, `PostToolBatch`, `SubagentStop`, `Stop`, `StopFailure`, `PreCompact`, `SessionFork`, `Notification` | no event |

The observer omits `tool_kind`. It uses Muse's `session_id`, `turn_id`,
`tool_use_id` and `child_session_id` only as identifiers. A domain-separated
deterministic hash replaces a non-empty identifier that does not fit the wire
grammar. Every native event gets a fresh `mu-` reference with 128 random bits,
created once before the socket exchange and retained for all transport retries.

## Install for the current user

Build Gifford, put the resulting `gifford` executable on the hook process's
`PATH`, and run Muse under `gifford run`. Validate and install this plugin by
absolute path, then approve and enable its current capability definitions:

```console
muse plugins validate /absolute/path/to/gifford/adapters/muse
muse plugins install /absolute/path/to/gifford/adapters/muse --scope user
muse plugins approve gifford-muse-adapter
muse plugins enable gifford-muse-adapter
muse plugins inspect gifford-muse-adapter
```

The manifest uses structured argv and contains no workspace-relative script
path. All registered hooks are asynchronous, so Muse does not wait on the
observer. The command always exits successfully and writes nothing to standard
output; its complete socket exchange is independently bounded to 250 ms.

The hello identifies build `1.4.13-R6996.1` literally. Running the observer
under another Muse build causes Gifford to reject the hello with
`harness_mismatch` and continue with PTY evidence; it never silently accepts an
unmeasured hook schema.

## Conservative lifecycle boundaries

Muse invokes `PreToolUse` before permission handling and invokes
`ToolUseStart` again after approval. The adapter follows the same lifecycle
boundary as the Codex hook adapter: `PreToolUse` opens the tool and
`ToolUseStart` does not duplicate or renew it. `PostToolBatch` likewise does
not duplicate the preceding per-tool completion.

`PermissionRequest` has no hook-correlatable resolution. Each invocation gets
a fresh 128-bit `perm-` reference, independent of both the event ID and all
tool content. The open approval can veto delivery for five seconds from
Gifford's receipt; after that, native state is unknown and PTY evidence decides.
The adapter never invents `approval.resolved`.

`Stop`, `SubagentStop` and `PreCompact` can still be intercepted or request
continuation, so they do not prove that a turn, subagent or compaction
completed. `StopFailure` was not observed live in the measured study, and
`SessionFork` has no exact v2 native event. `Notification(question_prompt)`
has neither a stable request reference nor a matching resolution hook. These
hooks therefore produce no event instead of synthesizing a lifecycle edge.
`Interrupt` is the measured cancellation boundary and closes open operations
for its turn through `turn.completed cancelled`.

## What it never does

The observer never writes to the PTY, submits a prompt, presses a key, answers
a question, approves or denies a tool, changes permissions, or synthesizes a
missing close event. A missing socket, rejected hello, malformed or oversized
stdin, random-source failure, timeout or any other internal error produces no
native event and leaves Muse running normally.
