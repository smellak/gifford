# Codex hooks

This observer supports exactly Codex CLI 0.154.0. It translates that version's
lifecycle hooks into Gifford's content-free native events. It retains only
identifiers and closed enums; prompts, tool inputs and outputs, assistant text,
paths, models and permission descriptions are discarded while JSON is decoded.

| Codex hook | Native event |
| --- | --- |
| `SessionStart` with `startup`, `resume` or `clear` | `session.started` |
| `SessionStart` with `compact` | no event |
| `SessionEnd` | `session.ended` |
| `UserPromptSubmit` | `prompt.submitted` |
| `PreToolUse` | `tool.started` |
| `PermissionRequest` | `approval.requested` |
| `PostToolUse` | `tool.completed` with `ok` |
| `Stop` | no event |
| `SubagentStart` | `subagent.started` |
| `SubagentStop` | no event |
| `PreCompact` | no event |
| `PostCompact` | `context.compaction_completed` |
| `Interrupt` | `turn.completed` with `cancelled` |

The observer omits `tool_kind`. It uses Codex's `session_id`, `turn_id` and
`tool_use_id` as native references when they fit the wire grammar. A
domain-separated deterministic hash replaces a non-empty identifier that does
not fit. Every native event gets a fresh `cx-` reference with 128 random bits,
created once before the socket exchange and retained for all transport retries.

## Install

Run Codex under `gifford run`, make sure the matching `gifford` binary is on the
hook process's `PATH`, and put the following object in either
`$CODEX_HOME/hooks.json` (normally `~/.codex/hooks.json`) or the project's
`.codex/hooks.json`. Merge it with any hooks already present. Equivalent inline
TOML under `[hooks]` in `config.toml` is also supported.

```json
{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "^(startup|resume|clear)$",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "SessionEnd": [
      {
        "matcher": "^other$",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "PermissionRequest": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "SubagentStart": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "PostCompact": [
      {
        "matcher": "^(manual|auto)$",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ],
    "Interrupt": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook codex", "timeout": 1 }
        ]
      }
    ]
  }
}
```

Codex does not run non-managed hooks until their current definitions have been
reviewed and accepted as trusted. Use Codex's `/hooks` command to review this
configuration after installing or changing it. The configuration adds hooks
only. It does not add, remove or alter permissions or `permission_mode`.

The command always exits successfully and writes nothing to standard output.
Its complete socket exchange is bounded to 250 ms. The hello identifies version
0.154.0 literally. Running it under another Codex version causes Gifford to
reject the hello with `harness_mismatch` and continue with PTY evidence; the
observer never silently accepts an unmeasured hook schema.

## Open-event limitations

Codex 0.154.0 does not include a tool or request ID in `PermissionRequest`.
Each invocation therefore gets a fresh 128-bit `perm-` request reference. It is
not derived from tool input and is not correlated with `PreToolUse`. Codex also
has no hook for the later approval decision, so the observer never invents an
`approval.resolved` event. The open approval can veto delivery for five seconds
from Gifford's receipt; after that, native state is unknown and PTY evidence
decides.

`PreToolUse` runs before all hook and permission decisions are resolved. If a
tool is then blocked, denied or fails inside its handler, Codex emits no
`PostToolUse`; the open tool has the same bounded five-second veto. `Interrupt`
does close open operations for its turn through `turn.completed cancelled`.
A shell command that exits non-zero still emits `PostToolUse` in this Codex
version, so `tool.completed ok` means the invocation completed, not that the
shell exit code was zero.

`Stop`, `SubagentStop` and `PreCompact` can be intercepted by another hook and
therefore do not prove that a turn, subagent or compaction completed. The
observer emits no event for them instead of synthesizing a closure.

## What it never does

The observer never writes to the PTY, submits a prompt, presses a key, answers a
question, approves or denies a tool, changes permissions, or synthesizes a
missing close event. A missing socket, rejected hello, malformed or oversized
stdin, random-source failure, timeout or any other internal error produces no
native event and leaves Codex running normally.
