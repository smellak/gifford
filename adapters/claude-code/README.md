# Claude Code hooks

This observer supports exactly Claude Code 2.1.280. It translates the twelve
hooks available in that version into Gifford's content-free native lifecycle
events. It reads identifiers and closed enums only; prompts, tool inputs,
outputs, errors, notification messages and assistant text are discarded.

| Claude Code hook | Native event |
| --- | --- |
| `SessionStart` with `startup` or `resume` | `session.started` |
| `UserPromptSubmit` | `prompt.submitted` |
| `PreToolUse` | `tool.started` |
| `PermissionRequest` | `approval.requested` |
| `PostToolUse` | `tool.completed` |
| `PostToolUseFailure` | `tool.failed` |
| `Notification` | no event |
| `SubagentStart` | `subagent.started` |
| `Stop` | `turn.completed` with `ok` |
| `StopFailure` | `turn.completed` with `failed` |
| `PreCompact` | no event |
| `SessionEnd` | `session.ended` |

`AskUserQuestion` is the one tool with a different mapping. Its
`PreToolUse` becomes `question.requested`; `PostToolUse` and
`PostToolUseFailure` become `question.resolved` with `ok` and `failed`
respectively. All three use Claude Code's `tool_use_id` as the request
reference.

## Install

Run Claude Code under `gifford run`, make sure the matching `gifford` binary is
on the hook process's `PATH`, and merge the following `hooks` object into a
Claude Code settings file. This configuration adds hooks only. It does not add,
remove or change any `permissions` entry.

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "PermissionRequest": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "PostToolUseFailure": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "Notification": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "SubagentStart": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "StopFailure": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "PreCompact": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ],
    "SessionEnd": [
      {
        "hooks": [
          { "type": "command", "command": "gifford adapter hook claude-code", "timeout": 5 }
        ]
      }
    ]
  }
}
```

The command always exits successfully. Its complete socket exchange is bounded
to 250 ms, well inside the hook timeout. The hello identifies version 2.1.280
literally. Running this observer under a different Claude Code version causes
Gifford to reject the hello with `harness_mismatch` and continue with PTY
evidence; it never silently accepts an unmeasured hook schema.

## Permission limitation in 2.1.280

`PermissionRequest` does not include `tool_use_id`. Each invocation therefore
gets a fresh 128-bit `perm-` reference. The reference is not derived from tool
input and is not correlated with `PreToolUse`. Claude Code 2.1.280 also emits no
hook when the user selects **No**, so the observer never invents an
`approval.resolved` or `tool.failed` event. The open approval can veto delivery
for five seconds from Gifford's receipt; after that, native state is unknown and
the PTY evidence decides.

`Notification(permission_prompt)` never opens or renews an approval. It arrives
later than `PermissionRequest` and carries no identity suitable for a native
request reference.

## What it never does

The observer never writes to the PTY, submits a prompt, presses a key, answers a
question, approves or denies a tool, changes permissions, or synthesizes a
missing close event. A missing socket, rejected hello, malformed or oversized
stdin, random-source failure, timeout or any other internal error produces no
native event and leaves Claude Code running normally.
