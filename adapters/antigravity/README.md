# Antigravity hooks

This observe-only adapter supports exactly Antigravity CLI 1.4.8. It translates
six hooks into Gifford's content-free native lifecycle events. It does not
change the native-event wire, make permission decisions, or infer lifecycle
edges that Antigravity did not report.

| Antigravity hook | Native event |
| --- | --- |
| `SessionStart` | `session.started` |
| `PreInvocation` | `turn.started` |
| `PostInvocation` | `turn.streaming` |
| `PreToolUse` | `tool.started` |
| `PostToolUse` with an absent or empty `error` | `tool.completed` with `ok` |
| `PostToolUse` with a non-empty `error` | `tool.failed` with `failed` |
| `Stop` with an explicitly present `fullyIdle=false` | `background_task.started` |
| `Stop` with `fullyIdle=true` | no event |

`PreInvocation` and `PostInvocation` describe one model invocation. The latter
does not prove that the agent loop or human turn ended. `PreToolUse` occurs
before permission resolution, so it does not prove that a tool was allowed.
`Stop` does not prove that a turn, session, or background task ended. The
adapter therefore never emits `turn.completed`, `session.ended`, an approval
event, or `background_task.completed`.

The background event can veto delivery for five seconds from Gifford's durable
receipt. Expiry makes native state unknown; it does not synthesize an idle or
completed state. `tool.failed` closes only the corresponding tool operation and
does not create a failed turn.

## Strict identity rules

Antigravity's identifiers and counters are accepted only with their documented
JSON types. Zero is a valid `invocationNum`, `stepIdx`, or `executionNum`.
Missing, null, non-integer, or negative values do not mean zero and produce no
event. A missing `fullyIdle` does not mean false. Missing or malformed required
fields always produce the neutral hook response and no event. An absent
`PostToolUse.error` is allowed, but a present value must be a string; null or
another JSON type produces no event.

The adapter uses `conversationId` as the session reference when it fits the
wire grammar. Otherwise it uses a domain-separated SHA-256 digest. Invocation,
tool, and execution references are domain-separated digests of the emitter
version, conversation ID, counter name, and canonical decimal counter. Every
event also gets a fresh `ag-` reference with 128 random bits.

Tool events deliberately omit `turn_ref`. The public hook payload does not
prove which model invocation owns a tool call, and the one-shot adapter never
chooses the most recent invocation by timing or proximity.

## Neutral control responses

The hook name is a closed command argument because Antigravity does not include
it in stdin. Each command writes exactly one JSON object followed by LF:

| Hook | Exact stdout |
| --- | --- |
| `SessionStart` | `{"injectSteps":[]}` |
| `PreToolUse` | `{"decision":"allow"}` |
| `PostToolUse` | `{}` |
| `PreInvocation` | `{"injectSteps":[]}` |
| `PostInvocation` | `{"injectSteps":[],"terminationBehavior":""}` |
| `Stop` | `{"decision":"allow"}` |

For `PreToolUse`, `allow` delegates to the next hook or Antigravity's default
permission policy; it does not grant permission. No response contains
`auto_approve`, a permission override, rewritten arguments, injected steps, a
reason, or a continuation request.

Malformed or oversized stdin, a missing socket, a rejected hello, a timeout, a
random-source failure, an internal error, or a panic produces no native event.
The command still writes the same neutral response, writes nothing to stderr,
and exits successfully. Its complete socket exchange is bounded to 250 ms.
What Antigravity itself does when the process is killed or exceeds the host
timeout must be established by a live acceptance run; this adapter does not
promise host behavior it cannot control.

## Install for the measured user

Build Gifford, run Antigravity under `gifford run`, and atomically merge the
following named group into the current user's
`~/.gemini/config/hooks.json`. Replace `/absolute/path/to/gifford` with the
absolute path of the measured binary. Preserve unrelated hook groups. The home
and configuration directory must be mode `0700`, and the file must be mode
`0600`.

```json
{
  "gifford-antigravity-observer": {
    "enabled": true,
    "SessionStart": [
      {
        "type": "command",
        "command": "/absolute/path/to/gifford adapter hook antigravity SessionStart",
        "timeout": 1
      }
    ],
    "PreInvocation": [
      {
        "type": "command",
        "command": "/absolute/path/to/gifford adapter hook antigravity PreInvocation",
        "timeout": 1
      }
    ],
    "PostInvocation": [
      {
        "type": "command",
        "command": "/absolute/path/to/gifford adapter hook antigravity PostInvocation",
        "timeout": 1
      }
    ],
    "PreToolUse": [
      {
        "matcher": "*",
        "hooks": [
          {
            "type": "command",
            "command": "/absolute/path/to/gifford adapter hook antigravity PreToolUse",
            "timeout": 1
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "*",
        "hooks": [
          {
            "type": "command",
            "command": "/absolute/path/to/gifford adapter hook antigravity PostToolUse",
            "timeout": 1
          }
        ]
      }
    ],
    "Stop": [
      {
        "type": "command",
        "command": "/absolute/path/to/gifford adapter hook antigravity Stop",
        "timeout": 1
      }
    ]
  }
}
```

Do not put this group in
`~/.gemini/antigravity-cli/settings.json`, and do not add the run-scoped
`GIFFORD_ADAPTER_SOCKET` to either file. `gifford run --agent` creates the
private socket and exposes it only to the measured harness process and its
descendants. The hello identifies `harness=antigravity`, version `1.4.8`,
`emitter_type=hook`, emitter version `1`, and native schema version `2`.
A version mismatch is rejected and leaves the PTY path authoritative.

## Acceptance safety stop

A live acceptance run must use only test files. A successful tool action must
already be allowed by policy, such as one of the configured Gifford MCP tools
or a default-permitted read. A failing action should read a test file that does
not exist. The run must never accept a permission dialog; denying one is safe.
The `fullyIdle=false` case counts only when it occurs without approving
anything.

If a tool that should ask for permission runs without displaying its menu,
stop the run immediately, remove the entire hook group, and reject this adapter
for that Antigravity version. The same full removal applies if any hook changes
arguments, results, stop behavior, or permission handling. Leaving a
control-altering hook installed while merely suppressing its native events is
not a safe fallback.

## Privacy and training boundary

The decoder projects only `conversationId`, the three integer counters,
`fullyIdle`, and whether `error` is empty. It discards prompts, user input,
workspace and transcript paths, artifact paths, tool names and arguments,
tool output, model names, termination text, and error text. It never opens a
transcript or artifact directory, hashes content, or logs hook input.

These events are operational observations, not training labels. In particular,
`PreToolUse` is not a positive tool-running target and `PostInvocation` is not
proof that the agent is still thinking. Any future use for dataset labels must
be enabled separately for a verified harness version and semantics. Until
then, the signals are informational or an operational veto only.
