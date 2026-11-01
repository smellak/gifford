# OpenCode native-events plugin

This observe-only plugin targets OpenCode 1.18.29. It reports content-free
session, turn, tool, approval, question, notification and compaction lifecycle
events through Gifford's native-events v2 socket.

## Install

Copy `gifford-native-events.js` into an OpenCode 1.18.29 `plugins/` directory,
either under the project `.opencode/` directory or an isolated
`OPENCODE_CONFIG_DIR`. The plugin uses only runtime-provided Node APIs and has no
package dependencies. Run OpenCode through `gifford run`; Gifford supplies
`GIFFORD_ADAPTER_SOCKET` to the harness process for that run.

If the variable or socket is absent, the plugin remains silent and OpenCode
continues normally. A receiver disconnect starts a new hello and resets the
connection sequence; unacknowledged events retain their emitter ID while they
are retried.

## Boundaries

The plugin never sends prompts, writes to the PTY, presses keys, answers
questions, approves permissions or changes a session. It never calls any
`tui.*` method. Prompt text, tool arguments and output, questions, answers and
other free text are neither read into an event nor sent over the socket.

Run its tests through the repository task:

```console
task adapters:opencode:test
```
