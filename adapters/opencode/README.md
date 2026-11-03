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

When the variable is set it wins untouched and no other candidate is tried.
Otherwise the plugin tries the `native.sock` of the runtime directory
first, then the `/run/gifford` system entry, which is accepted only when
no path component is a symlink, the directory is writable by neither
group nor others, and the leaf is a socket. Unlike the Claude hook, the
plugin does not read a `node_socket` path from the Gifford config; on a
node with a custom `node_socket`, set `GIFFORD_ADAPTER_SOCKET`. If no
candidate exists, the plugin remains silent and OpenCode continues
normally. A receiver disconnect starts a new hello and resets the
connection sequence; unacknowledged events retain their emitter ID while they
are retried.

OpenCode 1.18.29 reports normal completion through either an `idle`
`session.status` or the legacy `session.idle` event. The plugin emits
`turn.completed result=ok` only when one of those follows an active `busy`
turn. Initial or repeated idle events are ignored. `session.error` completes an
active turn as `failed`, except its explicit `MessageAbortedError`, which is
`cancelled`; a later idle event cannot replace either terminal result.

## Boundaries

The plugin never sends prompts, writes to the PTY, presses keys, answers
questions, approves permissions or changes a session. It never calls any
`tui.*` method. Prompt text, tool arguments and output, questions, answers and
other free text are neither read into an event nor sent over the socket.

Run its tests through the repository task:

```console
task adapters:opencode:test
```
