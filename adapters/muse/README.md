# Muse Code hooks

This observe-only plugin's hook surface has been measured on Muse Code builds
`1.4.13-R6996.1` and `1.4.14-R7309.1`. The current emitter handshake remains
pinned to `1.4.13-R6996.1`; under 1.4.14 it fails closed to PTY evidence rather
than claiming an unverified native connection. It translates the measured hook
payloads into Gifford's content-free native lifecycle events. It retains only
identifiers and the closed `source` and `trigger` enums; prompts, tool inputs
and outputs, model text, paths, errors, reasons and notifications are discarded
while JSON is decoded.

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

The `turn.started` and `turn.streaming` events from `PreLLMCall` and
`PostLLMCall` are informational. They do not open a busy operation or veto
delivery; the decision remains on the PTY path.

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
output or standard error.

Muse 1.4.13 removes `GIFFORD_ADAPTER_SOCKET` from hook environments. On Linux,
when that variable is absent or empty, the observer discovers the listener only
for the measured process topology `hook → Muse 1.4.13 → gifford run`: exactly
two parent transitions. It first authenticates the run's private `agents`
socket, then accepts only the `native-events` socket for the same run, runtime
and agent. It never searches an outer run. Wrappers and other process
topologies are not covered by run discovery. As a last resort on Linux, when
run discovery finds no measured agent anchor — for example a Muse session
resumed in tmux without a run — the observer tries the documented system node
entry `/run/gifford/native.sock` under the same 250 ms deadline: no path
component may be a symlink, the system directory must not be writable by group
or others, the leaf must be a socket, and the dialed peer must run as the uid
that owned the entry when it was bound. A measured run whose own native entry
is missing still fails closed with `native_missing`; it never diverts to the
system entry. Anything else produces no native event. On other operating
systems there is no discovery fallback.

Every component of a discovered path must be real rather than a symlink. The
runtime and its `agents` and `native-events` directories must belong to the
effective user and have mode 0700; both socket leaves must belong to that user
and have mode 0600. Ancestors before the runtime may be group- or
other-writable and are never modified. The authenticated native socket remains
pinned by file descriptor while Gifford connects and revalidates its route.
Changing an ancestor cannot redirect the connection to a different socket, but
it can make the exchange unavailable. Route revalidation does not promise to
detect a transient move already restored before the next check.

A non-empty `GIFFORD_ADAPTER_SOCKET` is always authoritative, even when it is
invalid or unreachable, and never falls back to discovery or to the system
entry. Discovery, peer and process authentication, and the hello/event/ack
exchange share one 250 ms deadline.

After the `agents` anchor authenticates the private runtime, a failure may leave
an empty operator-only marker at
`<runtime>/hook-diagnostics/muse/<opaque-run-token>/<closed-reason>.marker`.
Directories are mode 0700 and marker files are mode 0600; creation is exclusive,
file-descriptor-relative and does not follow symlinks. A run session can have at
most 16 closed reason markers. Nothing reads or removes them automatically. On
a persistent filesystem such as ext4, they remain until an operator deletes
them or removes the runtime. Failures before the anchor authenticates cannot
leave a marker. An exhausted deadline never leaves a marker.

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
