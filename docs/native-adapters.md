# Native adapter discovery

`gifford run --agent <agent_id>` discovers an optional native adapter through
a conventional private Unix socket. There is no command-line flag, environment
variable or persistent configuration field for the socket.

The path is:

```text
<runtime>/adapters/<agent_id>.sock
```

`<runtime>` is Gifford's resolved runtime directory (normally
`$XDG_RUNTIME_DIR/gifford`). The adapter socket must satisfy the local socket
and mode `0600` checks enforced by `internal/adapters`.

If the path does not exist, the run starts normally with the guaranteed
`generic_pty` manifest and prints no adapter notice. If the path exists,
Gifford completes the bounded adapter handshake before `register_agent`:

- a verified native manifest is sent unchanged both to the Hub and in the
  agent-socket greeting, provided local `config.yaml` carries the valid
  `node_id` needed to audit a later degradation;
- any connection, security, manifest or audit-identity failure keeps
  `generic_pty` and prints one notice before the harness takes over the
  terminal.

The PTY Proxy remains the input guard, capture path and delivery writer in
both cases. If a verified adapter dies during a run, its client degrades to
the `generic_pty` capability baseline without changing Draft Guard or
interrupting PTY delivery. The run retains one
`adapter.capabilities_changed` event with the original native registration
identity, `process_metrics` source quality and the exact generic capabilities
in `after`.

The event ID, envelope and canonical payload bytes remain unchanged for the
rest of the run. The agent socket emits it only after a valid
`register_agent_result`, once per Core connection, and replays that same event
after a reconnect. Applying it updates only the registered manifest's
capabilities and appends its audit event atomically; it does not revise the
session lifecycle timestamp, recycle the link or automatically enable a new
capability consumer.

## State-observation contract

Contracts v1.3 adds the Proxy-to-Core `report_state_observation` operation.
The frame is an observation, never a delivery decision. Its screen is either
`AVAILABLE`, after the adapter has excluded the current human-draft region, or
`WITHHELD`, with no screen bytes. It carries stable observation identity plus
observation, output and event generations so the Core can discard analysis
when any input changes.

The original `agent_frame` schema stays frozen; v1.3 peers use
`agent_frame_v1_3`. A Proxy sends no observation until the Core announces
`report_state_observation=true`. An older Core never sends that announcement,
so the Proxy keeps observation reporting disabled for the connection. PTY
capture and input guarding keep running independently.

Contracts v1.4 add the Core-to-Proxy `announce_support` operation and the
`agent_frame_v1_4` schema. When at least one additive receiver is wired, the
Core sends exactly one announcement immediately after `register_agent_result`.
It does not resend it on that connection; after reconnecting it sends one fresh
announcement because support is connection-local. When both functions are
false, the Core omits the announcement. An all-false body remains a valid
conservative representation for readers and fixtures, but is not emitted.

The Proxy retains the first announcement. If it receives a duplicate on the
same connection, it ignores the duplicate without responding and may count it
for diagnostics. It sends `report_state_observation` or general durable events
only when the corresponding announced field is true. An older Proxy may answer
the unknown announcement with a correlated `INVALID_FRAME`; the Core ignores
that compatibility response without cutting the link or retrying the
announcement.

For announced general durable events, `emit_event_result` correlates to the
source frame and carries `STORED`, `ALREADY_STORED` or terminal `REJECTED`.
`REJECTED` has only the fixed reasons `NOT_ALLOWED` and `CONFLICT`; no result
field can carry free-form diagnostics or event content.

The durable-event envelope is intentionally closed. It uses schema version 1,
the local node and authenticated connection's session, agent, harness, harness
version and adapter identities, plus the session's wall time, monotonic time and
sequence. Its source is `pty_screen`, stream is `pty`, sensitivity is `normal`
and `redaction_applied` is false. Turn, tool, message, decision, parent and
correlation lineage is absent. PTY lifecycle events have system origin; output
metadata has harness origin and `out` direction; human-input metadata has human
origin and `in` direction; delivery metadata has orchestrator origin and `in`
direction, with the envelope and payload carrying the same delivery ID. Capture
quota and gap markers have system origin and no direction. These two markers are
the only `capture.*` events allowed to use `pty_screen`; every other capture
event retains the existing bootstrap-source requirement, and `capture.chunk`
with `pty_screen` is rejected.

The Core also publishes one initial `UNKNOWN`, stale snapshot when a durable
session first becomes visible, using only the stored session, input guard,
channel, loop and approval state. It carries no screen bytes and makes no model
call. Its health is `DEGRADED` and delivery safety is `UNCERTAIN`; a connected
link alone never marks either healthy or safe. This keeps the human
`DEFER`/approval path available before an adapter has delivered its first
observation; later adapter reports supersede it.
