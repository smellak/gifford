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
