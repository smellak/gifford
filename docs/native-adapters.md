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
  agent-socket greeting;
- any connection, security or manifest failure keeps `generic_pty` and prints
  one notice before the harness takes over the terminal.

The PTY Proxy remains the input guard, capture path and delivery writer in
both cases. If a verified adapter dies during a run, its client degrades
locally to `generic_pty` and the run drains that transition without changing
Draft Guard or interrupting PTY delivery. Updating the already registered Hub
manifest and propagating `adapter.capabilities_changed` are intentionally
deferred until those protocols define an operation for it.
