# internal/channel — rules for agents

- `Decision` is an immutable value with no exported constructor: only the gates in
  this package produce one. Delivery code takes it as a typed parameter and calls
  `Revalidate` right before acting (R46).
- Only this package writes `channel_controls`, `communication_loops`,
  `channel_control_decisions`, `approval_requests` and the
  `control_state`/`control_reason`/`control_version` of messages and artifacts.
  Queries live in `queries/*.sql`, embedded (R47).
- No MCP tool changes a control. The only related tool is `request_loop_extension`,
  which only creates or annotates an approval request (R48).
- Deterministic by construction: never import internal/gateway, internal/analyst,
  any model client, or internal/proxy. depguard enforces it (R49).
- Loop counters are reserved in the same transaction that stores the message: the
  Hub opens the transaction and passes the `*sql.Tx` to `Admit`.
