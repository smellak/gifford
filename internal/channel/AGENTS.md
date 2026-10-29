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
- Evaluation is a pure function of the cached controls, the `Query` and the
  injected clock: no I/O, no goroutines, no logging, no package-level maps or
  slices (precedence and reason classes are `switch` statements, R13).
- No exported function or method returns a `Decision`, except `Admit` in the
  allowlist of `TestDecisionAPISurface`; `Accept` returns an `Acceptance`, which
  exposes no `Decision`.
- The gates read the controls and the epoch through the caller's transaction
  into a view of their own; they never read, load or publish the shared cache.
  A reply is refused with `ErrUnsupported` and an approval verdict with
  `ErrApprovalUnavailable` until 5.7. The acceptance needs the message's
  admission decisions and never relaxes the stored triple; its final triple
  carries the current epoch. The caller commits only on success or on the
  gate's own `DenyError`.
- `writers_test.go` checks the single-writer rule for the control triple outside
  this package against an explicit inventory.
- The in-memory cache changes only through `Reload` and through unexported
  functions called after a compare-and-swap write; nothing outside the package
  can change a control in memory.
