// Package channel is the Channel Control Plane: controls, precedence, gates,
// loops, budgets, approvals and explanations (PRD §9.18, Appendix L). See
// AGENTS.md in this directory.
//
// It keeps versioned controls for six scopes (global, node, agent, link,
// thread and loop), resolves the effective control for a sender → receiver
// pair on one plane, and turns it into an immutable Decision with a typed
// reason, the matched controls and the version vector it was taken with.
// The strongest restriction wins: OFF and DENY beat HOLD, HOLD beats
// REQUIRE_APPROVAL and REQUIRE_APPROVAL beats ALLOW; a lower scope can only
// harden a higher one.
//
// Invariants:
//   - No model ever changes a control. The package never imports a model
//     client, internal/gateway, internal/analyst or internal/proxy (R49).
//   - Nothing is delivered without a Decision whose outcome is ALLOW. Only
//     this package builds a Decision; it has no exported constructor (R46).
//   - The effective restriction is always the strongest one.
//   - A remote peer never relaxes a local control.
//   - OFF deletes nothing and leaves no partial PTY line.
//   - Any control change between decision and commit invalidates the delivery.
//   - STEP consumes exactly one token per authorized continuation, at delivery.
//   - An idempotent retry never consumes extra budget.
//   - An exhausted loop grows only by human action or administrative policy.
//   - A STOPPED loop is never resumed; a new generation starts instead.
//   - Controls survive restarts and are rebuilt from the event store.
//   - The TUI and the CLI show the effective state, not the value of one row.
//   - Every reply between agents belongs to a loop with a finite budget.
//   - No MCP tool changes a control. Only this package writes
//     channel_controls, communication_loops, channel_control_decisions,
//     approval_requests and the control fields of messages and artifacts;
//     its SQL lives in queries/*.sql (R47, R48).
//   - If the controls cannot be read, every evaluation answers HOLD with
//     CONTROL_UNAVAILABLE.
//   - Every evaluation judges every row at one instant.
//   - A gate's view is its own; the shared cache is never published from a
//     transaction.
//   - The Human Draft Guard, the writer lease and the two-phase commit are
//     not changed by this package.
//
// What it does not do: It never calls a model, never touches a PTY, never
// reads the wall clock (it receives a clock.Clock) and never deletes a queued
// message or artifact.
package channel
