// Package channel is the Channel Control Plane: controls, precedence, gates,
// loops, budgets, approvals and explanations (PRD §9.18, Appendix L). See
// AGENTS.md in this directory.
//
// Invariants: Only this package writes channel_controls, communication_loops,
// channel_control_decisions, approval_requests and control_state (R47).
// Decision has no exported constructor (R46). It is deterministic: it never
// imports a model client or the Proxy (R49).
//
// What it does not do: It never calls a model and never touches a PTY.
package channel
