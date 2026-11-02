// Package fallback decides a notice without Glimmer. When the model is
// off, failed, has its circuit open or gave no analysis within
// AnalysisDeadline, the pane detector gives the agent's state instead.
//
// The detector replaces the model, never the controls. Precedence, from
// highest to lowest: channel controls (OFF or HOLD, MANUAL, rejection,
// expiry, budgets, a pending human approval), then the write guards, then
// the state judgement. A Glimmer failure never releases a human hold, and
// the same order rules every re-notice.
//
// The package is pure: no tmux, no clock, no I/O. The caller passes the
// facts and the current time and acts on the Decision. Anything uncertain
// fails closed: no write, and an uncertain earlier attempt is never
// re-injected before it is reconciled.
//
// What it does not do: it never opens an approval, never changes a
// control and never writes to a pane.
package fallback
