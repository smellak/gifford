// Package proxy is PTY Proxy: it runs a harness on a child PTY, forwards bytes
// untouched, parses human input, keeps the Human Draft Guard and delivers
// injected text with a two-phase commit (PRD §9.1, §9.2).
//
// Invariants: Only writer.go writes to the child PTY. A human keystroke
// between prepare and commit aborts the delivery. No timeout ever closes a
// human draft. Time comes from internal/clock.
//
// What it does not do: It does not decide what to deliver and does not know
// the channel control plane exists (PRD §13.5).
package proxy
