// Package orchestrator is deterministic guards, the policy engine and the
// executor (PRD §9.6, §13).
//
// Invariants: executor.go is the only caller of delivery and it requires a
// channel.Decision with Allow as a typed parameter, revalidated right before
// acting (R46).
//
// What it does not do: It never writes the channel control tables.
package orchestrator
