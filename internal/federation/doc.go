// Package federation is node-to-node transport over mTLS with outbox, inbox,
// receipts and anti-replay (PRD §9.10, §15).
//
// Invariants: Remote controls are applied only through
// channel.ApplyRemoteControl.
//
// What it does not do: It does not decide delivery.
package federation
