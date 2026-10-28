// Package registry is the node registry and key directory (PRD §9.12, §15).
//
// Invariants: Entries are signed and versioned.
//
// What it does not do: It does not transport messages.
package registry
