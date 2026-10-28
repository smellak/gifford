// Package adapters is contracts and client for Native Adapters of each harness
// (PRD §8).
//
// Invariants: An adapter declares its capabilities through the capability
// manifest.
//
// What it does not do: It does not replace the PTY path when an adapter is
// missing.
package adapters
