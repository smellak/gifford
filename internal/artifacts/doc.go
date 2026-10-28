// Package artifacts is the Artifact Relay: local and federated file transfer
// (PRD §9.11, §16).
//
// Invariants: ready.go is the only emitter of FILE_READY and it requires a
// channel.Decision with Allow (R46).
//
// What it does not do: It does not write the control tables.
package artifacts
