// Package crypto is E2E profiles and canonical envelopes (PRD §14).
//
// Invariants: Canonical encoding is deterministic; keys never appear in logs.
//
// What it does not do: It does not manage transport; internal/federation does.
package crypto
