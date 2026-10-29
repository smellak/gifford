// Package registry is the node registry and key directory (PRD §9.12, §15).
//
// It owns the agent_keys table: registering agent signing keys, reporting
// the single usable key, tracking blocks, and applying human review
// decisions. It reads agents and sessions but never writes them, and it
// never creates tables.
//
// Invariants: Entries are signed and versioned. Every state change is
// re-signed in the same transaction and every read verifies the signature;
// a row that does not verify counts as absent (fails closed).
//
// What it does not do: It does not transport messages, it does not publish
// the node key, and it ships no CLI (Review is API only).
package registry
