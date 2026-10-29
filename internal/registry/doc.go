// Package registry is the node registry and key directory (PRD §9.12, §15).
//
// It owns nodes and agent_keys: pinning local and peer node keys,
// registering agent signing keys, importing signed peer directories,
// reporting the single usable key, tracking blocks, and applying human
// review decisions. It reads agents and sessions but never writes them,
// and it never creates tables.
//
// Invariants: Entries are signed and versioned. Local state changes are
// re-signed in their write transaction; remote changes are stored only with
// the issuer's valid signature. Every read verifies against the issuing
// node, and a row that does not verify counts as absent (fails closed).
//
// What it does not do: It does not transport messages, it does not publish
// the node key, and it ships no CLI (Review is API only).
package registry
