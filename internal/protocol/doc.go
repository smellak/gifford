// Package protocol is shared types of the protocol and their schema_version:
// event envelope, capture events, capability manifest, state snapshot, logical
// message and wire envelope, orchestrator decision, Core-Proxy orders, Gateway
// contracts and the channel control types (PRD §10, §14, §18, Appendix L).
//
// Invariants: It is frozen at the contracts-v1 tag. Any change is a breaking
// change: it bumps schema_version, updates the fixtures in testdata/ and the
// JSON Schemas in schemas/, and notifies every package owner.
//
// What it does not do: It holds no behaviour beyond validation; it never
// performs I/O.
package protocol
