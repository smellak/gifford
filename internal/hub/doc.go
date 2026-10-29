// Package hub is the Local Hub: message admission, routing and persistence on
// one node (PRD §9.9).
//
// The local switchboard is a Unix socket speaking line-delimited JSON-RPC
// 2.0, with every connection bound to the agent the peer process descends
// from. Its four internal methods admit and persist opaque signed envelopes,
// expose them only to their recipients, and record acknowledgements.
//
// Invariants: Admission goes through channel.Admit inside the same transaction
// that stores the message (R49).
//
// What it does not do: It does not write the control tables directly. It
// never sees message plaintext and never writes agent_keys.
package hub
