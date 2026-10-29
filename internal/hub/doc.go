// Package hub is the Local Hub: message admission, routing and persistence on
// one node (PRD §9.9).
//
// This skeleton (subphase 2.4, part one) is the local switchboard: a
// Unix socket speaking line-delimited JSON-RPC 2.0, with every connection
// bound to the agent the peer process descends from. The four internal
// methods sit behind small handler interfaces that later subphases fill;
// until then they answer -32603.
//
// Invariants: Admission goes through channel.Admit inside the same transaction
// that stores the message (R49).
//
// What it does not do: It does not write the control tables directly. It
// never sees message plaintext and never writes agent_keys: the socket
// layer routes opaque params to the handlers and owns no SQL of its own.
package hub
