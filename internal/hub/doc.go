// Package hub is the Local Hub: message admission, routing and persistence on
// one node (PRD §9.9).
//
// Invariants: Admission goes through channel.Admit inside the same transaction
// that stores the message (R49).
//
// What it does not do: It does not write the control tables directly.
package hub
