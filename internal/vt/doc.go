// Package vt is VT emulator shared by the Proxy and capture: screen
// reconstruction, snapshots, diffs, a normalised screen hash and input-line
// extraction (PRD §9.1).
//
// Invariants: Hashes are stable against clocks, spinners and counters.
//
// What it does not do: It never writes to a terminal. Replies to terminal
// queries are discarded, never sent anywhere.
package vt
