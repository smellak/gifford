// Package adapters is the local, length-delimited JSON link to a Native
// Adapter (PRD §8). It verifies the adapter's capability manifest at runtime,
// monitors the connection without ever becoming a PTY writer and exposes the
// harness probe used by the operator-only adapterprobe suite.
//
// Invariants: A native manifest is accepted only for the expected agent and
// harness, with a measured harness version, a native adapter type and the PTY
// Proxy as input guard. Any handshake or liveness failure returns to the
// generic PTY manifest. The adapter has no policy or persistence authority.
//
// What it does not do: It never writes to the PTY, replaces the PTY path,
// changes Draft Guard state or writes SQLite.
package adapters
