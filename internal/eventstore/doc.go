// Package eventstore is the SQLite (WAL) event store with embedded migrations
// from /migrations (PRD §10).
//
// Invariants: Batched writes; busy_timeout of five seconds; migrations go up
// and down.
//
// What it does not do: It does not write the channel control tables; only
// internal/channel does (R47).
package eventstore
