// Package eventstore is the SQLite (WAL) event store with embedded migrations
// from /migrations (PRD §10).
//
// It opens one SQLite file per node in WAL mode through the pure-Go driver
// modernc.org/sqlite, so the binary stays static, applies the migrations
// embedded by package migrations and appends event envelopes in batches.
//
// Invariants:
//   - Events are append-only: triggers refuse UPDATE and DELETE on events,
//     and recursive_triggers makes INSERT OR REPLACE fire them as well.
//   - An event_id and a (session_id, sequence) are stored at most once; a
//     duplicate refuses its whole batch with ErrDuplicateEvent.
//   - A batch is validated before any write and stored in one BEGIN
//     IMMEDIATE transaction: all of it or none of it.
//   - Every connection runs with journal_mode WAL, busy_timeout 5000 ms,
//     foreign_keys on, synchronous NORMAL and recursive_triggers on.
//   - One coordinated writer per process; other processes wait for the
//     write lock up to busy_timeout.
//   - Every migration goes up and down; the runner fails closed when the
//     database and the embedded migrations disagree.
//   - Times are TEXT in TimeLayout (UTC, nine fractional digits), so text
//     order is time order.
//
// What it does not do: It writes only events. The migrations create the
// tables of every owner (design §17), but each table other than events is
// written by its owner package through DB(); the channel control tables
// only by internal/channel (R47). It never reads the clock, never rewrites
// or deletes an event, and keeps no in-memory buffer: backpressure (design
// §23.5) comes later.
package eventstore
