// Package migrations embeds the SQL migrations of the event store (R8):
// one <YYYYMMDDHHMMSS>_<name>.sql file per schema change, with goose-style
// up and down sections, applied by internal/eventstore in version order.
//
// Invariants: A merged file is never edited, renamed or deleted; a change
// is a new file whose UTC timestamp sorts after every existing one. Every
// file has an up and a down section.
//
// What it does not do: It holds no logic. internal/eventstore parses,
// validates and applies the files.
package migrations
