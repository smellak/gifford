// Package dataset is dataset build, inspect and redact: manifests, splits and
// the readiness gate before training (PRD §9.16).
//
// Invariants: Nothing enters a dataset without passing internal/redact.
//
// What it does not do: It does not train models; ml/ does, offline.
package dataset
