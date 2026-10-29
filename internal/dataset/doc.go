// Package dataset is dataset build, inspect and redact: manifests, splits and
// the readiness gate before training (PRD §9.16).
//
// Window is the labelled ANALYZE_AGENT_STATE example: input, A-G target,
// provenance, domain, policy versions and split, with no raw bytes and
// no human content. WINDOW.md states the contract and what stays open.
//
// Invariants: Nothing enters a dataset without passing internal/redact.
//
// What it does not do: It does not train models; ml/ does, offline.
package dataset
