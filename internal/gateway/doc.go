// Package gateway is the Model Gateway: inference profiles, the client for
// Muse Glimmer served on Modal, and the cassettes used in tests (PRD §9.7,
// §18.8).
//
// Invariants: Nothing leaves the node without internal/redact. Tests replay
// cassettes and never call the real endpoint (R27).
//
// What it does not do: It does not decide; it returns distributions to the
// Analyst.
package gateway
