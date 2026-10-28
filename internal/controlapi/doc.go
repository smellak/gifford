// Package controlapi is the local JSON-RPC control API used by gifford ctl and
// the panel (PRD §9.14).
//
// Invariants: Control changes go through internal/channel methods, never raw
// SQL (R47).
//
// What it does not do: It does not render anything.
package controlapi
