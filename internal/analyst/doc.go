// Package analyst is the Analyst: it renders the versioned prompt templates in
// prompts/ and asks Muse Glimmer for the agent state through the Gateway (PRD
// §9.5, §11).
//
// Invariants: Glimmer output is advisory; it never writes anywhere and never
// bypasses a guard.
//
// What it does not do: It does not deliver messages.
package analyst
