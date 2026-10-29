// Package analyst is the Analyst: it reads the state of an agent from an
// observation (PRD §9.5, §11). A deterministic native route settles the
// phase from harness events or an exited process without a model; the
// universal PTY route preprocesses the screen and asks Muse Glimmer for
// ANALYZE_AGENT_STATE through the Gateway, with the versioned prompt
// template in prompts/ (embedded; its version travels in the payload).
//
// Invariants: Glimmer output is advisory; it never writes anywhere and never
// bypasses a guard. A failed model call gives UNKNOWN and marks the previous
// state stale: the Analyst never guesses. The screen is never rendered into
// the prompt; it travels as rows so the Gateway redacts it as a screen.
//
// What it does not do: It does not deliver messages, write to a terminal or
// touch the human draft.
package analyst
