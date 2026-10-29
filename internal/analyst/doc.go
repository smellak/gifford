// Package analyst is the Analyst: it reads the state of an agent from an
// observation (PRD §9.5, §11). A deterministic native route settles the
// phase from harness events or an exited process without a model; the
// universal PTY route builds the dataset.AnalyzeInput of the observation,
// the model input of training as is, with the whole screen, and asks Muse
// Glimmer for ANALYZE_AGENT_STATE through the Gateway, which redacts the
// whole screen, records the call and only then cuts the rows the model
// sees.
//
// The versioned prompt templates live in prompts/ (embedded): a fixed
// system message and a user message rendered by Prompts, which the Gateway
// applies to the payload after redaction. The messages only hold sanitised
// values, and the call's input digest covers them and their version as
// sent.
//
// Invariants: Glimmer output is advisory; it never writes anywhere and never
// bypasses a guard. A failed model call gives UNKNOWN and marks the previous
// state stale: the Analyst never guesses.
//
// What it does not do: It does not deliver messages, write to a terminal or
// touch the human draft.
package analyst
