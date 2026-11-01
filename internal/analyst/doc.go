// Package analyst is the Analyst: it reads the state of an agent from an
// observation (PRD §9.5, §11). A deterministic native route settles the
// phase from harness events or an exited process without a model; the
// universal PTY route builds the dataset.AnalyzeInput of the observation,
// the model input of training as is, and asks Muse Glimmer for
// ANALYZE_AGENT_STATE through the Gateway. Its screen_lines are the screen
// as screenproj.Project projects it under the policies the dataset build
// runs with: the human input excluded before anything leaves the node,
// the whole screen redacted with its width and domain-masked, then cut.
// For the same screen, input guard snapshot and policies, the payload is
// the training one byte for byte. Under the analyst-lora profile its
// previous_state stays empty, as in every dataset window. The Gateway redacts it again, as a
// second defence, records the call and renders the prompt.
//
// The versioned prompt templates live in prompts/ (embedded), one per
// model contract: a fixed system message and a user message rendered by
// Prompts, which the Gateway applies to the payload after redaction. The
// messages only hold sanitised values, and the call's input digest covers
// them and their version as sent. The answer letters of
// ANALYZE_AGENT_STATE (A–G) and TRIAGE_MESSAGE (A–D) are fixed. Those of
// CHOOSE_ORCHESTRATION_ACTION follow the request's allowed_actions, A
// first, so the endpoint reads them from the payload sent with the prompt.
// The Analyst itself only asks ANALYZE_AGENT_STATE; the other two
// templates serve the callers of those contracts.
//
// Invariants: Glimmer output is advisory; it never writes anywhere and never
// bypasses a guard. A failed model call gives UNKNOWN and marks the previous
// state stale: the Analyst never guesses. Without a valid input guard
// snapshot of the agent at the capture's input_guard_version, or when the
// projection refuses the screen, nothing reaches the Gateway: UNKNOWN, the
// previous state stale, ErrScreenWithheld.
//
// What it does not do: It does not deliver messages, write to a terminal,
// touch the human draft or change the input guard: it reads a copy of the
// guard's last report.
package analyst
