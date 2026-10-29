// Package gateway is the Model Gateway: inference profiles, the client for
// Muse Glimmer served on Modal, and the cassettes used in tests (PRD §9.7,
// §18.8).
//
// Invariants: Nothing leaves the node without internal/redact, applied to
// the decoded payload field by field: screens by RedactScreen, text by
// RedactText, and an unclassified field is refused. If redaction fails,
// nothing is sent and the call is redaction_failed. A contract's prompt
// template is applied after redaction, to the sanitised payload. Every
// validated call leaves one complete GlimmerCallRecord, and only an ok call
// returns a label and a distribution. The endpoint URL and credential come
// from the environment at run time and never reach a log, an error, a
// record or an encoded config. Tests replay cassettes and never call the
// real endpoint (R27).
//
// What it does not do: It does not decide; it returns distributions to the
// Analyst. Prompt templates belong to the Analyst, and deploying Glimmer is
// not done here.
package gateway
