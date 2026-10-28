// Package redact is local redactor used inline by the Gateway and offline by
// the dataset tools.
//
// Invariants: Placeholders keep the length so the screen layout is unchanged;
// every output carries redaction_policy_version; a redactor failure is an
// error, never an unredacted screen.
//
// What it does not do: It does not decide what leaves the node; the Gateway
// does, and only after redaction.
package redact
