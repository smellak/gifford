// Package escalate records message escalations to the operator.
//
// The fallback loop calls Escalate in its own transaction when the
// escalation decision fires, then admits the content-free operator notice
// through normal admission keyed by ClientMsgID, which the hub deduplicates.
// Escalated lets the loop fill Facts.Escalated so each message escalates
// once. Nothing here writes outside the caller's transaction.
package escalate
