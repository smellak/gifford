// Package triage classifies queued messages with TRIAGE_MESSAGE before the
// Orchestrator decides on them (PRD §9.6), and keeps one verdict per
// message in message_triage.
//
// A message is classified once, and only while one of its deliveries could
// be delivered now. That holds when its delivery_queue row is QUEUED at
// that generation and unexpired, the delivery has no pending orchestrator
// approval, its agent has no unresolved UNCERTAIN attempt, and a fresh,
// read-only evaluation of the channel allows it. That check runs in one
// read-only transaction, which is closed before the model is called.
// An end-to-end encrypted or CONFIDENTIAL message is never sent: it gets an
// unclassified verdict and no call, and so does a message whose envelope
// cannot be read. Any other message goes to the Gateway as its subject and
// body, cut to maxTextRunes; the Gateway redacts that text before it leaves.
// Concurrent triages of one message share one call.
//
// Invariants: No transaction is open during a model call. The channel
// evaluation publishes no Decision, compares and swaps nothing and consumes
// no budget. The stored control triple is never read, so a control changed
// after the message was queued counts. A verdict is written once, and a
// trigger refuses any update. It holds no text: a label or none, an outcome
// and the call id. A failed call leaves an unclassified verdict and is not
// retried; only a call the caller cancels leaves no verdict. No text
// reaches a log, an error or the database.
//
// What it does not do: It does not decide, deliver, open approvals or write
// the control tables, and it never changes a priority. The Orchestrator
// reads the verdict with Lookup.
package triage
