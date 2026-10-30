// Package notifications is the Human Notification Service and its adapters
// (PRD §9.13).
//
// Invariants: Notifications never carry unredacted screens or secrets.
// Eligible retained messages are HELD and APPROVAL_REQUIRED; DENIED messages
// are final and are not notified. A pending approval for the same message wins
// over the message notice so one object does not create two visible notices.
// The durable outbox is logically exactly-once, but the external tmux effect is
// at-least-once: a crash after display-message succeeds and before RENDERED is
// committed can repeat the same notification id.
//
// What it does not do: It does not deliver to agents.
package notifications
