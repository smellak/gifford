// Package renotify runs the no-Glimmer backup loop: once per turn it asks
// fallback.Decide for every pending message and acts on the Action and
// Next it gets back. A due slot triggers one renotice through the normal
// path; an unread message past the escalation deadline triggers one
// operator notice, filed as the node's system sender, and then one
// durable escalation mark in its own transaction. A crash between the
// two retries the filing under the same notice key, so the escalation
// is neither lost nor doubled.
//
// The package owns no pane and no schedule of its own: the judgement
// comes from fallback, whose precedence is defined once in decide. Facts
// arrive loaded by the caller; the worker wiring supplies them. The loop
// never writes with a control set, never notifies without grounds and
// never re-notifies a covered slot: it only carries out Decisions.
package renotify
