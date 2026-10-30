// Package proxies reconciles live PTY Proxy sessions with node-side agent
// links. It starts exactly one link for each connected, deliverable pty_proxy
// session and stops it as soon as reconciliation no longer finds that session.
//
// The registry owns no delivery policy and constructs no delivery frames. It
// exposes link state and the latest persisted report to later daemon stages.
package proxies
