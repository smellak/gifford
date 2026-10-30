// Package agentlink owns the node side of one Proxy agent socket.
//
// A link authenticates both the socket file and its process before answering
// the Proxy greeting, binds that greeting to one live Hub session, persists
// every input-guard report and monitors Proxy heartbeats. It reconnects with
// bounded backoff and reapplies the last requested automation pause state.
//
// The package is transport only. It does not construct delivery orders,
// decide whether a delivery is allowed or write to a PTY. The orchestrator
// executor owns delivery construction and supplies already-authorized frames
// through Send or Call.
package agentlink
