// Package agentlink owns the node side of one Proxy agent socket.
//
// A link authenticates both the socket file and its process before answering
// the Proxy greeting, binds that greeting to one live Hub session, persists
// every input-guard report and monitors Proxy heartbeats. It reconnects with
// bounded backoff and reapplies the last requested automation pause state.
//
// A state observation must validate and name the link's own agent and
// session, or the connection is dropped. The link persists none: it hands the
// newest one to an optional ObservationSink from its own goroutine, so a busy
// sink never stalls the socket reader and only the latest waiting report is
// kept.
//
// The package is transport only. It does not construct delivery orders,
// decide whether a delivery is allowed or write to a PTY. The orchestrator
// executor owns delivery construction and supplies already-authorized frames
// through Send or Call.
package agentlink
