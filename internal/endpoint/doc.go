// Package endpoint is the Agent Endpoint: the MCP server each agent talks to,
// and agent keys (PRD §9.8).
//
// Invariants: No MCP tool changes a channel control; the only related tool is
// request_loop_extension (R48).
//
// What it does not do: It does not deliver to a PTY.
package endpoint
