// Package agentsock is the agent socket of gifford run --agent: the
// Core's link to one wrapped session. It holds the frame codec, which
// the daemon shares, and, on Unix, the socket the session listens on,
// <runtime>/agents/<agent_id>.sock.
//
// The codec is stateless: a frame is the length of its JSON as 4 bytes
// big-endian, then the JSON of one protocol.AgentFrame, at most
// MaxFrameBytes, and reading validates it.
//
// The socket takes one connection at a time, from the same user only.
// On accept the session greets with register_agent and waits for its
// result. Registered, it reports the input guard on every new guard
// version, sends a heartbeat every 3 s and answers resync with the
// guard state. Any other order from the Core gets a frame error and has
// no effect.
//
// Invariants: No frame carries a human byte. The session loop never
// waits on the socket. One writer goroutine per connection writes every
// frame, with frame ids rising by one from 1.
package agentsock
