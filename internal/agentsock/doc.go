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
// version, sends a heartbeat every 3 s, answers resync with the guard
// state and takes the Core's support announcement. Any other order from
// the Core gets a frame error and has no effect.
//
// With an ObservationSource, it also reports state observations, but only
// on a connection whose Core has announced that it takes them. The first
// valid announce_support after the greeting fixes what the connection
// takes; a later one is ignored, never answered, and counted. Support is
// never inherited from an earlier connection, and the registration, the
// guard, the heartbeat and the deliveries never wait for it. Once a
// connection supports them, it first gets the last observation written
// again, with the same id and generations, or a first cut. It then cuts one
// when the source signals a change, at most every 250 ms, and when 1 s passes
// without one. A cut is written only when it validates, fits a frame and
// moves on from the last one: no generation or guard version goes back.
// Nothing is cut to fit.
//
// With a DurableEvents queue, it also writes durable events, but only on a
// connection whose Core has announced that it takes them, and one at a time:
// it writes the queue's head and waits for the Core's emit_event_result,
// which must answer that frame and name that event. The result settles the
// head, and the next one goes. Any other result is refused and settles
// nothing. A connection that ends first leaves the head in the queue, and the
// next connection writes it again, the same envelope with a new frame id. A
// head that is not of the connection, does not validate or does not fit a
// frame is refused, never cut. Without the queue or the announcement, nothing
// waits for a result, and a result is refused as before.
//
// Invariants: No frame carries a human byte; an observation's source
// removes the human draft before the cut. The session loop never waits on
// the socket. One writer goroutine per connection writes every frame,
// with frame ids rising by one from 1.
package agentsock
