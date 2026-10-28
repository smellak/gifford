package channel

// Shared literals of the channel tests. goconst counts package-wide, so
// every test spells these values through the same constants and helpers
// instead of repeating the strings.
const (
	testNodeA    = "node-a"
	testNodeB    = "node-b"
	testThreadID = "thread-1"
	testLoopID   = "loop_01JB0000000000000000000003"
	testOperator = "operator"
	testObjectID = "msg_42"
)

// testGlobalKey returns the global version key.
func testGlobalKey() string { return "global" }

// testNodeKey returns the version key of a node.
func testNodeKey(node string) string { return "node:" + node }

// testAgentKey returns the version key of an agent.
func testAgentKey(agent string) string { return "agent:" + agent }

// Fixed version keys of the fixtures; §7.2 pins the link key exactly.
const (
	testLinkVersionKey           = "link:agent-1→agent-2:message"
	testThreadVersionKey         = "thread:thread-1"
	testLoopVersionKey           = "loop:loop_01JB0000000000000000000003"
	testLoopGenerationVersionKey = "loop_generation:loop_01JB0000000000000000000003"
	testLoopOverrideVersionKey   = "loop_override:loop_01JB0000000000000000000003"
)
