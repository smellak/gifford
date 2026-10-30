package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/protocol"
)

// TestReadControlMethodsAreNotMCPTools locks the trust boundary: panel reads
// are Control API methods on daemon.sock, never agent-facing MCP tools.
func TestReadControlMethodsAreNotMCPTools(t *testing.T) {
	t.Parallel()

	tools := make(map[string]bool)
	for _, tool := range endpoint.Tools() {
		tools[tool.Name] = true
	}
	for _, method := range []protocol.ControlMethod{
		protocol.ControlMethodAgentList,
		protocol.ControlMethodMessageList,
		protocol.ControlMethodDecisionList,
		protocol.ControlMethodDecisionGet,
		protocol.ControlMethodModelStatus,
	} {
		require.False(t, tools[string(method)], "%s must not be exposed through MCP", method)
	}
}
