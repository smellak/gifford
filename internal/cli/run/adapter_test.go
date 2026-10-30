package run

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// TestHarnessesRegisterGenericPTY locks the guaranteed baseline for every
// concrete harness. An absent native handshake never grants native_cancel.
func TestHarnessesRegisterGenericPTY(t *testing.T) {
	for _, harness := range []protocol.Harness{
		protocol.HarnessClaudeCode,
		protocol.HarnessCodex,
		protocol.HarnessMuseCode,
		protocol.HarnessOpencode,
		protocol.HarnessAntigravity,
	} {
		t.Run(string(harness), func(t *testing.T) {
			reg, err := agentRegistration("agent-1", string(harness), "1.2.3")
			require.NoError(t, err)
			require.NoError(t, reg.Validate())
			manifest := reg.Manifest
			require.Equal(t, protocol.AgentID("agent-1"), manifest.AgentID)
			require.Equal(t, harness, manifest.Harness)
			require.Equal(t, protocol.AdapterTypeGenericPTY, manifest.AdapterType)
			require.Equal(t, protocol.InputGuardProviderPTYProxy, manifest.InputGuardProvider)
			require.Equal(t, protocol.CaptureModePTYProxy, manifest.CaptureMode)
			require.True(t, manifest.Deliverable)
			require.True(t, manifest.Capabilities.PTYCapture)
			require.True(t, manifest.Capabilities.MCP)
			require.False(t, manifest.Capabilities.NativeCancel)
		})
	}
}
