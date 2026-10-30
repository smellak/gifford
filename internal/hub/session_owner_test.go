package hub

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestSessionOwnerReplacementMakesLateReleaseHarmless(t *testing.T) {
	t.Parallel()

	var owners sessionOwnerDirectory
	const (
		agent   = protocol.AgentID("agent-owner")
		session = protocol.OpaqueID("session-owner")
	)
	first := owners.bind(agent, session)
	require.NotZero(t, first)
	require.True(t, owners.current(first))
	second := owners.bind(agent, session)
	require.NotZero(t, second)
	require.NotEqual(t, first, second)
	require.False(t, owners.current(first))
	require.True(t, owners.current(second))
	require.False(t, owners.release(first), "the replaced transport no longer owns lifecycle")
	require.True(t, owners.current(second))
	require.True(t, owners.release(second))
	require.False(t, owners.current(second))
}
