package hub

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const endpointTestAgent = protocol.AgentID("agent-endpoint")

func TestEndpointDirectoryReplacementAndReleaseAreGenerationSafe(t *testing.T) {
	t.Parallel()

	var directory endpointDirectory
	first := EndpointIdentity{
		AgentID: endpointTestAgent, SessionID: "session-endpoint",
		Process:   EndpointProcess{PID: 101, StartTime: 201},
		Ancestors: []EndpointProcess{{PID: 100, StartTime: 200}},
	}
	firstLease := directory.bind(first)
	require.NotZero(t, firstLease)

	got, ok := directory.lookup(first.AgentID, first.SessionID)
	require.True(t, ok)
	require.Equal(t, first, got)
	got.Ancestors[0].PID = 999
	got, ok = directory.lookup(first.AgentID, first.SessionID)
	require.True(t, ok)
	require.Equal(t, int64(100), got.Ancestors[0].PID, "callers must not mutate retained ancestry")

	replacement := first
	replacement.Process = EndpointProcess{PID: 102, StartTime: 202}
	replacementLease := directory.bind(replacement)
	require.NotEqual(t, firstLease, replacementLease)
	directory.release(firstLease)
	got, ok = directory.lookup(first.AgentID, first.SessionID)
	require.True(t, ok, "closing a replaced connection must not remove its replacement")
	require.Equal(t, replacement.Process, got.Process)

	directory.release(replacementLease)
	_, ok = directory.lookup(first.AgentID, first.SessionID)
	require.False(t, ok)
}

func TestEndpointDirectoryScopesIdentityToAgentAndSession(t *testing.T) {
	t.Parallel()

	var directory endpointDirectory
	identity := EndpointIdentity{
		AgentID: endpointTestAgent, SessionID: "session-endpoint",
		Process: EndpointProcess{PID: 101, StartTime: 201},
	}
	lease := directory.bind(identity)
	require.NotZero(t, lease)
	for _, key := range []struct {
		agent   protocol.AgentID
		session protocol.OpaqueID
	}{
		{agent: "agent-other", session: identity.SessionID},
		{agent: identity.AgentID, session: "session-other"},
	} {
		_, ok := directory.lookup(key.agent, key.session)
		require.False(t, ok)
	}

	directory.revoke(identity.AgentID, identity.SessionID)
	_, ok := directory.lookup(identity.AgentID, identity.SessionID)
	require.False(t, ok)
	directory.release(lease)
}

func TestEndpointDirectoryRevokesEverySessionForAgent(t *testing.T) {
	t.Parallel()

	var directory endpointDirectory
	for index, session := range []protocol.OpaqueID{"session-one", "session-two"} {
		lease := directory.bind(EndpointIdentity{
			AgentID: endpointTestAgent, SessionID: session,
			Process: EndpointProcess{PID: int64(101 + index), StartTime: int64(201 + index)},
		})
		require.NotZero(t, lease)
	}
	directory.bind(EndpointIdentity{
		AgentID: "agent-other", SessionID: "session-other",
		Process: EndpointProcess{PID: 110, StartTime: 210},
	})

	directory.revokeAgent(endpointTestAgent)
	for _, session := range []protocol.OpaqueID{"session-one", "session-two"} {
		_, ok := directory.lookup(endpointTestAgent, session)
		require.False(t, ok)
	}
	_, ok := directory.lookup("agent-other", "session-other")
	require.True(t, ok)
}
