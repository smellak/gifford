//go:build linux

package hub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const lateWhoAmI = `{"jsonrpc":"2.0","method":"whoami","id":1}`

// TestLateWhoAmIBindsByLineageWithoutOwnership proves a connection that
// arrives before its session registers learns the agent on a later whoami,
// by the lineage accept uses, and owns nothing: closing it leaves the
// session connected and its owner bound.
func TestLateWhoAmIBindsByLineageWithoutOwnership(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-late-lineage")
	child := dialChild(t, fixture.sock)
	child.send(t, lateWhoAmI)
	require.Equal(t, codeNotReady, errorCode(t, child.recv(t)), "no session yet, no identity")

	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	owner := dialRegistrationConn(t, fixture.sock)
	defer func() { _ = owner.Close() }()
	session := registeredSessionID(t, registrationExchange(t, owner,
		registerAgentLine(t, 1, registrationManifest(agent), self)))

	child.send(t, lateWhoAmI)
	require.Equal(t, string(agent), child.recv(t)["result"].(map[string]any)["agent_id"])
	child.close(t)
	waitServerConnections(t, fixture.server, 1)
	requireSessionLifecycle(t, fixture, session, protocol.SessionStateConnected, false)
	whoami := registrationExchange(t, owner, lateWhoAmI)
	require.Equal(t, string(agent), whoami["result"].(map[string]any)["agent_id"],
		"the owner keeps its lease")
}

// TestLateWhoAmIKeepsAcceptRulesAndNeverMoves proves a late whoami applies
// the accept rules, so the session's own process still has to register, and
// that the late binding is final: when its session detaches and another
// agent's session becomes the connected one of the same lineage, the
// connection is dropped instead of answering as that agent.
func TestLateWhoAmIKeepsAcceptRulesAndNeverMoves(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	child := dialChild(t, fixture.sock)
	child.send(t, lateWhoAmI)
	require.Equal(t, codeNotReady, errorCode(t, child.recv(t)))
	root := dialRegistrationConn(t, fixture.sock)
	defer func() { _ = root.Close() }()
	require.Equal(t, codeNotReady, errorCode(t, registrationExchange(t, root, lateWhoAmI)))

	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	insertLifecycleSession(t, fixture, "session-late-a", "agent-late-a",
		protocol.SessionStateConnected, self, nil)
	require.Equal(t, codeNotReady, errorCode(t, registrationExchange(t, root, lateWhoAmI)),
		"the session's own process must still register")
	child.send(t, lateWhoAmI)
	require.Equal(t, "agent-late-a", child.recv(t)["result"].(map[string]any)["agent_id"])

	_, err = fixture.store.DB().ExecContext(t.Context(),
		"UPDATE sessions SET state = ? WHERE session_id = ?",
		protocol.SessionStateDetached, "session-late-a")
	require.NoError(t, err)
	insertLifecycleSession(t, fixture, "session-late-b", "agent-late-b",
		protocol.SessionStateConnected, self, nil)
	child.send(t, lateWhoAmI)
	require.NotContains(t, child.readAll(t), "{", "a bound connection never answers as another agent")
	select {
	case childErr := <-child.done:
		require.NoError(t, childErr)
	case <-time.After(15 * time.Second):
		_ = child.cmd.Process.Kill()
		t.Fatal("the dropped connection's child did not exit")
	}
}
