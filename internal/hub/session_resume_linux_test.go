//go:build linux

package hub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func TestDetachedDescendantWithoutEndpointLeaseStaysRevoked(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-descendant-resume")
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	request := registerAgentLine(t, 1, registrationManifest(agent), self)
	owner := dialRegistrationConn(t, fixture.sock)
	session := registeredSessionID(t, registrationExchange(t, owner, request))
	child := dialChild(t, fixture.sock)
	child.send(t, `{"jsonrpc":"2.0","method":"whoami","id":2}`)
	require.NotContains(t, child.recv(t), "error")
	var initialRevision string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT updated_at FROM sessions WHERE session_id = ?", session).Scan(&initialRevision))
	require.NoError(t, owner.Close())
	var detachedRevision string
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, updated_at FROM sessions WHERE session_id = ?", session).
			Scan(&state, &detachedRevision)
		return err == nil && state == protocol.SessionStateDetached
	}, 5*time.Second, time.Millisecond)
	replacement := dialRegistrationConn(t, fixture.sock)
	defer func() { _ = replacement.Close() }()
	require.Equal(t, session,
		registeredSessionID(t, registrationExchange(t, replacement, request)))
	var resumedRevision string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT updated_at FROM sessions WHERE session_id = ?", session).Scan(&resumedRevision))
	require.NotEqual(t, initialRevision, detachedRevision)
	require.NotEqual(t, detachedRevision, resumedRevision)
	child.send(t, `{"jsonrpc":"2.0","method":"whoami","id":3}`)
	require.NotContains(t, child.readAll(t), "{",
		"a pre-detach child connection must stay revoked after resume")
	select {
	case childErr := <-child.done:
		require.NoError(t, childErr)
	case <-time.After(15 * time.Second):
		_ = child.cmd.Process.Kill()
		t.Fatal("old descendant connection did not exit")
	}
}
