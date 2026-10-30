package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the maintained verification of process lineage: the
// /proc walk, ancestor matching, and the CurrentProcess convention. These
// tests repoint procFSRoot at a scratch tree, so they never run in
// parallel with each other.

// withProcRoot points the walk at a scratch process tree for the test.
func withProcRoot(t *testing.T, root string) {
	t.Helper()
	previous := procFSRoot
	procFSRoot = root
	t.Cleanup(func() { procFSRoot = previous })
}

// writeStat writes a fake stat file: pid, a comm that may hold spaces and
// parentheses, ppid, and the start time, with filler fields matching the
// real layout (ppid second after comm, starttime twentieth).
func writeStat(t *testing.T, root string, pid int, comm string, ppid int, start int64) {
	t.Helper()
	fields := "S " + strconv.Itoa(ppid) + " 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 " + strconv.FormatInt(start, 10)
	content := fmt.Sprintf("%d (%s) %s", pid, comm, fields)
	require.NoError(t, os.MkdirAll(filepath.Join(root, strconv.Itoa(pid)), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, strconv.Itoa(pid), "stat"), []byte(content), 0o644))
}

// TestResolveAncestor proves the walk climbs: pid 100 descends from pid 50
// through ppid links, and the session anchored at 50 resolves.
func TestResolveAncestor(t *testing.T) {
	root := t.TempDir()
	withProcRoot(t, root)
	writeStat(t, root, 100, "worker", 50, 7)
	writeStat(t, root, 50, "launcher", 1, 5)
	writeStat(t, root, 1, "init", 0, 2)

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-tree")
	claim := protocol.ProcessIdentity{PID: 50, ProcessStartTime: 5}
	testSession(ctx, t, store.DB(), "session-tree", "agent-tree", &claim, nil)

	agent, err := ResolveAgentPID(ctx, store.DB(), 100)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-tree"), agent)
}

func TestResolveAgentSessionExcludesDetachedAndEndedSessions(t *testing.T) {
	root := t.TempDir()
	withProcRoot(t, root)
	writeStat(t, root, 100, "worker", 50, 7)
	writeStat(t, root, 50, "launcher", 0, 5)

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-session-state")
	claim := protocol.ProcessIdentity{PID: 50, ProcessStartTime: 5}
	testSession(ctx, t, store.DB(), "session-state", "agent-session-state", &claim, nil)

	agent, session, err := ResolveAgentSessionPID(ctx, store.DB(), 100)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-session-state"), agent)
	require.Equal(t, protocol.OpaqueID("session-state"), session)

	_, err = store.DB().ExecContext(ctx, `UPDATE sessions SET state = ?
		WHERE session_id = 'session-state'`, protocol.SessionStateDetached)
	require.NoError(t, err)
	_, _, err = ResolveAgentSessionPID(ctx, store.DB(), 100)
	require.ErrorIs(t, err, ErrNoRegisteredAncestor)
	_, err = ResolveAgentPID(ctx, store.DB(), 100)
	require.ErrorIs(t, err, ErrNoRegisteredAncestor)

	_, err = store.DB().ExecContext(ctx, `UPDATE sessions SET state = ?, ended_at = ?
		WHERE session_id = 'session-state'`, protocol.SessionStateConnected,
		"2026-10-30T00:00:00.000000000Z")
	require.NoError(t, err)
	_, _, err = ResolveAgentSessionPID(ctx, store.DB(), 100)
	require.ErrorIs(t, err, ErrNoRegisteredAncestor)
}

// TestResolveNearestAncestor proves the nearest matching ancestor wins
// when two sessions anchor the chain.
func TestResolveNearestAncestor(t *testing.T) {
	root := t.TempDir()
	withProcRoot(t, root)
	writeStat(t, root, 100, "worker", 50, 7)
	writeStat(t, root, 50, "middle", 10, 5)
	writeStat(t, root, 10, "top", 0, 3)

	store := testStore(t)
	ctx := t.Context()
	for _, session := range []struct{ id, agent string }{
		{"session-near", "agent-near"},
		{"session-far", "agent-far"},
	} {
		testAgent(ctx, t, store.DB(), session.agent)
	}
	near := protocol.ProcessIdentity{PID: 50, ProcessStartTime: 5}
	far := protocol.ProcessIdentity{PID: 10, ProcessStartTime: 3}
	testSession(ctx, t, store.DB(), "session-near", "agent-near", &near, nil)
	testSession(ctx, t, store.DB(), "session-far", "agent-far", &far, nil)

	agent, err := ResolveAgentPID(ctx, store.DB(), 100)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-near"), agent)
}

// TestResolveParsesComm proves the stat parser skips a comm holding
// spaces and parentheses by splitting after the last closing paren.
func TestResolveParsesComm(t *testing.T) {
	root := t.TempDir()
	withProcRoot(t, root)
	writeStat(t, root, 100, "my app (worker) v2", 0, 9)

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-comm")
	claim := protocol.ProcessIdentity{PID: 100, ProcessStartTime: 9}
	testSession(ctx, t, store.DB(), "session-comm", "agent-comm", &claim, nil)

	agent, err := ResolveAgentPID(ctx, store.DB(), 100)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-comm"), agent)
}

// TestResolveNoAncestor proves the failure modes: an unknown pid, a chain
// with no session, and a recycled pid whose start time moved on.
func TestResolveNoAncestor(t *testing.T) {
	root := t.TempDir()
	withProcRoot(t, root)
	writeStat(t, root, 100, "orphan", 0, 7)
	writeStat(t, root, 60, "recycled", 0, 8)

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-absent")
	moved := protocol.ProcessIdentity{PID: 60, ProcessStartTime: 6}
	testSession(ctx, t, store.DB(), "session-absent", "agent-absent", &moved, nil)

	for _, pid := range []int{999999, 100, 60} {
		_, err := ResolveAgentPID(ctx, store.DB(), pid)
		require.ErrorIs(t, err, ErrNoRegisteredAncestor, "pid %d", pid)
	}
	_, err := ResolveAgentPID(ctx, store.DB(), 0)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestCurrentProcessRoundTrip proves the convention both sides share: the
// live process described by CurrentProcess resolves through the real
// /proc when its session is registered.
func TestCurrentProcessRoundTrip(t *testing.T) {
	self, err := CurrentProcess()
	require.NoError(t, err)
	require.NoError(t, self.Validate())

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-self")
	claim := self
	testSession(ctx, t, store.DB(), "session-self", "agent-self", &claim, nil)

	agent, err := ResolveAgentPID(ctx, store.DB(), int(self.PID))
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-self"), agent)
}
