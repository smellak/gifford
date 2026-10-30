package orchestrator

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
)

func processBindingRecord() deliveryRecord {
	return deliveryRecord{
		QueueDelivery: daemon.QueueDelivery{AgentID: executorTestAgent},
		sessionID:     executorTestSession,
		process: protocol.ProcessIdentity{
			PID: 101, ProcessStartTime: 202, UID: 303, GID: 404,
		},
	}
}

func callProcessGuard(
	t *testing.T,
	executor *Executor,
	record deliveryRecord,
) (bool, string, error) {
	t.Helper()
	tx, err := executor.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	pass, detail, guardErr := executor.processGuard(t.Context(), tx, record)
	if guardErr != nil {
		require.NoError(t, tx.Rollback())
		return false, "", guardErr
	}
	require.NoError(t, tx.Commit())
	return pass, detail, nil
}

func replacementExecutor(t *testing.T, fixture *executorFixture) *Executor {
	t.Helper()
	executor, err := NewExecutor(ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.executor.channel,
		Approvals: fixture.controller, Links: fixture.links,
		Clock: fixture.clock, Entropy: bytes.NewReader(bytes.Repeat([]byte{1}, 4096)),
		Processes: fixture.processes, Endpoints: fixture.endpoint,
	})
	require.NoError(t, err)
	return executor
}

func setRegisteredEndpoint(
	fixture *executorFixture,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	process ProcessToken,
	live bool,
) {
	fixture.endpoint.mu.Lock()
	fixture.endpoint.agent = agent
	fixture.endpoint.session = session
	fixture.endpoint.pid = process.PID
	fixture.endpoint.start = process.StartTime
	fixture.endpoint.live = live
	fixture.endpoint.mu.Unlock()
}

func setProcessObservation(
	fixture *executorFixture,
	tree ProcessTree,
) {
	fixture.processes.mu.Lock()
	fixture.processes.tree = tree
	fixture.processes.err = nil
	fixture.processes.mu.Unlock()
}

func storedProcessBinding(t *testing.T, db *sql.DB) (string, string) {
	t.Helper()
	var state string
	var reason sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, block_reason
		FROM orchestrator_process_bindings WHERE session_id = ?`, executorTestSession).
		Scan(&state, &reason))
	return state, reason.String
}

func TestProcessBindingSurvivesExecutorRestartAndBlocksBootChange(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	record := processBindingRecord()
	pass, _, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.True(t, pass)

	pass, _, err = callProcessGuard(t, replacementExecutor(t, fixture), record)
	require.NoError(t, err)
	require.True(t, pass, "a node restart on the same boot must revalidate the durable child")

	fixture.processes.mu.Lock()
	fixture.processes.tree.BootID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	fixture.processes.mu.Unlock()
	pass, _, err = callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.False(t, pass)
	state, reason := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBlocked, state)
	require.Equal(t, "BOOT_CHANGED", reason)

	fixture.processes.mu.Lock()
	fixture.processes.tree.BootID = executorTestBootID
	fixture.processes.mu.Unlock()
	pass, detail, err := callProcessGuard(t, replacementExecutor(t, fixture), record)
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, "permanently blocked")
}

func TestBoundProcessIdentityChangesPermanentlyBlockSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*executorFixture, *deliveryRecord)
		wantReason string
	}{
		{
			name: "child dies",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				fixture.processes.tree.DirectChildren = nil
				fixture.processes.tree.Descendants = nil
			},
			wantReason: "CHILD_CHANGED",
		},
		{
			name: "child pid reused",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				reused := ProcessToken{PID: 102, StartTime: 999}
				fixture.processes.tree.DirectChildren = []ProcessToken{reused}
				fixture.processes.tree.Descendants = []ProcessToken{reused}
			},
			wantReason: "CHILD_CHANGED",
		},
		{
			name: "parent identity changes",
			mutate: func(fixture *executorFixture, record *deliveryRecord) {
				record.process.ProcessStartTime = 999
				fixture.processes.tree.Root.StartTime = 999
			},
			wantReason: "PARENT_CHANGED",
		},
		{
			name: "process observation becomes unreadable",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				fixture.processes.err = errors.New("proc unavailable")
			},
			wantReason: "OBSERVATION_FAILED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newExecutorFixture(t)
			record := processBindingRecord()
			pass, _, err := callProcessGuard(t, fixture.executor, record)
			require.NoError(t, err)
			require.True(t, pass)

			fixture.processes.mu.Lock()
			test.mutate(fixture, &record)
			fixture.processes.mu.Unlock()
			pass, _, err = callProcessGuard(t, fixture.executor, record)
			require.NoError(t, err)
			require.False(t, pass)
			state, reason := storedProcessBinding(t, fixture.store.DB())
			require.Equal(t, processBindingStateBlocked, state)
			require.Equal(t, test.wantReason, reason)

			setProcessObservation(fixture, ProcessTree{
				BootID:         executorTestBootID,
				Root:           ProcessToken{PID: 101, StartTime: 202},
				DirectChildren: []ProcessToken{{PID: 102, StartTime: 203}},
				Descendants:    []ProcessToken{{PID: 102, StartTime: 203}},
			})
			pass, detail, err := callProcessGuard(t, replacementExecutor(t, fixture), processBindingRecord())
			require.NoError(t, err)
			require.False(t, pass, "a reconnect or restart must never learn a replacement child")
			require.Contains(t, detail, "permanently blocked")
		})
	}
}

func TestProcessBindingRequiresOneDirectChildBeforeBinding(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	record := processBindingRecord()
	base := fixture.processes.tree

	for _, children := range [][]ProcessToken{
		nil,
		{{PID: 102, StartTime: 203}, {PID: 103, StartTime: 204}},
	} {
		observation := base
		observation.DirectChildren = children
		observation.Descendants = append([]ProcessToken(nil), children...)
		setProcessObservation(fixture, observation)
		pass, _, err := callProcessGuard(t, fixture.executor, record)
		require.NoError(t, err)
		require.False(t, pass)
		var count int64
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
			FROM orchestrator_process_bindings WHERE session_id = ?`, executorTestSession).Scan(&count))
		require.Zero(t, count, "an ambiguous first observation must not choose a child")
	}

	setProcessObservation(fixture, base)
	pass, _, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.True(t, pass)
	state, reason := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBound, state)
	require.Empty(t, reason)
}

func TestAdditionalDescendantDefersWithoutRebinding(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	record := processBindingRecord()
	pass, _, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.True(t, pass)

	fixture.processes.mu.Lock()
	fixture.processes.tree.Descendants = append(
		fixture.processes.tree.Descendants,
		ProcessToken{PID: 103, StartTime: 204},
	)
	fixture.processes.mu.Unlock()
	pass, detail, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, "untrusted harness descendant")
	state, _ := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBound, state)

	fixture.processes.mu.Lock()
	fixture.processes.tree.Descendants = fixture.processes.tree.Descendants[:1]
	fixture.processes.mu.Unlock()
	pass, _, err = callProcessGuard(t, replacementExecutor(t, fixture), record)
	require.NoError(t, err)
	require.True(t, pass)
}

func TestRegisteredEndpointIsTheOnlyAdditionalProcessAllowed(t *testing.T) {
	t.Parallel()

	endpoint := ProcessToken{PID: 103, StartTime: 204}
	endpointChild := ProcessToken{PID: 104, StartTime: 205}
	other := ProcessToken{PID: 105, StartTime: 206}
	tests := []struct {
		name        string
		direct      []ProcessToken
		descendants []ProcessToken
		agent       protocol.AgentID
		session     protocol.OpaqueID
		registered  ProcessToken
		live        bool
		wantPass    bool
	}{
		{
			name: "endpoint below harness", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
			wantPass: true,
		},
		{
			name:        "direct endpoint beside harness",
			direct:      []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
			wantPass: true,
		},
		{
			name: "disconnected endpoint", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint,
		},
		{
			name: "different agent", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			agent:       "agent-other", session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name: "different session", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint},
			agent:       executorTestAgent, session: "session-other", registered: endpoint, live: true,
		},
		{
			name: "recycled endpoint pid", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, {PID: endpoint.PID, StartTime: 999}},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name: "endpoint child does not inherit", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint, endpointChild},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name: "another tool remains untrusted", direct: []ProcessToken{{PID: 102, StartTime: 203}},
			descendants: []ProcessToken{{PID: 102, StartTime: 203}, endpoint, other},
			agent:       executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newExecutorFixture(t)
			observation := fixture.processes.tree
			observation.DirectChildren = test.direct
			observation.Descendants = test.descendants
			setProcessObservation(fixture, observation)
			setRegisteredEndpoint(fixture, test.agent, test.session, test.registered, test.live)
			pass, detail, err := callProcessGuard(t, fixture.executor, processBindingRecord())
			require.NoError(t, err)
			require.Equal(t, test.wantPass, pass)
			if !test.wantPass {
				require.NotEmpty(t, detail)
			}
		})
	}
}

func TestEndpointDisconnectStopsTheExceptionWithoutRebinding(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	record := processBindingRecord()
	endpoint := ProcessToken{PID: 103, StartTime: 204}
	fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants, endpoint)
	setRegisteredEndpoint(fixture, executorTestAgent, executorTestSession, endpoint, true)
	pass, _, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.True(t, pass)

	setRegisteredEndpoint(fixture, executorTestAgent, executorTestSession, endpoint, false)
	pass, detail, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, "untrusted harness descendant")
	state, _ := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBound, state)
}

func TestInitialUnreadableObservationPermanentlyBlocksSession(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	fixture.processes.mu.Lock()
	fixture.processes.err = errors.New("proc unavailable")
	fixture.processes.mu.Unlock()
	pass, _, err := callProcessGuard(t, fixture.executor, processBindingRecord())
	require.NoError(t, err)
	require.False(t, pass)
	state, reason := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBlocked, state)
	require.Equal(t, "OBSERVATION_FAILED", reason)

	fixture.processes.mu.Lock()
	fixture.processes.err = nil
	fixture.processes.mu.Unlock()
	pass, _, err = callProcessGuard(t, replacementExecutor(t, fixture), processBindingRecord())
	require.NoError(t, err)
	require.False(t, pass)
}
