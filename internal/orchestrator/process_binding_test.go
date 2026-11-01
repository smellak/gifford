package orchestrator

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
	return callProcessGuardPhase(t, executor, record, true)
}

func callProcessGuardPhase(
	t *testing.T,
	executor *Executor,
	record deliveryRecord,
	pinLayout bool,
) (bool, string, error) {
	t.Helper()
	tx, err := executor.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	pass, detail, guardErr := executor.processGuard(t.Context(), tx, record, pinLayout)
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

func setProcessObservation(fixture *executorFixture, tree ProcessTree) {
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

func storedProcessBindingVersion(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var version int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT layout_version
		FROM orchestrator_process_bindings WHERE session_id = ?`, executorTestSession).Scan(&version))
	return version
}

func setMeasuredCodexManifest(t *testing.T, fixture *executorFixture) {
	t.Helper()
	var raw string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT manifest_json FROM sessions WHERE session_id = ?`, executorTestSession).Scan(&raw))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(raw), &manifest))
	manifest.Harness = protocol.HarnessCodex
	manifest.HarnessVersion = "0.154.0"
	updated, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(),
		`UPDATE sessions SET manifest_json = ? WHERE session_id = ?`, string(updated), executorTestSession)
	require.NoError(t, err)
}

func processHarnessNode(tree ProcessTree) ProcessNode { return tree.Descendants[0] }

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
				fixture.processes.tree.Descendants = nil
			},
			wantReason: "CHILD_CHANGED",
		},
		{
			name: "child pid reused",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				reused := ProcessToken{PID: 102, StartTime: 999}
				fixture.processes.tree.Descendants = []ProcessNode{
					testProcessNode(reused, fixture.processes.tree.Root, executorTestHarnessExecutable),
				}
			},
			wantReason: "CHILD_CHANGED",
		},
		{
			name: "child executable changes",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				fixture.processes.tree.Descendants[0].Executable = "/opt/gifford-test/other-codex"
			},
			wantReason: "CHILD_EXECUTABLE_CHANGED",
		},
		{
			name: "child executable becomes unavailable",
			mutate: func(fixture *executorFixture, _ *deliveryRecord) {
				fixture.processes.tree.Descendants[0].Executable = ""
			},
			wantReason: "CHILD_EXECUTABLE_CHANGED",
		},
		{
			name: "parent identity changes",
			mutate: func(fixture *executorFixture, record *deliveryRecord) {
				record.process.ProcessStartTime = 999
				fixture.processes.tree.Root.StartTime = 999
				fixture.processes.tree.Descendants[0].Parent.StartTime = 999
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

			parent := ProcessToken{PID: 101, StartTime: 202}
			child := ProcessToken{PID: 102, StartTime: 203}
			setProcessObservation(fixture, ProcessTree{
				BootID: executorTestBootID, Root: parent,
				Descendants: []ProcessNode{
					testProcessNode(child, parent, executorTestHarnessExecutable),
				},
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
	parent := base.Root

	for _, nodes := range [][]ProcessNode{
		nil,
		{
			testProcessNode(ProcessToken{PID: 102, StartTime: 203}, parent, executorTestHarnessExecutable),
			testProcessNode(ProcessToken{PID: 103, StartTime: 204}, parent, executorTestToolExecutable),
		},
	} {
		observation := base
		observation.Descendants = nodes
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
	harness := processHarnessNode(fixture.processes.tree)
	fixture.processes.tree.Descendants = append(
		fixture.processes.tree.Descendants,
		testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestToolExecutable),
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

	parent := ProcessToken{PID: 101, StartTime: 202}
	harness := ProcessToken{PID: 102, StartTime: 203}
	endpoint := ProcessToken{PID: 103, StartTime: 204}
	endpointChild := ProcessToken{PID: 104, StartTime: 205}
	other := ProcessToken{PID: 105, StartTime: 206}
	harnessNode := testProcessNode(harness, parent, executorTestHarnessExecutable)
	endpointExecutable := "/opt/gifford-test/gifford-mcp"
	tests := []struct {
		name       string
		nodes      []ProcessNode
		agent      protocol.AgentID
		session    protocol.OpaqueID
		registered ProcessToken
		live       bool
		wantPass   bool
	}{
		{
			name:  "endpoint below harness",
			nodes: []ProcessNode{harnessNode, testProcessNode(endpoint, harness, endpointExecutable)},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
			wantPass: true,
		},
		{
			name:  "direct endpoint beside harness",
			nodes: []ProcessNode{harnessNode, testProcessNode(endpoint, parent, endpointExecutable)},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
			wantPass: true,
		},
		{
			name:  "disconnected endpoint",
			nodes: []ProcessNode{harnessNode, testProcessNode(endpoint, harness, endpointExecutable)},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint,
		},
		{
			name:  "different agent",
			nodes: []ProcessNode{harnessNode, testProcessNode(endpoint, harness, endpointExecutable)},
			agent: "agent-other", session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name:  "different session",
			nodes: []ProcessNode{harnessNode, testProcessNode(endpoint, harness, endpointExecutable)},
			agent: executorTestAgent, session: "session-other", registered: endpoint, live: true,
		},
		{
			name: "recycled endpoint pid",
			nodes: []ProcessNode{
				harnessNode,
				testProcessNode(ProcessToken{PID: endpoint.PID, StartTime: 999}, harness, endpointExecutable),
			},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name: "endpoint child does not inherit",
			nodes: []ProcessNode{
				harnessNode,
				testProcessNode(endpoint, harness, endpointExecutable),
				testProcessNode(endpointChild, endpoint, executorTestToolExecutable),
			},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
		{
			name: "another tool remains untrusted",
			nodes: []ProcessNode{
				harnessNode,
				testProcessNode(endpoint, harness, endpointExecutable),
				testProcessNode(other, harness, executorTestToolExecutable),
			},
			agent: executorTestAgent, session: executorTestSession, registered: endpoint, live: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newExecutorFixture(t)
			observation := fixture.processes.tree
			observation.Descendants = test.nodes
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
	harness := processHarnessNode(fixture.processes.tree)
	fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants,
		testProcessNode(endpoint, harness.Token, "/opt/gifford-test/gifford-mcp"))
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

func TestMeasuredCodexAuxiliaryLayout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		addNodes func(ProcessNode) []ProcessNode
		wantPass bool
	}{
		{
			name: "harness and one measured auxiliary",
			addNodes: func(harness ProcessNode) []ProcessNode {
				return []ProcessNode{
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable),
				}
			},
			wantPass: true,
		},
		{
			name: "tool below harness",
			addNodes: func(harness ProcessNode) []ProcessNode {
				return []ProcessNode{
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestToolExecutable),
				}
			},
		},
		{
			name: "auxiliary plus tool below auxiliary",
			addNodes: func(harness ProcessNode) []ProcessNode {
				auxiliary := ProcessToken{PID: 103, StartTime: 204}
				return []ProcessNode{
					testProcessNode(auxiliary, harness.Token, executorTestAuxiliaryExecutable),
					testProcessNode(ProcessToken{PID: 104, StartTime: 205}, auxiliary, executorTestToolExecutable),
				}
			},
		},
		{
			name: "second auxiliary",
			addNodes: func(harness ProcessNode) []ProcessNode {
				return []ProcessNode{
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable),
					testProcessNode(ProcessToken{PID: 104, StartTime: 205}, harness.Token, executorTestAuxiliaryExecutable),
				}
			},
		},
		{
			name: "right name in another installation",
			addNodes: func(harness ProcessNode) []ProcessNode {
				return []ProcessNode{
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, "/tmp/codex-code-mode-host"),
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newExecutorFixture(t)
			setMeasuredCodexManifest(t, fixture)
			harness := processHarnessNode(fixture.processes.tree)
			fixture.processes.tree.Descendants = append(
				fixture.processes.tree.Descendants, test.addNodes(harness)...,
			)
			pass, detail, err := callProcessGuard(t, fixture.executor, processBindingRecord())
			require.NoError(t, err)
			require.Equal(t, test.wantPass, pass)
			if !test.wantPass {
				require.Contains(t, detail, "untrusted harness descendant")
			}
		})
	}
}

func TestAuxiliaryRequiresExactMeasuredProfileVersion(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	harness := processHarnessNode(fixture.processes.tree)
	fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants,
		testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable))
	pass, detail, err := callProcessGuard(t, fixture.executor, processBindingRecord())
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, "untrusted harness descendant")
}

func TestMeasuredAuxiliaryIdentityCannotChangeAfterPreparation(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*ProcessNode)
	}{
		{
			name: "pid reused",
			mutate: func(auxiliary *ProcessNode) {
				auxiliary.Token.StartTime++
			},
		},
		{
			name: "replacement pid",
			mutate: func(auxiliary *ProcessNode) {
				auxiliary.Token.PID++
			},
		},
		{
			name:   "auxiliary appears",
			mutate: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newExecutorFixture(t)
			setMeasuredCodexManifest(t, fixture)
			harness := processHarnessNode(fixture.processes.tree)
			if test.mutate != nil {
				fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants,
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable))
			}
			record := processBindingRecord()
			pass, _, err := callProcessGuardPhase(t, fixture.executor, record, true)
			require.NoError(t, err)
			require.True(t, pass)
			record.processBindingVersion = storedProcessBindingVersion(t, fixture.store.DB())

			if test.mutate == nil {
				fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants,
					testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable))
			} else {
				test.mutate(&fixture.processes.tree.Descendants[1])
			}
			pass, detail, err := callProcessGuardPhase(t, fixture.executor, record, false)
			require.NoError(t, err)
			require.False(t, pass)
			require.Contains(t, detail, "changed after preparation")

			pass, _, err = callProcessGuardPhase(t, fixture.executor, record, true)
			require.NoError(t, err)
			require.True(t, pass, "a later delivery may pin a stable measured auxiliary")
		})
	}
}

func TestPreparedLayoutVersionRejectsConcurrentRepin(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	setMeasuredCodexManifest(t, fixture)
	harness := processHarnessNode(fixture.processes.tree)
	fixture.processes.tree.Descendants = append(fixture.processes.tree.Descendants,
		testProcessNode(ProcessToken{PID: 103, StartTime: 204}, harness.Token, executorTestAuxiliaryExecutable))
	record := processBindingRecord()
	pass, _, err := callProcessGuardPhase(t, fixture.executor, record, true)
	require.NoError(t, err)
	require.True(t, pass)
	record.processBindingVersion = storedProcessBindingVersion(t, fixture.store.DB())

	fixture.processes.tree.Descendants[1].Token.PID++
	pass, _, err = callProcessGuardPhase(t, fixture.executor, processBindingRecord(), true)
	require.NoError(t, err)
	require.True(t, pass, "another delivery may pin its own stable observation")

	pass, detail, err := callProcessGuardPhase(t, fixture.executor, record, false)
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, "binding changed after preparation")
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
