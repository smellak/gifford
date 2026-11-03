package controlapi

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const incidentTestInstance = "01JB0000000000000000000088"

func incidentFixture(t *testing.T) *readFixture {
	t.Helper()
	f := newReadFixture(t, true)
	seedReadRows(t, f)
	f.server.operator = "node-operator"
	stamp := eventstore.FormatTime(readTestTime)
	// These are isolated source-table fixtures, not production writers.
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO approval_requests
		(approval_id, source, object_type, object_id, agent_id, options_json, state, created_at, request_note)
		VALUES ('apr_01JB0000000000000000000077', 'orchestrator', 'message', ?, ?,
		'["deliver_now","reject"]', 'PENDING', ?, 'canary-operator-note')`, readTestMessage, readTestAgent, stamp)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO tmux_instances
		(instance_id, agent_id, machine_id, boot_id, owner_user, owner_uid, socket, pane_id,
		pane_pid, pane_start, pane_comm, harness, harness_version, harness_pid, harness_start,
		harness_comm, agent_uid, agent_gid, source, registered_by, session_id, state, invalid_reason,
		created_at, updated_at, harness_argv0, harness_argv1)
		VALUES (?, ?, 'canary-machine', 'canary-boot', 'canary-user', 1000, '/canary/socket', '%1',
		101, 1, 'shell', 'codex', '1.2.3', 102, 2, 'codex', 1001, 1001, 'test', 1000, ?, 'ACTIVE', NULL,
		?, ?, 'canary-argv0', 'canary-argv1'),
		('01JB0000000000000000000089', 'agent-invalid', 'canary-machine', 'canary-boot', 'canary-user',
		1000, '/canary/socket', '%2', 103, 3, 'shell', 'codex', '1.2.3', 104, 4, 'codex', 1002, 1002,
		'test', 1000, NULL, 'INVALID', 'harness_changed', ?, ?, '', '')`,
		incidentTestInstance, readTestAgent, readTestSession, stamp, stamp, stamp, stamp)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET capture_mode = 'tmux_attach' WHERE session_id = ?`, readTestSession)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE input_guards SET draft_state = 'UNKNOWN',
		activity_state = 'IDLE', detection_source = 'tmux_attach' WHERE session_id = ?`, readTestSession)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO escalations
		(message_id, agent_id, operator_agent, created_at) VALUES (?, ?, 'original-operator', ?)`,
		readTestMessage, readTestAgent, stamp)
	require.NoError(t, err)
	return f
}

func TestIncidentProjectsEveryKindWithoutInventedFacts(t *testing.T) {
	f := incidentFixture(t)
	page := readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList, protocol.IncidentListParams{})
	require.Len(t, page.Incidents, 5)
	require.NoError(t, page.Validate())
	byKind := make(map[protocol.IncidentKind]protocol.IncidentListItem)
	for _, item := range page.Incidents {
		byKind[item.Kind] = item
		require.Equal(t, protocol.IncidentOwnerOperator, item.OwnerKind)
		require.NotNil(t, item.OwnerID)
		require.Nil(t, item.NextNoticeAt)
		require.Nil(t, item.ResolvedAt)
		if item.Kind != protocol.IncidentKindEscalated {
			require.Equal(t, protocol.AgentID("node-operator"), *item.OwnerID)
		}
	}
	require.Equal(t, protocol.IncidentStateUncertain, byKind[protocol.IncidentKindDeliveryUncertain].State)
	require.Equal(t, "DISCONNECTED", byKind[protocol.IncidentKindDeliveryUncertain].Reason)
	require.Equal(t, protocol.IncidentFixAreaDelivery, byKind[protocol.IncidentKindDeliveryUncertain].FixArea)
	require.Equal(t, protocol.IncidentStatePending, byKind[protocol.IncidentKindApprovalPending].State)
	require.Equal(t, "orchestrator", byKind[protocol.IncidentKindApprovalPending].Reason)
	require.Equal(t, protocol.IncidentFixAreaDelivery, byKind[protocol.IncidentKindApprovalPending].FixArea)
	require.Equal(t, protocol.IncidentStateInvalid, byKind[protocol.IncidentKindTmuxInstanceInvalid].State)
	require.Equal(t, "harness_changed", byKind[protocol.IncidentKindTmuxInstanceInvalid].Reason)
	require.Equal(t, protocol.IncidentFixAreaInstances, byKind[protocol.IncidentKindTmuxInstanceInvalid].FixArea)
	require.Equal(t, "DRAFT_UNKNOWN", byKind[protocol.IncidentKindDetectorUncertain].Reason)
	require.Equal(t, protocol.IncidentFixAreaDetector, byKind[protocol.IncidentKindDetectorUncertain].FixArea)
	require.Equal(t, protocol.IncidentStateEscalated, byKind[protocol.IncidentKindEscalated].State)
	require.Equal(t, protocol.AgentID("original-operator"), *byKind[protocol.IncidentKindEscalated].OwnerID)
	require.Equal(t, protocol.IncidentFixAreaUnknown, byKind[protocol.IncidentKindEscalated].FixArea,
		"a matching current delivery does not prove why escalation happened")
	require.Equal(t, readTestTime, *byKind[protocol.IncidentKindEscalated].EscalatedAt)
	require.Equal(t, readTestTime, byKind[protocol.IncidentKindDetectorUncertain].OpenedAt)
	encoded, err := json.Marshal(page)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "canary")

	f.server.operator = ""
	page = readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList, protocol.IncidentListParams{})
	for _, item := range page.Incidents {
		if item.Kind != protocol.IncidentKindEscalated {
			require.Nil(t, item.OwnerID, "no fabricated owner when unconfigured")
		}
	}
}

func TestIncidentPaginationFiltersAndCursorBinding(t *testing.T) {
	f := incidentFixture(t)
	all := readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList, protocol.IncidentListParams{})
	limit := int64(1)
	params := protocol.IncidentListParams{Limit: &limit}
	var got []protocol.IncidentListItem
	for range 6 {
		page := readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList, params)
		got = append(got, page.Incidents...)
		if page.NextCursor == "" {
			break
		}
		require.True(t, page.NextCursor.Valid())
		params.Cursor = page.NextCursor
	}
	require.Equal(t, all.Incidents, got, "equal timestamps must neither duplicate nor skip kinds")
	page := readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList,
		protocol.IncidentListParams{AgentID: readTestAgent, Kind: protocol.IncidentKindDetectorUncertain})
	require.Len(t, page.Incidents, 1)
	for _, params := range []protocol.IncidentListParams{
		{Cursor: params.Cursor, AgentID: "different-agent"},
		{Cursor: params.Cursor, Kind: protocol.IncidentKindEscalated},
		{Cursor: "bad"},
		{Kind: "NEW"},
	} {
		response := f.server.Handle(t.Context(), readPeer(), rpcRequest(t, protocol.ControlMethodIncidentList, params))
		require.NotNil(t, response.Error)
		require.Equal(t, int64(rpcInvalidParams), response.Error.Code)
	}
	filter := cursorFilter(protocol.ControlMethodIncidentList, "", "", "node-operator")
	longID := strings.Repeat("a", 128)
	cursor, err := encodeIncidentCursor(filter, readTestTime.Add(123456789*time.Nanosecond), protocol.IncidentKindTmuxInstanceInvalid, longID)
	require.NoError(t, err)
	require.True(t, cursor.Valid())
	position, err := decodeIncidentCursor(cursor, filter)
	require.NoError(t, err)
	require.Equal(t, longID, position[2])
}

func TestIncidentResolutionAndDetectorCertaintyWithdrawCurrentRows(t *testing.T) {
	f := incidentFixture(t)
	_, err := f.store.DB().ExecContext(t.Context(), `UPDATE approval_requests SET state = 'APPROVED', resolved_at = ?`, eventstore.FormatTime(readTestTime))
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE delivery_attempts SET resolution_approval_id = 'apr_01JB0000000000000000000077'`)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE input_guards SET draft_state = 'OPEN', activity_state = 'ACTIVE'`)
	require.NoError(t, err)
	page := readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList, protocol.IncidentListParams{})
	require.Len(t, page.Incidents, 2, "busy human input is not detector uncertainty")
	for _, item := range page.Incidents {
		require.Contains(t, []protocol.IncidentKind{protocol.IncidentKindEscalated, protocol.IncidentKindTmuxInstanceInvalid}, item.Kind)
	}
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE input_guards SET activity_state = 'UNKNOWN'`)
	require.NoError(t, err)
	page = readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList,
		protocol.IncidentListParams{Kind: protocol.IncidentKindDetectorUncertain})
	require.Len(t, page.Incidents, 1)
	require.Equal(t, "ACTIVITY_UNKNOWN", page.Incidents[0].Reason)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED', ended_at = ?`, eventstore.FormatTime(readTestTime))
	require.NoError(t, err)
	page = readResult[protocol.IncidentListResult](t, f.server, protocol.ControlMethodIncidentList,
		protocol.IncidentListParams{Kind: protocol.IncidentKindDetectorUncertain})
	require.Empty(t, page.Incidents, "an ended session cannot report a live detector state")
}

func TestIncidentReadsOnlyThroughProtectedQueryPool(t *testing.T) {
	f := incidentFixture(t)
	require.False(t, methodMutates(protocol.ControlMethodIncidentList))
	require.True(t, methodProtectsNodeReads(protocol.ControlMethodIncidentList))
	request := rpcRequest(t, protocol.ControlMethodIncidentList, protocol.IncidentListParams{})
	response := f.server.Handle(t.Context(), Peer{}, request)
	require.Equal(t, int64(rpcDomainError), response.Error.Code)
	f.server.readDB = f.store.DB()
	response = f.server.Handle(t.Context(), readPeer(), request)
	require.Equal(t, int64(rpcInternalError), response.Error.Code, "writable pool is refused")
	f.server.readDB = nil
	response = f.server.Handle(t.Context(), readPeer(), request)
	require.Equal(t, int64(rpcInternalError), response.Error.Code)
	f.server.readDB = f.readDB
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.server.readIncidents(ctx, protocol.IncidentListParams{})
	require.Error(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE tmux_instances SET invalid_reason = 'canary-free-text' WHERE state = 'INVALID'`)
	require.NoError(t, err)
	response = f.server.Handle(t.Context(), readPeer(), request)
	require.Equal(t, int64(rpcInternalError), response.Error.Code)
	require.NotContains(t, string(response.Result), "canary")
}

func TestIncidentQueryPlanUsesExistingIndexesWithoutCorrelatedScan(t *testing.T) {
	f := incidentFixture(t)
	rows, err := f.readDB.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+incidentListSQL,
		"", "", "", "", 0, "", "", "", 51)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	details := strings.Join(plan, "\n")
	t.Log(details)
	require.NotContains(t, details, "CORRELATED", "no per-row history scan")
	for _, index := range []string{"delivery_attempts_agent_uncertain", "approval_requests_pending", "tmux_instances_active_", "sqlite_autoindex_input_guards_1", "sqlite_autoindex_escalations_1"} {
		require.Contains(t, details, index)
	}
	require.NoError(t, rows.Close())
}

// This latency check uses the normal SQLite build, not the race-instrumented
// driver. The independent race task exercises concurrent reads instead.
func TestIncidentReadCapacity(t *testing.T) {
	f := incidentFixture(t)
	_, err := f.store.DB().ExecContext(t.Context(), `WITH RECURSIVE n(x) AS
		(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 10000)
		INSERT INTO escalations(message_id, agent_id, operator_agent, created_at)
		SELECT printf('01JC0000000000000000%06d', x), 'history-agent', 'history-operator', ? FROM n`,
		eventstore.FormatTime(readTestTime.Add(-time.Hour)))
	require.NoError(t, err)
	page, err := f.server.readIncidents(t.Context(), protocol.IncidentListParams{})
	require.NoError(t, err)
	require.Len(t, page.Incidents, int(protocol.DefaultListLimit))
	require.NotEmpty(t, page.NextCursor)
	var count int
	require.NoError(t, f.readDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM escalations").Scan(&count))
	require.Equal(t, 10001, count, "a read does not consume or mutate incident sources")
}

func TestIncidentConcurrentReads(t *testing.T) {
	f := incidentFixture(t)
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		workers.Go(func() {
			_, err := f.server.readIncidents(t.Context(), protocol.IncidentListParams{})
			failures <- err
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}
