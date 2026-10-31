package controlapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	notificationstore "github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	agentListSQL = `
SELECT a.agent_id, a.node_id, a.harness,
       s.session_id, s.state, s.capture_mode, s.deliverable,
       json_extract(s.manifest_json, '$.harness_version'),
       ig.draft_state, ig.activity_state, ig.version, ig.updated_at,
       ac.tx_enabled, ac.rx_enabled, ac.auto_delivery_enabled,
       ac.auto_continue_enabled
FROM agents AS a
LEFT JOIN sessions AS s ON s.session_id = (
    SELECT recent.session_id FROM sessions AS recent
    WHERE recent.agent_id = a.agent_id
    ORDER BY (recent.ended_at IS NULL) DESC, recent.started_at DESC,
             recent.session_id DESC
    LIMIT 1
)
LEFT JOIN input_guards AS ig ON ig.session_id = s.session_id
LEFT JOIN channel_controls AS ac
  ON ac.scope_type = 'agent' AND ac.scope_key = a.agent_id AND ac.plane = 'all'
WHERE (? = 0 OR a.agent_id > ?)
ORDER BY a.agent_id ASC
LIMIT ?`

	agentQueueCountsSQL = `
SELECT page.value,
       (SELECT COUNT(*) FROM delivery_queue AS q
          WHERE q.agent_id = page.value AND q.state IN ('QUEUED', 'RETRY_WAIT')),
       (SELECT COUNT(*) FROM delivery_queue AS d
          WHERE d.agent_id = page.value AND d.state IN ('CLAIMED', 'DELIVERING')),
       (SELECT COUNT(*) FROM delivery_attempts AS u
          WHERE u.agent_id = page.value AND u.state = 'UNCERTAIN'
            AND u.resolution_approval_id IS NULL)
FROM json_each(?) AS page
ORDER BY CAST(page.key AS INTEGER) ASC
LIMIT ?`

	messageListSQL = `
SELECT q.message_id, q.agent_id, q.delivery_generation, q.from_agent,
       q.thread_ref, q.state, q.priority, q.created_at, q.expires_at,
       q.attempts,
       (SELECT recent.terminal_reason FROM delivery_attempts AS recent
          WHERE recent.message_id = q.message_id
            AND recent.agent_id = q.agent_id
            AND recent.delivery_generation = q.delivery_generation
          ORDER BY recent.created_at DESC, recent.delivery_id DESC
          LIMIT 1),
       CASE
         WHEN EXISTS (SELECT 1 FROM acks AS acknowledged
                       WHERE acknowledged.message_id = q.message_id
                         AND acknowledged.agent_id = q.agent_id
                         AND acknowledged.delivery_generation = q.delivery_generation
                         AND acknowledged.status = 'ACKNOWLEDGED')
           THEN 'ACKNOWLEDGED'
         WHEN EXISTS (SELECT 1 FROM acks AS processing
                       WHERE processing.message_id = q.message_id
                         AND processing.agent_id = q.agent_id
                         AND processing.delivery_generation = q.delivery_generation
                         AND processing.status = 'PROCESSING')
           THEN 'PROCESSING'
         ELSE ''
       END
FROM delivery_queue AS q
WHERE (? = '' OR q.agent_id = ?)
  AND (? = '' OR q.state = ?)
  AND (? = 0 OR (q.created_at, q.message_id, q.agent_id,
                  q.delivery_generation) < (?, ?, ?, ?))
ORDER BY q.created_at DESC, q.message_id DESC, q.agent_id DESC,
         q.delivery_generation DESC
LIMIT ?`

	decisionListSQL = `
WITH combined AS (
    SELECT decision_id, 'channel' AS family, stage, outcome,
           '' AS selected_action, to_agent AS agent_id,
           object_id AS message_id, created_at, reason AS channel_reason
      FROM channel_control_decisions
     WHERE object_type = 'message' AND to_agent IS NOT NULL
    UNION ALL
    SELECT decision_id, 'orchestrator' AS family, '' AS stage,
           '' AS outcome, selected_action, agent_id, message_id,
           created_at, '' AS channel_reason
      FROM orchestrator_decisions
     WHERE message_id IS NOT NULL
)
SELECT decision_id, family, stage, outcome, selected_action, agent_id,
       message_id, created_at, channel_reason
FROM combined
WHERE (? = '' OR agent_id = ?)
  AND (? = '' OR message_id = ?)
  AND (? = '' OR family = ?)
  AND (? = 0 OR (created_at, decision_id) < (?, ?))
ORDER BY created_at DESC, decision_id DESC
LIMIT ?`

	channelDecisionSQL = `
SELECT decision_id, stage, outcome, to_agent, object_id, created_at,
       reason,
       COALESCE(NULLIF(json_extract(explanation_json, '$.blocker'), ''),
                json_extract(explanation_json, '$.lines[#-1].scope'))
FROM channel_control_decisions
WHERE decision_id = ? AND object_type = 'message' AND to_agent IS NOT NULL
LIMIT 1`

	// Thread scope keys embed the readable thread id. The evidence keeps the
	// remaining stored control epochs while honoring the metadata-only boundary.
	channelVersionsSQL = `
SELECT key, value
FROM json_each((SELECT control_versions_json
                  FROM channel_control_decisions
                 WHERE decision_id = ?))
WHERE key NOT LIKE 'thread:%'
ORDER BY key ASC
LIMIT 65`

	orchestratorDecisionSQL = `
SELECT decision_id, selected_action, agent_id, message_id, created_at,
       policy_version, decision_model, glimmer_call_id, execution_status,
       channel_decision_id
FROM orchestrator_decisions
WHERE decision_id = ? AND message_id IS NOT NULL
LIMIT 1`

	orchestratorActionsSQL = `
SELECT value
FROM json_each((SELECT allowed_actions_json
                  FROM orchestrator_decisions
                 WHERE decision_id = ?))
ORDER BY CAST(key AS INTEGER) ASC
LIMIT 13`

	orchestratorGuardsSQL = `
SELECT json_extract(value, '$.guard'), json_extract(value, '$.outcome')
FROM json_each((SELECT hard_guards_json
                  FROM orchestrator_decisions
                 WHERE decision_id = ?))
ORDER BY CAST(key AS INTEGER) ASC
LIMIT 33`

	orchestratorFailedGuardsSQL = `
SELECT json_extract(value, '$.guard')
FROM json_each((SELECT hard_guards_json
                  FROM orchestrator_decisions
                 WHERE decision_id = ?))
WHERE json_extract(value, '$.outcome') = 'fail'
ORDER BY CAST(key AS INTEGER) ASC
LIMIT 32`

	orchestratorChannelReasonSQL = `
SELECT c.reason
FROM orchestrator_decisions AS o
LEFT JOIN channel_control_decisions AS c
       ON c.decision_id = o.channel_decision_id
WHERE o.decision_id = ?
LIMIT 1`

	modelStatusSQL = `
SELECT call_id, status, latency_ms, created_at, inference_profile,
       base_model_revision, adapter_revision, dataset_version
FROM glimmer_calls
ORDER BY created_at DESC, call_id DESC
LIMIT 1`
)

// controlReadSQL is intentionally enumerable: privacy tests audit every
// Control API-owned statement. notification.list SQL stays with the
// notifications registry and is audited in that package.
var controlReadSQL = []string{
	agentListSQL,
	agentQueueCountsSQL,
	messageListSQL,
	decisionListSQL,
	channelDecisionSQL,
	channelVersionsSQL,
	orchestratorDecisionSQL,
	orchestratorActionsSQL,
	orchestratorGuardsSQL,
	orchestratorFailedGuardsSQL,
	orchestratorChannelReasonSQL,
	modelStatusSQL,
}

type readCursor struct {
	Version  int      `json:"v"`
	Method   string   `json:"m"`
	Filter   string   `json:"f"`
	Position []string `json:"p"`
}

func (s *Server) beginRead(ctx context.Context) (*sql.Tx, error) {
	if s.readDB == nil {
		return nil, errUnsupported
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin control read: %w", err)
	}
	var queryOnly int
	if err := tx.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("verify control read connection: %w", err)
	}
	if queryOnly != 1 {
		_ = tx.Rollback()
		return nil, fmt.Errorf("control read connection is not query-only")
	}
	return tx, nil
}

func finishRead(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit control read: %w", err)
	}
	return nil
}

func (s *Server) readAgents(ctx context.Context, params protocol.AgentListParams) (protocol.AgentListResult, error) {
	const method = protocol.ControlMethodAgentList
	result := protocol.AgentListResult{Agents: []protocol.AgentListItem{}}
	position, err := decodeReadCursor(params.Cursor, "a", cursorFilter(method), 1)
	if err != nil {
		return result, err
	}
	cursorAgent := ""
	hasCursor := 0
	if len(position) != 0 {
		cursorAgent = position[0]
		if !protocol.AgentID(cursorAgent).Valid() {
			return result, invalidCursor()
		}
		hasCursor = 1
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, agentListSQL, hasCursor, cursorAgent, params.EffectiveLimit()+1)
	if err != nil {
		return result, fmt.Errorf("query agent list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type agentRow struct {
		item protocol.AgentListItem
	}
	page := make([]agentRow, 0, params.EffectiveLimit()+1)
	for rows.Next() {
		var agentID, nodeID, harness string
		var sessionID, state, captureMode, harnessVersion sql.NullString
		var deliverable sql.NullInt64
		var draftState, activityState, guardObserved sql.NullString
		var guardVersion sql.NullInt64
		var txFlag, rxFlag, deliveryFlag, continueFlag sql.NullInt64
		if err := rows.Scan(
			&agentID, &nodeID, &harness,
			&sessionID, &state, &captureMode, &deliverable, &harnessVersion,
			&draftState, &activityState, &guardVersion, &guardObserved,
			&txFlag, &rxFlag, &deliveryFlag, &continueFlag,
		); err != nil {
			return result, fmt.Errorf("scan agent list: %w", err)
		}
		item, err := mapAgentRow(agentID, nodeID, harness, sessionID, state, captureMode,
			deliverable, harnessVersion, draftState, activityState, guardVersion,
			guardObserved, txFlag, rxFlag, deliveryFlag, continueFlag)
		if err != nil {
			return result, err
		}
		page = append(page, agentRow{item: item})
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close agent list: %w", err)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate agent list: %w", err)
	}
	if int64(len(page)) > params.EffectiveLimit() {
		page = page[:params.EffectiveLimit()]
		last := page[len(page)-1].item.AgentID
		result.NextCursor, err = encodeReadCursor("a", cursorFilter(method), []string{string(last)})
		if err != nil {
			return result, err
		}
	}
	for _, row := range page {
		result.Agents = append(result.Agents, row.item)
	}
	if err := readAgentQueueCounts(ctx, tx, result.Agents); err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "agents", "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func mapAgentRow(
	agentID, nodeID, harness string,
	sessionID, state, captureMode sql.NullString,
	deliverable sql.NullInt64,
	harnessVersion, draftState, activityState sql.NullString,
	guardVersion sql.NullInt64,
	guardObserved sql.NullString,
	txFlag, rxFlag, deliveryFlag, continueFlag sql.NullInt64,
) (protocol.AgentListItem, error) {
	const method = protocol.ControlMethodAgentList
	item := protocol.AgentListItem{
		AgentID: protocol.AgentID(agentID), NodeID: protocol.NodeID(nodeID),
		Harness: protocol.Harness(harness),
		Phase: protocol.AgentPhaseStatus{
			Value: protocol.AgentPhaseUnknown, Source: protocol.AgentPhaseSourceNone,
		},
	}
	if !item.AgentID.Valid() {
		return item, storedFieldError(method, "agents", "agent_id")
	}
	if !item.NodeID.Valid() {
		return item, storedFieldError(method, "agents", "node_id")
	}
	if !item.Harness.Valid() {
		return item, storedFieldError(method, "agents", "harness")
	}
	if !sessionID.Valid || !protocol.OpaqueID(sessionID.String).Valid() {
		return item, storedFieldError(method, "sessions", "session_id")
	}
	item.SessionState = protocol.SessionState(state.String)
	if !state.Valid || !item.SessionState.Valid() {
		return item, storedFieldError(method, "sessions", "state")
	}
	switch protocol.CaptureMode(captureMode.String) {
	case protocol.CaptureModePTYProxy:
		item.Mode = protocol.AgentRunModeRun
	case protocol.CaptureModeBootstrapPipePane:
		item.Mode = protocol.AgentRunModeObserved
	default:
		return item, storedFieldError(method, "sessions", "capture_mode")
	}
	if !deliverable.Valid || deliverable.Int64 < 0 || deliverable.Int64 > 1 {
		return item, storedFieldError(method, "sessions", "deliverable")
	}
	item.Deliverable = deliverable.Int64 == 1
	if !harnessVersion.Valid || harnessVersion.String == "" || len(harnessVersion.String) > 64 {
		return item, storedFieldError(method, "sessions", "manifest_json.harness_version")
	}
	item.HarnessVersion = harnessVersion.String
	keyboard, err := mapAgentKeyboard(draftState, activityState, guardVersion, guardObserved)
	if err != nil {
		return item, err
	}
	item.Keyboard = keyboard
	flags := []*sql.NullInt64{&txFlag, &rxFlag, &deliveryFlag, &continueFlag}
	values := [4]bool{true, true, true, true}
	columns := []string{"tx_enabled", "rx_enabled", "auto_delivery_enabled", "auto_continue_enabled"}
	for i, flag := range flags {
		if !flag.Valid {
			continue
		}
		if flag.Int64 < 0 || flag.Int64 > 1 {
			return item, storedFieldError(method, "channel_controls", columns[i])
		}
		values[i] = flag.Int64 == 1
	}
	item.Channel = protocol.AgentEffectiveChannel{
		TX: values[0], RX: values[1], AutoDelivery: values[2], AutoContinue: values[3],
		Mode: readAgentMode(values[0], values[1], values[2], values[3]),
	}
	if err := item.Validate(); err != nil {
		return item, storedFieldError(method, "agents", "row")
	}
	return item, nil
}

func readAgentQueueCounts(ctx context.Context, tx *sql.Tx, agents []protocol.AgentListItem) error {
	if len(agents) == 0 {
		return nil
	}
	ids := make([]protocol.AgentID, 0, len(agents))
	positions := make(map[protocol.AgentID]int, len(agents))
	for i := range agents {
		ids = append(ids, agents[i].AgentID)
		positions[agents[i].AgentID] = i
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("encode agent queue page: %w", err)
	}
	rows, err := tx.QueryContext(ctx, agentQueueCountsSQL, string(raw), len(agents))
	if err != nil {
		return fmt.Errorf("query agent queue counts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	seen := make(map[protocol.AgentID]bool, len(agents))
	for rows.Next() {
		var id protocol.AgentID
		var counts protocol.AgentQueueCounts
		if err := rows.Scan(&id, &counts.Queued, &counts.Delivering, &counts.Uncertain); err != nil {
			return fmt.Errorf("scan agent queue counts: %w", err)
		}
		position, ok := positions[id]
		if !ok || seen[id] || counts.Validate() != nil {
			return storedFieldError(protocol.ControlMethodAgentList, "delivery_queue", "counts")
		}
		agents[position].Queue = counts
		seen[id] = true
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close agent queue counts: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate agent queue counts: %w", err)
	}
	if len(seen) != len(agents) {
		return storedFieldError(protocol.ControlMethodAgentList, "delivery_queue", "counts")
	}
	return nil
}

func mapAgentKeyboard(
	draftState, activityState sql.NullString,
	guardVersion sql.NullInt64,
	guardObserved sql.NullString,
) (*protocol.AgentKeyboardState, error) {
	const method = protocol.ControlMethodAgentList
	if !guardVersion.Valid {
		if draftState.Valid || activityState.Valid || guardObserved.Valid {
			return nil, storedFieldError(method, "input_guards", "version")
		}
		return nil, nil
	}
	keyboard := &protocol.AgentKeyboardState{
		DraftState:        protocol.DraftState(draftState.String),
		ActivityState:     protocol.ActivityState(activityState.String),
		InputGuardVersion: guardVersion.Int64,
	}
	if !draftState.Valid || !keyboard.DraftState.Valid() {
		return nil, storedFieldError(method, "input_guards", "draft_state")
	}
	if !activityState.Valid || !keyboard.ActivityState.Valid() {
		return nil, storedFieldError(method, "input_guards", "activity_state")
	}
	if guardVersion.Int64 < 0 {
		return nil, storedFieldError(method, "input_guards", "version")
	}
	observed, err := parseStoredTime(method, "input_guards", "updated_at", guardObserved.String)
	if err != nil || !guardObserved.Valid {
		return nil, storedFieldError(method, "input_guards", "updated_at")
	}
	keyboard.ObservedAt = observed
	if err := keyboard.Validate(); err != nil {
		return nil, storedFieldError(method, "input_guards", "keyboard")
	}
	return keyboard, nil
}

func readAgentMode(tx, rx, delivery, continuation bool) protocol.AgentMode {
	switch {
	case tx && rx && delivery && continuation:
		return protocol.AgentModeFull
	case tx && rx && !delivery && !continuation:
		return protocol.AgentModeManual
	case !tx && rx && delivery && !continuation:
		return protocol.AgentModeReceiveOnly
	case tx && !rx && !continuation:
		return protocol.AgentModeSendOnly
	case !tx && !rx && !delivery && !continuation:
		return protocol.AgentModeOff
	default:
		return ""
	}
}

func (s *Server) readMessages(ctx context.Context, params protocol.MessageListParams) (protocol.MessageListResult, error) {
	rows, err := s.readDeliveryRows(ctx, protocol.ControlMethodMessageList, params)
	result := protocol.MessageListResult{Messages: []protocol.MessageListItem{}}
	if err != nil {
		return result, err
	}
	result.NextCursor = rows.NextCursor
	for _, row := range rows.Deliveries {
		result.Messages = append(result.Messages, protocol.MessageListItem{
			MessageID: row.MessageID, FromAgent: row.FromAgent, ToAgent: row.ToAgent,
			ThreadRef: row.ThreadRef, State: row.State, Priority: row.Priority,
			CreatedAt: row.CreatedAt, ExpiresAt: row.ExpiresAt, Attempts: row.Attempts,
			LastReason: row.LastReason,
		})
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(protocol.ControlMethodMessageList, "delivery_queue", "result")
	}
	return result, nil
}

func (s *Server) readDeliveries(
	ctx context.Context,
	params protocol.MessageListParams,
) (protocol.DeliveryListResult, error) {
	return s.readDeliveryRows(ctx, protocol.ControlMethodDeliveryList, params)
}

// readDeliveryRows uses a descending keyset cursor. Rows inserted after page
// one appear only on a fresh first page. State is mutable, so changing a row
// into or out of a filtered state between pages can make that row appear twice
// or not at all; the cursor promises ordering, not a frozen view.
func (s *Server) readDeliveryRows(
	ctx context.Context,
	method protocol.ControlMethod,
	params protocol.MessageListParams,
) (protocol.DeliveryListResult, error) {
	result := protocol.DeliveryListResult{Deliveries: []protocol.DeliveryListItem{}}
	filter := cursorFilter(method, string(params.AgentID), string(params.State))
	position, err := decodeReadCursor(params.Cursor, "m", filter, 4)
	if err != nil {
		return result, err
	}
	hasCursor := 0
	createdAt, messageID, agentID := "", "", ""
	var generation int64
	if len(position) != 0 {
		createdAt, messageID, agentID = position[0], position[1], position[2]
		generation, err = strconv.ParseInt(position[3], 10, 64)
		if _, timeErr := eventstore.ParseTime(createdAt); timeErr != nil ||
			!protocol.ULID(messageID).Valid() || !protocol.AgentID(agentID).Valid() ||
			err != nil || generation < 1 {
			return result, invalidCursor()
		}
		hasCursor = 1
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, messageListSQL,
		string(params.AgentID), string(params.AgentID), string(params.State), string(params.State),
		hasCursor, createdAt, messageID, agentID, generation, params.EffectiveLimit()+1)
	if err != nil {
		return result, fmt.Errorf("query message list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type messageRow struct {
		item       protocol.DeliveryListItem
		generation int64
	}
	page := make([]messageRow, 0, params.EffectiveLimit()+1)
	for rows.Next() {
		var raw messageRow
		var message, toAgent, fromAgent, threadRef, state, created, expires, ackStatus string
		var priorityRank, attempts int64
		var lastReason sql.NullString
		if err := rows.Scan(&message, &toAgent, &raw.generation, &fromAgent,
			&threadRef, &state, &priorityRank, &created, &expires, &attempts, &lastReason,
			&ackStatus); err != nil {
			return result, fmt.Errorf("scan message list: %w", err)
		}
		item, err := mapDeliveryRow(method, message, fromAgent, toAgent, threadRef, state,
			priorityRank, created, expires, attempts, lastReason, ackStatus)
		if err != nil {
			return result, err
		}
		if raw.generation < 1 {
			return result, storedFieldError(method, "delivery_queue", "delivery_generation")
		}
		raw.item = item
		page = append(page, raw)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close message list: %w", err)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate message list: %w", err)
	}
	if int64(len(page)) > params.EffectiveLimit() {
		page = page[:params.EffectiveLimit()]
		last := page[len(page)-1]
		result.NextCursor, err = encodeReadCursor("m", filter, []string{
			eventstore.FormatTime(last.item.CreatedAt), string(last.item.MessageID),
			string(last.item.ToAgent), strconv.FormatInt(last.generation, 10),
		})
		if err != nil {
			return result, err
		}
	}
	for _, row := range page {
		result.Deliveries = append(result.Deliveries, row.item)
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "delivery_queue", "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func mapDeliveryRow(
	method protocol.ControlMethod,
	message, fromAgent, toAgent, threadRef, state string,
	priorityRank int64,
	created, expires string,
	attempts int64,
	lastReason sql.NullString,
	ackStatus string,
) (protocol.DeliveryListItem, error) {
	item := protocol.DeliveryListItem{
		MessageID: protocol.ULID(message), FromAgent: protocol.AgentID(fromAgent),
		ToAgent: protocol.AgentID(toAgent), ThreadRef: protocol.ThreadRef(threadRef),
		State: protocol.DeliveryQueueState(state), Attempts: attempts,
	}
	if !item.MessageID.Valid() {
		return item, storedFieldError(method, "delivery_queue", "message_id")
	}
	if !item.FromAgent.Valid() {
		return item, storedFieldError(method, "delivery_queue", "from_agent")
	}
	if !item.ToAgent.Valid() {
		return item, storedFieldError(method, "delivery_queue", "agent_id")
	}
	if !item.ThreadRef.Valid() {
		return item, storedFieldError(method, "delivery_queue", "thread_ref")
	}
	if !item.State.Valid() {
		return item, storedFieldError(method, "delivery_queue", "state")
	}
	priority, err := eventstore.PriorityFromRank(priorityRank)
	if err != nil {
		return item, storedFieldError(method, "delivery_queue", "priority")
	}
	item.Priority = priority
	item.CreatedAt, err = parseStoredTime(method, "delivery_queue", "created_at", created)
	if err != nil {
		return item, err
	}
	item.ExpiresAt, err = parseStoredTime(method, "delivery_queue", "expires_at", expires)
	if err != nil {
		return item, err
	}
	if attempts < 0 {
		return item, storedFieldError(method, "delivery_queue", "attempts")
	}
	if lastReason.Valid {
		reason := protocol.ReasonCode(lastReason.String)
		if reason.Valid() {
			item.LastReason = reason
		} else {
			logStoredField(method, "delivery_attempts", "terminal_reason")
		}
	}
	if ackStatus != "" {
		item.AckStatus = protocol.AckStatus(ackStatus)
		if !item.AckStatus.Valid() {
			return item, storedFieldError(method, "acks", "status")
		}
	}
	if err := item.Validate(); err != nil {
		return item, storedFieldError(method, "delivery_queue", "row")
	}
	return item, nil
}

// readNotifications uses the notification registry's descending keyset read.
// The registry owns its SQL and row decoder; Control API owns the opaque
// cursor and public metadata projection.
func (s *Server) readNotifications(
	ctx context.Context,
	params protocol.NotificationListParams,
) (protocol.NotificationListResult, error) {
	const method = protocol.ControlMethodNotificationList
	result := protocol.NotificationListResult{Notifications: []protocol.NotificationListItem{}}
	filter := cursorFilter(method, string(params.State))
	position, err := decodeReadCursor(params.Cursor, "n", filter, 2)
	if err != nil {
		return result, err
	}
	var beforeCreatedAt *time.Time
	beforeID := protocol.OpaqueID("")
	if len(position) != 0 {
		nanoseconds, parseErr := strconv.ParseInt(position[0], 10, 64)
		parsed := time.Unix(0, nanoseconds).UTC()
		beforeID = protocol.OpaqueID(position[1])
		if parseErr != nil || !beforeID.Valid() {
			return result, invalidCursor()
		}
		beforeCreatedAt = &parsed
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	page, err := notificationstore.List(ctx, tx, notificationstore.ListQuery{
		State: notificationstore.State(params.State), BeforeCreatedAt: beforeCreatedAt,
		BeforeID: beforeID, Limit: params.EffectiveLimit() + 1,
	})
	if err != nil {
		if errors.Is(err, notificationstore.ErrInvalidRecord) {
			return result, storedFieldError(method, "human_notifications", "row")
		}
		return result, fmt.Errorf("read notification registry: %w", err)
	}
	result.PendingCount = page.PendingCount
	if int64(len(page.Notifications)) > params.EffectiveLimit() {
		page.Notifications = page.Notifications[:params.EffectiveLimit()]
		last := page.Notifications[len(page.Notifications)-1]
		result.NextCursor, err = encodeReadCursor("n", filter, []string{
			strconv.FormatInt(last.CreatedAt.UnixNano(), 10), string(last.ID),
		})
		if err != nil {
			return result, err
		}
	}
	for _, row := range page.Notifications {
		item := mapNotificationRow(row)
		if err := item.Validate(); err != nil {
			column := "row"
			var violation *protocol.ValidationError
			if errors.As(err, &violation) {
				column = violation.Path
			}
			return result, storedFieldError(method, "human_notifications", column)
		}
		result.Notifications = append(result.Notifications, item)
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "human_notifications", "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func mapNotificationRow(row notificationstore.Notification) protocol.NotificationListItem {
	recipients := make([]protocol.AgentID, len(row.LocalRecipients))
	copy(recipients, row.LocalRecipients)
	return protocol.NotificationListItem{
		NotificationID:  row.ID,
		Class:           protocol.NotificationClass(row.Class),
		ObjectID:        row.ObjectID,
		SenderAgent:     row.Sender,
		RecipientAgents: recipients,
		Priority:        row.Priority,
		Reason:          protocol.NotificationReason(row.Reason),
		State:           protocol.NotificationState(row.State),
		Adapter:         protocol.NotificationAdapter(row.Adapter),
		Attempts:        int64(row.Attempts),
		LastErrorReason: protocol.NotificationFailureReason(row.LastError),
		CreatedAt:       row.CreatedAt,
		UpdatedAt:       row.UpdatedAt,
		LastAttemptAt:   cloneReadTime(row.LastAttemptAt),
		NextAttemptAt:   cloneReadTime(row.NextAttemptAt),
		RenderedAt:      cloneReadTime(row.RenderedAt),
	}
}

func cloneReadTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

type decisionPageRow struct {
	item          protocol.DecisionListItem
	channelReason string
}

func (s *Server) readDecisions(ctx context.Context, params protocol.DecisionListParams) (protocol.DecisionListResult, error) {
	const method = protocol.ControlMethodDecisionList
	result := protocol.DecisionListResult{Decisions: []protocol.DecisionListItem{}}
	filter := cursorFilter(method, string(params.AgentID), string(params.MessageID), string(params.Type))
	position, err := decodeReadCursor(params.Cursor, "d", filter, 2)
	if err != nil {
		return result, err
	}
	hasCursor := 0
	createdAt, decisionID := "", ""
	if len(position) != 0 {
		createdAt, decisionID = position[0], position[1]
		if _, timeErr := eventstore.ParseTime(createdAt); timeErr != nil ||
			!protocol.DecisionID(decisionID).Valid() {
			return result, invalidCursor()
		}
		hasCursor = 1
	}
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, decisionListSQL,
		string(params.AgentID), string(params.AgentID),
		string(params.MessageID), string(params.MessageID),
		string(params.Type), string(params.Type),
		hasCursor, createdAt, decisionID, params.EffectiveLimit()+1)
	if err != nil {
		return result, fmt.Errorf("query decision list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	page := make([]decisionPageRow, 0, params.EffectiveLimit()+1)
	for rows.Next() {
		var raw decisionPageRow
		var id, family, stage, outcome, action, agent, message, created string
		if err := rows.Scan(&id, &family, &stage, &outcome, &action, &agent,
			&message, &created, &raw.channelReason); err != nil {
			return result, fmt.Errorf("scan decision list: %w", err)
		}
		raw.item, err = mapDecisionSummary(method, id, family, stage, outcome,
			action, agent, message, created)
		if err != nil {
			return result, err
		}
		page = append(page, raw)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close decision list: %w", err)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate decision list: %w", err)
	}
	if int64(len(page)) > params.EffectiveLimit() {
		page = page[:params.EffectiveLimit()]
		last := page[len(page)-1].item
		result.NextCursor, err = encodeReadCursor("d", filter, []string{
			eventstore.FormatTime(last.CreatedAt), string(last.DecisionID),
		})
		if err != nil {
			return result, err
		}
	}
	for i := range page {
		if page[i].item.Type == protocol.DecisionTypeChannel {
			reason := protocol.ReasonCode(page[i].channelReason)
			if !reason.Valid() {
				return result, storedFieldError(method, "channel_control_decisions", "reason")
			}
			page[i].item.ReasonCodes = []protocol.ReasonCode{reason}
		} else {
			page[i].item.ReasonCodes, err = readOrchestratorReasons(ctx, tx, method,
				protocol.ULID(page[i].item.DecisionID), page[i].item.SelectedAction)
			if err != nil {
				return result, err
			}
		}
		if err := page[i].item.Validate(); err != nil {
			return result, storedFieldError(method, decisionTable(page[i].item.Type), "row")
		}
		result.Decisions = append(result.Decisions, page[i].item)
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "decisions", "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func mapDecisionSummary(
	method protocol.ControlMethod,
	id, family, stage, outcome, action, agent, message, created string,
) (protocol.DecisionListItem, error) {
	item := protocol.DecisionListItem{
		DecisionID: protocol.DecisionID(id), Type: protocol.DecisionType(family),
		Stage: protocol.DecisionStage(stage), Outcome: protocol.DecisionOutcome(outcome),
		SelectedAction: protocol.OrchestratorAction(action), AgentID: protocol.AgentID(agent),
		MessageID: protocol.ULID(message),
	}
	table := decisionTable(item.Type)
	if !item.DecisionID.Valid() {
		return item, storedFieldError(method, table, "decision_id")
	}
	if !item.Type.Valid() {
		return item, storedFieldError(method, "decisions", "family")
	}
	if !item.AgentID.Valid() {
		return item, storedFieldError(method, table, "agent_id")
	}
	if !item.MessageID.Valid() {
		return item, storedFieldError(method, table, "message_id")
	}
	var err error
	item.CreatedAt, err = parseStoredTime(method, table, "created_at", created)
	if err != nil {
		return item, err
	}
	switch item.Type {
	case protocol.DecisionTypeChannel:
		if !item.Stage.Valid() {
			return item, storedFieldError(method, table, "stage")
		}
		if !item.Outcome.Valid() {
			return item, storedFieldError(method, table, "outcome")
		}
	case protocol.DecisionTypeOrchestrator:
		if !item.SelectedAction.Valid() {
			return item, storedFieldError(method, table, "selected_action")
		}
	}
	return item, nil
}

func (s *Server) readDecision(ctx context.Context, params protocol.DecisionGetParams) (protocol.DecisionGetResult, error) {
	tx, err := s.beginRead(ctx)
	if err != nil {
		return protocol.DecisionGetResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var result protocol.DecisionGetResult
	if strings.HasPrefix(string(params.DecisionID), "chd_") {
		result, err = readChannelDecision(ctx, tx, params.DecisionID)
	} else {
		result, err = readOrchestratorDecision(ctx, tx, params.DecisionID)
	}
	if err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(protocol.ControlMethodDecisionGet,
			decisionTable(result.Type), "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func readChannelDecision(ctx context.Context, tx *sql.Tx, id protocol.DecisionID) (protocol.DecisionGetResult, error) {
	const method = protocol.ControlMethodDecisionGet
	var decisionID, stage, outcome, agent, message, created, reason, scope string
	err := tx.QueryRowContext(ctx, channelDecisionSQL, id).Scan(
		&decisionID, &stage, &outcome, &agent, &message, &created, &reason, &scope)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.DecisionGetResult{}, channel.ErrNotFound
	}
	if err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("query channel decision: %w", err)
	}
	summary, err := mapDecisionSummary(method, decisionID, string(protocol.DecisionTypeChannel),
		stage, outcome, "", agent, message, created)
	if err != nil {
		return protocol.DecisionGetResult{}, err
	}
	reasonCode := protocol.ReasonCode(reason)
	if !reasonCode.Valid() {
		return protocol.DecisionGetResult{}, storedFieldError(method, "channel_control_decisions", "reason")
	}
	decidingScope := protocol.ExplanationScope(scope)
	if !decidingScope.Valid() {
		return protocol.DecisionGetResult{}, storedFieldError(method, "channel_control_decisions", "explanation_json.scope")
	}
	versions := protocol.ControlVersions{}
	rows, err := tx.QueryContext(ctx, channelVersionsSQL, id)
	if err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("query channel decision versions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key string
		var version int64
		if err := rows.Scan(&key, &version); err != nil {
			return protocol.DecisionGetResult{}, fmt.Errorf("scan channel decision versions: %w", err)
		}
		if key == "" || version < 0 {
			return protocol.DecisionGetResult{}, storedFieldError(method, "channel_control_decisions", "control_versions_json")
		}
		versions[key] = version
	}
	if err := rows.Close(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("close channel decision versions: %w", err)
	}
	if err := rows.Err(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("iterate channel decision versions: %w", err)
	}
	return protocol.DecisionGetResult{
		DecisionID: summary.DecisionID, Type: summary.Type, Stage: summary.Stage,
		Outcome: summary.Outcome, AgentID: summary.AgentID, MessageID: summary.MessageID,
		CreatedAt: summary.CreatedAt, ReasonCodes: []protocol.ReasonCode{reasonCode},
		ChannelEvidence: &protocol.ChannelDecisionEvidence{
			ControlVersions: versions, DecidingScope: decidingScope,
		},
	}, nil
}

func readOrchestratorDecision(ctx context.Context, tx *sql.Tx, id protocol.DecisionID) (protocol.DecisionGetResult, error) {
	const method = protocol.ControlMethodDecisionGet
	var decisionID, action, agent, message, created, policy, execution, channelDecision string
	var model, callID sql.NullString
	err := tx.QueryRowContext(ctx, orchestratorDecisionSQL, id).Scan(
		&decisionID, &action, &agent, &message, &created, &policy,
		&model, &callID, &execution, &channelDecision)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.DecisionGetResult{}, channel.ErrNotFound
	}
	if err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("query orchestrator decision: %w", err)
	}
	summary, err := mapDecisionSummary(method, decisionID, string(protocol.DecisionTypeOrchestrator),
		"", "", action, agent, message, created)
	if err != nil {
		return protocol.DecisionGetResult{}, err
	}
	evidence := &protocol.OrchestratorDecisionEvidence{
		AllowedActions: []protocol.OrchestratorAction{},
		Guards:         []protocol.DecisionGuardEvidence{}, SelectedAction: summary.SelectedAction,
		PolicyVersion:     protocol.ModelMetadataValue(policy),
		ExecutionStatus:   protocol.ExecutionStatus(execution),
		ChannelDecisionID: protocol.ChannelDecisionID(channelDecision),
	}
	if !evidence.PolicyVersion.Valid() {
		return protocol.DecisionGetResult{}, storedFieldError(method, "orchestrator_decisions", "policy_version")
	}
	if !evidence.ExecutionStatus.Valid() {
		return protocol.DecisionGetResult{}, storedFieldError(method, "orchestrator_decisions", "execution_status")
	}
	if !evidence.ChannelDecisionID.Valid() {
		return protocol.DecisionGetResult{}, storedFieldError(method, "orchestrator_decisions", "channel_decision_id")
	}
	if model.Valid {
		value := protocol.ModelMetadataValue(model.String)
		if value.Valid() {
			evidence.DecisionModel = value
		} else {
			logStoredField(method, "orchestrator_decisions", "decision_model")
		}
	}
	if callID.Valid {
		value := protocol.OpaqueID(callID.String)
		if value.Valid() {
			evidence.GlimmerCallID = value
		} else {
			logStoredField(method, "orchestrator_decisions", "glimmer_call_id")
		}
	}
	actionRows, err := tx.QueryContext(ctx, orchestratorActionsSQL, id)
	if err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("query orchestrator actions: %w", err)
	}
	defer func() { _ = actionRows.Close() }()
	for actionRows.Next() {
		var value string
		if err := actionRows.Scan(&value); err != nil {
			return protocol.DecisionGetResult{}, fmt.Errorf("scan orchestrator actions: %w", err)
		}
		actionValue := protocol.OrchestratorAction(value)
		if !actionValue.Valid() {
			return protocol.DecisionGetResult{}, storedFieldError(method, "orchestrator_decisions", "allowed_actions_json")
		}
		evidence.AllowedActions = append(evidence.AllowedActions, actionValue)
	}
	if err := actionRows.Close(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("close orchestrator actions: %w", err)
	}
	if err := actionRows.Err(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("iterate orchestrator actions: %w", err)
	}
	guardRows, err := tx.QueryContext(ctx, orchestratorGuardsSQL, id)
	if err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("query orchestrator guards: %w", err)
	}
	defer func() { _ = guardRows.Close() }()
	for guardRows.Next() {
		var guard, outcome string
		if err := guardRows.Scan(&guard, &outcome); err != nil {
			return protocol.DecisionGetResult{}, fmt.Errorf("scan orchestrator guards: %w", err)
		}
		item := protocol.DecisionGuardEvidence{
			Guard: protocol.GuardName(guard), Outcome: protocol.GuardOutcome(outcome),
		}
		if err := item.Validate(); err != nil {
			return protocol.DecisionGetResult{}, storedFieldError(method, "orchestrator_decisions", "hard_guards_json")
		}
		evidence.Guards = append(evidence.Guards, item)
	}
	if err := guardRows.Close(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("close orchestrator guards: %w", err)
	}
	if err := guardRows.Err(); err != nil {
		return protocol.DecisionGetResult{}, fmt.Errorf("iterate orchestrator guards: %w", err)
	}
	reasons, err := readOrchestratorReasons(ctx, tx, method, protocol.ULID(id), summary.SelectedAction)
	if err != nil {
		return protocol.DecisionGetResult{}, err
	}
	return protocol.DecisionGetResult{
		DecisionID: summary.DecisionID, Type: summary.Type,
		SelectedAction: summary.SelectedAction, AgentID: summary.AgentID,
		MessageID: summary.MessageID, CreatedAt: summary.CreatedAt,
		ReasonCodes: reasons, OrchestratorEvidence: evidence,
	}, nil
}

func readOrchestratorReasons(
	ctx context.Context,
	tx *sql.Tx,
	method protocol.ControlMethod,
	id protocol.ULID,
	selected protocol.OrchestratorAction,
) ([]protocol.ReasonCode, error) {
	reasons := make([]protocol.ReasonCode, 0, 4)
	seen := make(map[protocol.ReasonCode]bool)
	rows, err := tx.QueryContext(ctx, orchestratorFailedGuardsSQL, id)
	if err != nil {
		return nil, fmt.Errorf("query failed orchestrator guards: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan failed orchestrator guards: %w", err)
		}
		reason := protocol.ReasonCode(raw.String)
		if !raw.Valid || !reason.Valid() {
			logStoredField(method, "orchestrator_decisions", "hard_guards_json")
			continue
		}
		if !seen[reason] && len(reasons) < 32 {
			seen[reason] = true
			reasons = append(reasons, reason)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close failed orchestrator guards: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate failed orchestrator guards: %w", err)
	}
	if len(reasons) != 0 {
		return reasons, nil
	}
	var channelReason sql.NullString
	if err := tx.QueryRowContext(ctx, orchestratorChannelReasonSQL, id).Scan(&channelReason); err != nil {
		return nil, fmt.Errorf("query orchestrator channel reason: %w", err)
	}
	if channelReason.Valid {
		reason := protocol.ReasonCode(channelReason.String)
		if reason.Valid() {
			return []protocol.ReasonCode{reason}, nil
		}
		logStoredField(method, "channel_control_decisions", "reason")
	}
	fallback := protocol.ReasonCode(selected)
	if !fallback.Valid() {
		return nil, storedFieldError(method, "orchestrator_decisions", "selected_action")
	}
	return []protocol.ReasonCode{fallback}, nil
}

func (s *Server) readModelStatus(ctx context.Context) (protocol.ModelStatusResult, error) {
	const method = protocol.ControlMethodModelStatus
	result := unavailableModelStatus()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	var callID, status, created, profile, baseRevision string
	var latency int64
	var adapterRevision, datasetVersion sql.NullString
	err = tx.QueryRowContext(ctx, modelStatusSQL).Scan(
		&callID, &status, &latency, &created, &profile, &baseRevision,
		&adapterRevision, &datasetVersion)
	if errors.Is(err, sql.ErrNoRows) {
		if err := finishRead(tx); err != nil {
			return result, err
		}
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("query model status: %w", err)
	}
	observed, err := parseStoredTime(method, "glimmer_calls", "created_at", created)
	if err != nil {
		return result, err
	}
	last := &protocol.ModelLastCall{
		CallID: protocol.OpaqueID(callID), Status: protocol.GlimmerCallStatus(status),
		LatencyMS: latency, ObservedAt: observed,
	}
	if !last.CallID.Valid() {
		return result, storedFieldError(method, "glimmer_calls", "call_id")
	}
	if !last.Status.Valid() {
		return result, storedFieldError(method, "glimmer_calls", "status")
	}
	if err := last.Validate(); err != nil {
		return result, storedFieldError(method, "glimmer_calls", "latency_ms")
	}
	active := &protocol.ActiveModelProfile{
		Profile: protocol.InferenceProfile(profile), Source: protocol.ModelProfileSourceGlimmerCall,
		ObservedAt: observed,
	}
	if err := active.Validate(); err != nil {
		return result, storedFieldError(method, "glimmer_calls", "inference_profile")
	}
	result.ActiveProfile = active
	result.LastCall = last
	result.Deployment.BaseRevision = modelMetadata(method, "base_model_revision", sql.NullString{String: baseRevision, Valid: true})
	result.Deployment.AdapterRevision = modelMetadata(method, "adapter_revision", adapterRevision)
	result.Deployment.DatasetVersion = modelMetadata(method, "dataset_version", datasetVersion)
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "glimmer_calls", "result")
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	return result, nil
}

func unavailableModelStatus() protocol.ModelStatusResult {
	unavailable := protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityUnavailable}
	return protocol.ModelStatusResult{Deployment: protocol.ModelDeploymentMetadata{
		BaseRevision: unavailable, AdapterRevision: unavailable,
		DatasetVersion: unavailable, ABVariant: unavailable,
	}}
}

func modelMetadata(method protocol.ControlMethod, column string, raw sql.NullString) protocol.ModelMetadata {
	if !raw.Valid {
		return protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityUnavailable}
	}
	value := protocol.ModelMetadataValue(raw.String)
	if !value.Valid() {
		logStoredField(method, "glimmer_calls", column)
		return protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityUnknown}
	}
	return protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityKnown, Value: value}
}

func decisionTable(family protocol.DecisionType) string {
	if family == protocol.DecisionTypeChannel {
		return "channel_control_decisions"
	}
	return "orchestrator_decisions"
}

func parseStoredTime(method protocol.ControlMethod, table, column, raw string) (time.Time, error) {
	parsed, err := eventstore.ParseTime(raw)
	if err != nil {
		return time.Time{}, storedFieldError(method, table, column)
	}
	return parsed, nil
}

func logStoredField(method protocol.ControlMethod, table, column string) {
	slog.Error("Invalid stored control read field", "method", method, "table", table, "column", column)
}

func storedFieldError(method protocol.ControlMethod, table, column string) error {
	logStoredField(method, table, column)
	return fmt.Errorf("invalid stored control read field %s.%s", table, column)
}

func cursorFilter(method protocol.ControlMethod, values ...string) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, string(method))
	for _, value := range values {
		_, _ = io.WriteString(hash, "\x00")
		_, _ = io.WriteString(hash, value)
	}
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil)[:8])
}

func encodeReadCursor(method, filter string, position []string) (protocol.PageCursor, error) {
	raw, err := json.Marshal(readCursor{Version: 1, Method: method, Filter: filter, Position: position})
	if err != nil {
		return "", fmt.Errorf("encode read cursor: %w", err)
	}
	cursor := protocol.PageCursor(base64.RawURLEncoding.EncodeToString(raw))
	if !cursor.Valid() {
		return "", fmt.Errorf("encoded read cursor exceeds protocol bound")
	}
	return cursor, nil
}

func decodeReadCursor(cursor protocol.PageCursor, method, filter string, positions int) ([]string, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(cursor))
	if err != nil {
		return nil, invalidCursor()
	}
	var decoded readCursor
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		return nil, invalidCursor()
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, invalidCursor()
	}
	if decoded.Version != 1 || decoded.Method != method || decoded.Filter != filter ||
		len(decoded.Position) != positions {
		return nil, invalidCursor()
	}
	return decoded.Position, nil
}

func invalidCursor() error {
	return fmt.Errorf("invalid page cursor: %w", protocol.ErrInvalid)
}
