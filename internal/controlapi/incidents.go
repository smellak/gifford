package controlapi

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// Each source is read once. All per-row joins address existing primary or
// partial indexes: there is no correlated history scan. No bodies, argv,
// operator notes or terminal contents belong in this projection.
const incidentListSQL = `
WITH incidents AS (
 SELECT 'DELIVERY_UNCERTAIN' AS kind, d.delivery_id AS source_id,
        d.agent_id, d.message_id, '' AS owner_id, 'delivery' AS fix_area,
        'UNCERTAIN' AS state, COALESCE(d.terminal_reason, 'DELIVERY_UNCERTAIN') AS reason,
        COALESCE(d.finished_at, d.updated_at) AS opened_at, d.updated_at,
        e.created_at AS escalated_at
 FROM delivery_attempts d
 LEFT JOIN escalations e ON e.message_id = d.message_id AND e.agent_id = d.agent_id
 WHERE d.state = 'UNCERTAIN' AND d.resolution_approval_id IS NULL
 UNION ALL
 SELECT 'APPROVAL_PENDING', a.approval_id, COALESCE(a.agent_id, ''),
        CASE WHEN a.object_type = 'message' THEN a.object_id ELSE '' END,
        '', 'delivery', 'PENDING', a.source, a.created_at, a.created_at, e.created_at
 FROM approval_requests a
 LEFT JOIN escalations e ON a.object_type = 'message' AND e.message_id = a.object_id
                            AND e.agent_id = a.agent_id
 WHERE a.state = 'PENDING'
 UNION ALL
 SELECT 'TMUX_INSTANCE_INVALID', i.instance_id, i.agent_id, '', '', 'instances',
        'INVALID', i.invalid_reason, i.updated_at, i.updated_at, NULL
 FROM tmux_instances i WHERE i.state = 'INVALID'
 UNION ALL
 SELECT 'DETECTOR_UNCERTAIN', i.instance_id, i.agent_id, '', '', 'detector',
        'UNCERTAIN',
        CASE WHEN g.draft_state = 'UNKNOWN' THEN 'DRAFT_UNKNOWN' ELSE 'ACTIVITY_UNKNOWN' END,
        g.updated_at, g.updated_at, NULL
 FROM tmux_instances i
 JOIN sessions s ON s.session_id = i.session_id AND s.agent_id = i.agent_id
 JOIN input_guards g ON g.session_id = s.session_id AND g.agent_id = i.agent_id
 WHERE i.state = 'ACTIVE' AND s.state = 'CONNECTED' AND s.ended_at IS NULL
       AND s.capture_mode = 'tmux_attach' AND g.detection_source = 'tmux_attach'
       AND (g.draft_state = 'UNKNOWN' OR g.activity_state = 'UNKNOWN')
 UNION ALL
 SELECT 'ESCALATED', e.message_id, e.agent_id, e.message_id, e.operator_agent,
        'unknown', 'ESCALATED', 'ESCALATED', e.created_at, e.created_at, e.created_at
 FROM escalations e
)
SELECT i.kind, i.source_id, i.agent_id, i.message_id, i.owner_id, i.fix_area, i.state, i.reason,
       i.opened_at, i.updated_at, i.escalated_at, b.source_read_at
FROM incidents i
LEFT JOIN bridge_message_mappings b ON b.gifford_message_id = i.message_id
WHERE (? = '' OR i.agent_id = ?) AND (? = '' OR i.kind = ?)
  AND (? = 0 OR (i.opened_at, i.kind, i.source_id) < (?, ?, ?))
ORDER BY i.opened_at DESC, i.kind DESC, i.source_id DESC
LIMIT ?`

// incidentReadBudget is the panel's polling slot for one incident.list read.
const incidentReadBudget = 250 * time.Millisecond

// The escalation schema does not record an origin or a next-notice schedule.
// Matching an old failure by agent/message would invent provenance, so its
// fix_area is explicitly unknown until an origin is actually stored.
func (s *Server) readIncidents(ctx context.Context, params protocol.IncidentListParams) (protocol.IncidentListResult, error) {
	const method = protocol.ControlMethodIncidentList
	result := protocol.IncidentListResult{Incidents: []protocol.IncidentListItem{}}
	filter := cursorFilter(method, string(params.AgentID), string(params.Kind), string(s.operator))
	position, err := decodeIncidentCursor(params.Cursor, filter)
	if err != nil {
		return result, err
	}
	hasCursor := 0
	opened, kind, id := "", "", ""
	if len(position) != 0 {
		opened, kind, id = position[0], position[1], position[2]
		if _, err := eventstore.ParseTime(opened); err != nil || !protocol.IncidentKind(kind).Valid() || !protocol.OpaqueID(id).Valid() {
			return result, invalidCursor()
		}
		hasCursor = 1
	}
	// Cancel the database query as well, so a slow projection cannot
	// outlive the panel's polling slot.
	budget := s.incidentBudget
	if budget <= 0 {
		budget = incidentReadBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	tx, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, incidentListSQL,
		params.AgentID, params.AgentID, params.Kind, params.Kind,
		hasCursor, opened, kind, id, params.EffectiveLimit()+1)
	if err != nil {
		return result, fmt.Errorf("query incident list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item protocol.IncidentListItem
		var sourceID, owner, openedAt, updatedAt string
		var escalated, sourceRead sql.NullString
		if err := rows.Scan(&item.Kind, &sourceID, &item.AgentID, &item.MessageID,
			&owner, &item.FixArea, &item.State, &item.Reason, &openedAt, &updatedAt, &escalated, &sourceRead); err != nil {
			return result, fmt.Errorf("scan incident: %w", err)
		}
		if !protocol.OpaqueID(sourceID).Valid() || !incidentReasonValid(item) {
			return result, storedFieldError(method, "incidents", "source_or_reason")
		}
		item.IncidentID = string(item.Kind) + ":" + sourceID
		item.OwnerKind = protocol.IncidentOwnerOperator
		if item.Kind != protocol.IncidentKindEscalated {
			owner = string(s.operator)
		}
		if owner != "" {
			ownerID := protocol.AgentID(owner)
			item.OwnerID = &ownerID
		} else if item.Kind == protocol.IncidentKindEscalated {
			return result, storedFieldError(method, "escalations", "operator_agent")
		}
		if item.OpenedAt, err = parseStoredTime(method, "incidents", "opened_at", openedAt); err != nil {
			return result, err
		}
		if item.UpdatedAt, err = parseStoredTime(method, "incidents", "updated_at", updatedAt); err != nil {
			return result, err
		}
		if escalated.Valid {
			stamp, err := parseStoredTime(method, "escalations", "created_at", escalated.String)
			if err != nil {
				return result, err
			}
			item.EscalatedAt = &stamp
		}
		if sourceRead.Valid {
			stamp, err := parseStoredTime(method, "bridge_message_mappings", "source_read_at", sourceRead.String)
			if err != nil {
				return result, err
			}
			item.SourceReadAt = &stamp
		}
		if err := item.Validate(); err != nil {
			return result, storedFieldError(method, "incidents", "row")
		}
		result.Incidents = append(result.Incidents, item)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate incidents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close incidents: %w", err)
	}
	if int64(len(result.Incidents)) > params.EffectiveLimit() {
		result.Incidents = result.Incidents[:params.EffectiveLimit()]
		last := result.Incidents[len(result.Incidents)-1]
		_, sourceID, _ := strings.Cut(last.IncidentID, ":")
		result.NextCursor, err = encodeIncidentCursor(filter, last.OpenedAt, last.Kind, sourceID)
		if err != nil {
			return result, err
		}
	}
	if err := finishRead(tx); err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, storedFieldError(method, "incidents", "result")
	}
	return result, nil
}

// The compact tuple fits the existing 256-byte cursor bound even for a
// 128-character source id; the filter binds it to this method and operator.
func encodeIncidentCursor(filter string, opened time.Time, kind protocol.IncidentKind, sourceID string) (protocol.PageCursor, error) {
	kinds := kind.EnumValues()
	for index, value := range kinds {
		if value != string(kind) {
			continue
		}
		raw, err := json.Marshal([5]string{"i1", filter, eventstore.FormatTime(opened), strconv.Itoa(index), sourceID})
		if err != nil {
			return "", fmt.Errorf("encode incident cursor: %w", err)
		}
		cursor := protocol.PageCursor(base64.RawURLEncoding.EncodeToString(raw))
		if !cursor.Valid() {
			return "", invalidCursor()
		}
		return cursor, nil
	}
	return "", invalidCursor()
}

func decodeIncidentCursor(cursor protocol.PageCursor, filter string) ([]string, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(cursor))
	if err != nil {
		return nil, invalidCursor()
	}
	var tuple []string
	if err := json.Unmarshal(raw, &tuple); err != nil || len(tuple) != 5 || tuple[0] != "i1" || tuple[1] != filter {
		return nil, invalidCursor()
	}
	index, err := strconv.Atoi(tuple[3])
	kinds := protocol.IncidentKind("").EnumValues()
	if err != nil || index < 0 || index >= len(kinds) {
		return nil, invalidCursor()
	}
	return []string{tuple[2], kinds[index], tuple[4]}, nil
}

func incidentReasonValid(item protocol.IncidentListItem) bool {
	switch item.Kind {
	case protocol.IncidentKindDeliveryUncertain:
		return protocol.ReasonCode(item.Reason).Valid() || item.Reason == string(protocol.IncidentKindDeliveryUncertain)
	case protocol.IncidentKindApprovalPending:
		return protocol.ApprovalSource(item.Reason).Valid()
	case protocol.IncidentKindTmuxInstanceInvalid:
		switch tmuxinstance.Reason(item.Reason) {
		case tmuxinstance.ReasonBootChanged, tmuxinstance.ReasonSocketChanged, tmuxinstance.ReasonOwnerChanged,
			tmuxinstance.ReasonPaneGone, tmuxinstance.ReasonPanePIDChanged, tmuxinstance.ReasonPaneStartChanged,
			tmuxinstance.ReasonHarnessChanged, tmuxinstance.ReasonChainChanged, tmuxinstance.ReasonAdminRevoked:
			return true
		}
	case protocol.IncidentKindDetectorUncertain:
		return item.Reason == "DRAFT_UNKNOWN" || item.Reason == "ACTIVITY_UNKNOWN"
	case protocol.IncidentKindEscalated:
		return item.Reason == string(protocol.IncidentKindEscalated)
	}
	return false
}
