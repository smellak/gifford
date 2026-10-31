package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
)

const controlEventTimeout = 5 * time.Second

type controlEventValue struct {
	Deleted      bool                 `json:"deleted"`
	Mode         protocol.ControlMode `json:"mode"`
	TX           *bool                `json:"tx"`
	RX           *bool                `json:"rx"`
	AutoDelivery *bool                `json:"auto_delivery"`
	AutoContinue *bool                `json:"auto_continue"`
}

type controlRestriction struct {
	payload   protocol.ChannelEventPayload
	recipient bool
	sender    bool
	pause     protocol.AgentID
}

// controlEventSink reacts after a control transaction commits. It finds only
// agents with PREPARED attempts affected by the new restriction, then asks the
// worker to cancel them before a commit can race the new control. An agent
// control that turns automatic delivery off pauses the agent's Proxy; any
// other agent control asks the worker to resume it, which the worker does
// only if the agent's confirmed control allows automatic delivery.
type controlEventSink struct {
	db      *sql.DB
	timeout time.Duration

	mu     sync.RWMutex
	worker daemon.PreparedController
}

func newControlEventSink(db *sql.DB) *controlEventSink {
	return &controlEventSink{db: db, timeout: controlEventTimeout}
}

func (s *controlEventSink) attach(worker daemon.PreparedController) {
	s.mu.Lock()
	s.worker = worker
	s.mu.Unlock()
}

func (s *controlEventSink) EmitControlEvents(
	ctx context.Context,
	events []channel.ControlEvent,
) error {
	s.mu.RLock()
	worker := s.worker
	s.mu.RUnlock()
	if worker == nil {
		return fmt.Errorf("control event worker is unavailable")
	}
	callCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	cancelAgents := make(map[protocol.AgentID]struct{})
	pauseAgents := make(map[protocol.AgentID]struct{})
	resumeAgents := make(map[protocol.AgentID]struct{})
	for _, event := range events {
		restriction, ok, err := restrictiveControlEvent(event)
		if err != nil {
			return err
		}
		if event.Payload.ScopeType == protocol.ScopeTypeAgent && restriction.pause == "" {
			resumeAgents[protocol.AgentID(event.Payload.ScopeKey)] = struct{}{}
		}
		if !ok {
			continue
		}
		agents, err := s.affectedPreparedAgents(callCtx, restriction)
		if err != nil {
			return err
		}
		for _, agent := range agents {
			cancelAgents[agent] = struct{}{}
		}
		if restriction.pause != "" {
			pauseAgents[restriction.pause] = struct{}{}
		}
	}

	var joined error
	for _, agent := range sortedAgents(cancelAgents) {
		if err := worker.ControlChanged(callCtx, agent); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	for _, agent := range sortedAgents(pauseAgents) {
		if err := worker.PauseAutomation(callCtx, agent); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	for _, agent := range sortedAgents(resumeAgents) {
		if err := worker.ResumeAutomation(callCtx, agent); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func restrictiveControlEvent(event channel.ControlEvent) (controlRestriction, bool, error) {
	payload := event.Payload
	var value controlEventValue
	if err := json.Unmarshal(payload.NewValue, &value); err != nil {
		return controlRestriction{}, false, fmt.Errorf("decode committed control value: %w", err)
	}
	if value.Deleted {
		return controlRestriction{}, false, nil
	}
	restriction := controlRestriction{payload: payload}
	switch payload.ScopeType {
	case protocol.ScopeTypeGlobal, protocol.ScopeTypeNode:
		if value.Mode == "" {
			return controlRestriction{}, false, fmt.Errorf("committed channel mode is missing")
		}
		return restriction, value.Mode != protocol.ControlModeActive, nil
	case protocol.ScopeTypeAgent:
		restriction.recipient = isFalse(value.RX) || isFalse(value.AutoDelivery)
		restriction.sender = isFalse(value.TX) || isFalse(value.AutoContinue)
		if isFalse(value.AutoDelivery) {
			restriction.pause = protocol.AgentID(payload.ScopeKey)
		}
		return restriction, restriction.recipient || restriction.sender, nil
	case protocol.ScopeTypeLink:
		if payload.Plane != protocol.PlaneMessage {
			return controlRestriction{}, false, nil
		}
		if value.Mode == "" {
			return controlRestriction{}, false, fmt.Errorf("committed link mode is missing")
		}
		return restriction, value.Mode != protocol.ControlModeAllow, nil
	case protocol.ScopeTypeThread:
		if value.Mode == "" {
			return controlRestriction{}, false, fmt.Errorf("committed thread mode is missing")
		}
		return restriction, value.Mode != protocol.ControlModeAllow, nil
	case protocol.ScopeTypeLoop:
		return restriction,
			value.Mode == protocol.ControlModePaused || value.Mode == protocol.ControlModeStopped,
			nil
	default:
		return controlRestriction{}, false, nil
	}
}

func isFalse(value *bool) bool { return value != nil && !*value }

func (s *controlEventSink) affectedPreparedAgents(
	ctx context.Context,
	restriction controlRestriction,
) ([]protocol.AgentID, error) {
	query := `SELECT DISTINCT da.agent_id
		FROM delivery_attempts AS da
		JOIN delivery_queue AS dq
		  ON dq.message_id = da.message_id AND dq.agent_id = da.agent_id
		 AND dq.delivery_generation = da.delivery_generation
		JOIN messages AS m ON m.message_id = da.message_id
		WHERE da.state = 'PREPARED'`
	var args []any
	payload := restriction.payload
	switch payload.ScopeType {
	case protocol.ScopeTypeGlobal:
	case protocol.ScopeTypeNode:
		query += " AND (m.origin_node = ? OR m.destination_node = ?)"
		args = append(args, payload.ScopeKey, payload.ScopeKey)
	case protocol.ScopeTypeAgent:
		switch {
		case restriction.recipient && restriction.sender:
			query += " AND (da.agent_id = ? OR dq.from_agent = ?)"
			args = append(args, payload.ScopeKey, payload.ScopeKey)
		case restriction.recipient:
			query += " AND da.agent_id = ?"
			args = append(args, payload.ScopeKey)
		case restriction.sender:
			query += " AND dq.from_agent = ?"
			args = append(args, payload.ScopeKey)
		default:
			return nil, nil
		}
	case protocol.ScopeTypeLink:
		query += " AND dq.from_agent = ? AND da.agent_id = ?"
		args = append(args, payload.FromAgent, payload.ToAgent)
	case protocol.ScopeTypeThread:
		query += " AND m.thread_id = ?"
		args = append(args, payload.ScopeKey)
	case protocol.ScopeTypeLoop:
		query += " AND m.loop_id = ?"
		args = append(args, payload.ScopeKey)
	default:
		return nil, nil
	}
	query += " ORDER BY da.agent_id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select prepared agents after control change: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var agents []protocol.AgentID
	for rows.Next() {
		var agent protocol.AgentID
		if err := rows.Scan(&agent); err != nil {
			return nil, fmt.Errorf("scan prepared agent after control change: %w", err)
		}
		if !agent.Valid() {
			return nil, fmt.Errorf("invalid prepared agent after control change")
		}
		agents = append(agents, agent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read prepared agents after control change: %w", err)
	}
	return agents, nil
}

func sortedAgents(set map[protocol.AgentID]struct{}) []protocol.AgentID {
	agents := make([]protocol.AgentID, 0, len(set))
	for agent := range set {
		agents = append(agents, agent)
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i] < agents[j] })
	return agents
}
