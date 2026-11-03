package renotify

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

// ChannelReader supplies one agent's channel view. *channel.Controller
// satisfies it; tests substitute a script.
type ChannelReader interface {
	Agent(ctx context.Context, agent protocol.AgentID) (protocol.AgentChannel, error)
}

// Loader reads pending work from durable state. Every fact comes from a
// durable row: nothing is probed live here. Anything unreadable fails
// closed to Hold, so a missing fact never notifies and never escalates,
// and the turn stays quiet. GuardsPassed means the durable gates pass;
// the worker re-evaluates the live guards when the renotice it triggers
// reaches the attempt, so a wrong guess costs a queued row, never an
// unguarded notice. Pane stays provisional for the same reason: the
// worker confirms the live pane at attempt time.
type Loader struct {
	DB       *sql.DB
	Channels ChannelReader
	// Clock stamps the expiry check. Nil means the system clock.
	Clock clock.Clock
	// Logger records skipped messages. Nil means the default logger.
	Logger *slog.Logger
}

// now reports the time the loader reasons about.
func (l *Loader) now() time.Time {
	if l.Clock == nil {
		return clock.System{}.Now()
	}
	return l.Clock.Now()
}

// log reports skipped messages.
func (l *Loader) log() *slog.Logger {
	if l.Logger == nil {
		return slog.Default()
	}
	return l.Logger
}

// Items returns one item per message and recipient whose latest delivery
// row is queued or delivered and whose message is still open. Rows the
// worker actively owns (claimed, delivering, waiting retry) and terminal
// dead rows (failed, expired, cancelled) are left alone, as are read,
// acknowledged or finished messages.
func (l *Loader) Items(ctx context.Context) ([]Item, error) {
	if l.DB == nil {
		return nil, fmt.Errorf("renotify: nil database")
	}
	rows, err := l.DB.QueryContext(ctx, `SELECT dq.message_id, dq.agent_id, dq.control_state,
		first.pending_since, m.expires_at, m.state
		FROM delivery_queue dq
		JOIN (SELECT message_id, agent_id, MIN(created_at) AS pending_since
			FROM delivery_queue GROUP BY message_id, agent_id) first
		ON first.message_id = dq.message_id AND first.agent_id = dq.agent_id
		JOIN messages m ON m.message_id = dq.message_id
		WHERE dq.delivery_generation = (
			SELECT MAX(delivery_generation) FROM delivery_queue
			WHERE message_id = dq.message_id AND agent_id = dq.agent_id)
		AND dq.state IN ('QUEUED', 'DELIVERED')`)
	if err != nil {
		return nil, fmt.Errorf("renotify: list pending deliveries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.messageID, &c.agentID, &c.control, &c.pending, &c.expires, &c.msgState); err != nil {
			return nil, fmt.Errorf("renotify: scan pending delivery: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("renotify: read pending deliveries: %w", err)
	}
	var items []Item
	for _, c := range candidates {
		switch c.msgState {
		case "ACKNOWLEDGED", "REPLIED", "FAILED", "EXPIRED", "CANCELLED":
			continue
		}
		if expires, err := eventstore.ParseTime(c.expires); err != nil || !l.now().Before(expires) {
			continue
		}
		item, ok, err := l.item(ctx, c)
		if err != nil {
			// One unreadable message never stops the rest of the
			// turn: record it with its ids and skip it alone.
			l.log().WarnContext(ctx, "Renotify skipped an unreadable message",
				"message_id", c.messageID, "agent_id", c.agentID, "error", err)
			continue
		}
		if ok {
			items = append(items, item)
		}
	}
	return items, nil
}

type candidate struct {
	messageID string
	agentID   string
	control   string
	pending   string
	expires   string
	msgState  string
}

func (l *Loader) item(ctx context.Context, c candidate) (Item, bool, error) {
	messageID, agentID, control, pending := c.messageID, c.agentID, c.control, c.pending
	msgID := protocol.ULID(messageID)
	agent := protocol.AgentID(agentID)
	if !msgID.Valid() || !agent.Valid() {
		return Item{}, false, nil
	}
	pendingSince, err := eventstore.ParseTime(pending)
	if err != nil {
		return Item{}, false, fmt.Errorf("renotify: parse pending time: %w", err)
	}
	facts, ok, err := l.facts(ctx, msgID, agent, control)
	if err != nil {
		return Item{}, false, err
	}
	if !ok {
		return Item{}, false, nil
	}
	return Item{MessageID: msgID, AgentID: agent, Facts: fallback.Facts{
		PendingSince: pendingSince,
		Read:         facts.read,
		Notices:      facts.notices,
		Uncertain:    facts.uncertain,
		Escalated:    facts.escalated,
		Controls:     facts.controls,
		GuardsPassed: facts.guards,
		Pane:         facts.pane,
	}}, true, nil
}

type loadedFacts struct {
	read      bool
	notices   []time.Time
	uncertain bool
	escalated bool
	controls  fallback.Controls
	guards    bool
	pane      fallback.PaneState
}

func (l *Loader) facts(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, control string) (loadedFacts, bool, error) {
	var facts loadedFacts
	if err := l.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM acks
		WHERE message_id = ? AND agent_id = ? AND status = 'ACKNOWLEDGED')`,
		string(messageID), string(agentID)).Scan(&facts.read); err != nil {
		return facts, false, fmt.Errorf("renotify: read acknowledgements: %w", err)
	}
	rows, err := l.DB.QueryContext(ctx, `SELECT created_at FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ?
		AND state IN ('COMMIT_SENT', 'COMMITTED', 'COMMIT_NOT_CONSUMED')
		ORDER BY created_at`, string(messageID), string(agentID))
	if err != nil {
		return facts, false, fmt.Errorf("renotify: list notices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var at string
		if err := rows.Scan(&at); err != nil {
			return facts, false, fmt.Errorf("renotify: scan notice: %w", err)
		}
		sent, err := eventstore.ParseTime(at)
		if err != nil {
			return facts, false, fmt.Errorf("renotify: parse notice time: %w", err)
		}
		facts.notices = append(facts.notices, sent)
	}
	if err := rows.Err(); err != nil {
		return facts, false, fmt.Errorf("renotify: read notices: %w", err)
	}
	if err := l.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ? AND state = 'UNCERTAIN'
		AND resolution_approval_id IS NULL)`,
		string(messageID), string(agentID)).Scan(&facts.uncertain); err != nil {
		return facts, false, fmt.Errorf("renotify: read uncertainty: %w", err)
	}
	controls, guards := l.gates(ctx, agentID, control)
	facts.controls, facts.guards = controls, guards
	facts.pane = fallback.PaneIdle
	escalated, err := escalate.Escalated(ctx, l.DB, messageID)
	if err != nil {
		// An unreadable escalation record suppresses escalation: assume
		// it already happened so the turn holds instead of admitting a
		// notice the mark could never deduplicate.
		controls.Hold = true
		facts.controls = controls
		facts.guards = false
		facts.escalated = true
		return facts, true, nil
	}
	facts.escalated = escalated
	return facts, true, nil
}

// gates maps the durable gates onto controls. Unknown or unreadable
// inputs fail closed to Hold, which drops escalation as well.
func (l *Loader) gates(ctx context.Context, agentID protocol.AgentID, control string) (fallback.Controls, bool) {
	var controls fallback.Controls
	switch control {
	case "ALLOWED":
	case "HELD":
		controls.Hold = true
	case "APPROVAL_REQUIRED":
		controls.ApprovalPending = true
	case "DENIED":
		controls.Rejected = true
	default:
		controls.Hold = true
	}
	if l.Channels == nil {
		controls.Hold = true
		return controls, false
	}
	view, err := l.Channels.Agent(ctx, agentID)
	if err != nil {
		controls.Hold = true
		return controls, false
	}
	agentControls := escalate.ControlsOf(view)
	controls.Off = controls.Off || agentControls.Off
	controls.Hold = controls.Hold || agentControls.Hold
	controls.Manual = controls.Manual || agentControls.Manual
	if control != "ALLOWED" || !sessionDeliverable(ctx, l.DB, agentID) {
		controls.Hold = true
		return controls, false
	}
	return controls, true
}

func sessionDeliverable(ctx context.Context, db *sql.DB, agentID protocol.AgentID) bool {
	var ok bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions
		WHERE agent_id = ? AND state = 'CONNECTED' AND deliverable = 1
		AND ended_at IS NULL AND capture_mode IN ('pty_proxy', 'tmux_attach'))`,
		string(agentID)).Scan(&ok); err != nil || !ok {
		return false
	}
	return true
}
