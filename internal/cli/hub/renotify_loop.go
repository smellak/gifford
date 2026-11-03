package hub

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// queuedRenoticeItem loads one queued delivery as a backup-loop item.
// Guards, detector state, read state and escalation marks arrive from
// the worker wiring; until then every fact stays fail-closed, so the
// loop evaluates without ever notifying. Escalation effects bind when
// the operator escalation package lands: until then the loop reports
// the pending escalation instead of acting on it.
func queuedRenoticeItem(
	messageID protocol.ULID,
	agentID protocol.AgentID,
	pendingSince string,
) (renotify.Item, error) {
	since, err := eventstore.ParseTime(pendingSince)
	if err != nil {
		return renotify.Item{}, fmt.Errorf("renotify: parse pending time: %w", err)
	}
	return renotify.Item{
		MessageID: messageID,
		AgentID:   agentID,
		Facts: fallback.Facts{
			PendingSince: since,
		},
	}, nil
}

// loadQueuedRenotices lists queued deliveries for one backup turn.
func loadQueuedRenotices(ctx context.Context, db *sql.DB) ([]renotify.Item, error) {
	rows, err := db.QueryContext(ctx, `SELECT message_id, agent_id, created_at
		FROM delivery_queue WHERE state = 'QUEUED' ORDER BY created_at, message_id`)
	if err != nil {
		return nil, fmt.Errorf("renotify: list queued: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var items []renotify.Item
	for rows.Next() {
		var messageID, agentID, createdAt string
		if err := rows.Scan(&messageID, &agentID, &createdAt); err != nil {
			return nil, fmt.Errorf("renotify: scan queued: %w", err)
		}
		item, err := queuedRenoticeItem(
			protocol.ULID(messageID), protocol.AgentID(agentID), createdAt)
		if err != nil {
			return nil, err
		}
		if !item.MessageID.Valid() || !item.AgentID.Valid() {
			continue
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("renotify: read queued: %w", err)
	}
	return items, nil
}

type ownedRenotifyLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startRenotifyLoop runs the backup loop in the node until stop. The
// runner carries the loop; facts and effects bind as their packages
// land, always fail-closed until then.
func startRenotifyLoop(
	parent context.Context,
	errW io.Writer,
	runner renotify.Turner,
	loader renotify.Loader,
	clk agentlink.Clock,
) *ownedRenotifyLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedRenotifyLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		(&renotify.Driver{Turner: runner, Loader: loader, Clock: clk, ErrW: errW}).Run(ctx)
	}()
	return loop
}

func (l *ownedRenotifyLoop) stop() {
	l.cancel()
	<-l.done
}
