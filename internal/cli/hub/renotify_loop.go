package hub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/escalate"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// newRenotifyRunner binds the backup loop effects to the node services.
// The runner renotices through the normal delivery path and escalates
// to the configured operator, the same owner incident.list reports.
// The notice files as a normal message signed by the node's system
// sender, so every reader serves it. Without an operator the escalator
// stays unbound and the runner keeps the renotices while skipping
// escalation quietly, so no notice ever goes to an empty operator.
// operatorNoticeFiler files one escalation notice. The production hub
// service implements it; tests record or stub it.
type operatorNoticeFiler interface {
	FileOperatorNotice(ctx context.Context, sender protocol.AgentID, operator protocol.AgentID, original protocol.ULID, pendingSince time.Time) error
}

func newRenotifyRunner(
	db *sql.DB,
	operator protocol.AgentID,
	engine renotify.DeliveryInitializer,
	service operatorNoticeFiler,
	clk clock.Clock,
) *renotify.Runner {
	if clk == nil {
		clk = clock.System{}
	}
	var markEscalated func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error
	if escalator, err := escalate.New(operator, clk); err == nil {
		markEscalated = escalator.Escalate
	}
	return &renotify.Runner{
		DB:       db,
		Operator: operator,
		Notify: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			return renotify.Requeue(ctx, tx, clk, engine, messageID, agentID)
		},
		FileOperatorNotice: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID, pendingSince time.Time) error {
			err := service.FileOperatorNotice(ctx, agentID, operator, messageID, pendingSince)
			if errors.Is(err, hubsrv.ErrOperatorNoticeExpired) {
				return renotify.ErrOperatorNoticeExpired
			}
			if err != nil {
				return fmt.Errorf("hub service: %w", err)
			}
			return nil
		},
		MarkEscalated: markEscalated,
	}
}

type ownedRenotifyLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startRenotifyLoop runs the backup loop in the node until stop. The
// runner carries the loop with real facts and effects; the operator
// binds when its config value arrives.
func startRenotifyLoop(
	parent context.Context,
	errW io.Writer,
	runner renotify.Turner,
	loader renotify.LoadFunc,
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
