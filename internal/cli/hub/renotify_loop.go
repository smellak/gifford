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

// operatorNoticeFiler files one escalation notice: FileOperatorNotice to
// the first operator, FileOperatorNoticeCopy to each one after it. The
// production hub service implements it; tests record or stub it.
type operatorNoticeFiler interface {
	FileOperatorNotice(ctx context.Context, sender protocol.AgentID, operator protocol.AgentID, original protocol.ULID, pendingSince time.Time) error
	FileOperatorNoticeCopy(ctx context.Context, sender protocol.AgentID, operator protocol.AgentID, original protocol.ULID, pendingSince time.Time) error
}

// newRenotifyRunner binds the backup loop effects to the node services.
// The runner renotices through the normal delivery path and escalates
// to every configured operator once the message has waited
// escalateAfter (zero keeps the default). The first operator is the
// owner incident.list reports and the one the escalation mark names.
// The notice files as a normal message signed by the node's system
// sender, so every reader serves it. The service reads a prior notice
// back under its own node id, the same origin it stamps on every local
// admission, never under whatever the nodes table lists first. Without
// an operator the escalator stays unbound and the runner keeps the
// renotices while skipping escalation quietly, so no notice ever goes
// to an empty operator.
func newRenotifyRunner(
	db *sql.DB,
	operators []protocol.AgentID,
	escalateAfter time.Duration,
	engine renotify.DeliveryInitializer,
	service operatorNoticeFiler,
	clk clock.Clock,
) *renotify.Runner {
	if clk == nil {
		clk = clock.System{}
	}
	var markEscalated func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error
	if len(operators) > 0 {
		if escalator, err := escalate.New(operators[0], clk); err == nil {
			markEscalated = escalator.Escalate
		}
	}
	return &renotify.Runner{
		DB:            db,
		Operators:     operators,
		EscalateAfter: escalateAfter,
		Notify: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			return renotify.Requeue(ctx, tx, clk, engine, messageID, agentID)
		},
		FileOperatorNotice: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID, pendingSince time.Time) error {
			file := service.FileOperatorNoticeCopy
			if operator == operators[0] {
				file = service.FileOperatorNotice
			}
			err := file(ctx, agentID, operator, messageID, pendingSince)
			if errors.Is(err, hubsrv.ErrOperatorNoticeExpired) {
				return renotify.ErrOperatorNoticeExpired
			}
			if errors.Is(err, hubsrv.ErrOperatorNotDeliverable) {
				return fmt.Errorf("hub service: %w: %w", renotify.ErrOperatorNotDeliverable, err)
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

// firstOperator is the operator that owns the incident register, or none.
func firstOperator(operators []protocol.AgentID) protocol.AgentID {
	if len(operators) == 0 {
		return ""
	}
	return operators[0]
}
