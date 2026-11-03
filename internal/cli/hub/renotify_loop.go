package hub

import (
	"context"
	"database/sql"
	"io"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// newRenotifyRunner binds the backup loop effects to the node services.
// The runner renotices through the normal delivery path and escalates
// to the configured operator, the same owner incident.list reports.
// Without an operator the escalator stays unbound and the runner keeps
// the renotices while skipping escalation quietly, so no notice ever
// goes to an empty operator. nodeID is the hub's own id, the same
// origin the service stamps on every local admission: the loop must
// read the prior notice back under it, never under whatever the nodes
// table happens to list first.
func newRenotifyRunner(
	db *sql.DB,
	operator protocol.AgentID,
	engine renotify.DeliveryInitializer,
	service renotify.MessageAdmitter,
	clk clock.Clock,
	nodeID protocol.NodeID,
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
		AdmitOperator: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
			return renotify.AdmitOperatorNotice(ctx, service, db, clk, nodeID, agentID, operator, messageID)
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
