package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	tmuxDeferInitial = time.Second
	tmuxDeferMaximum = 8 * time.Second
	tmuxDeferLimit   = 1024
)

// This is a no-write scheduling hint, never delivery authority. A fresh pane,
// keyboard and process check still runs under the writer lock on every pass.
// Only unchanged blocked work skips the full write-producing authorization.
type tmuxDeferState struct {
	candidate     daemon.QueueDelivery
	session       protocol.OpaqueID
	instance      protocol.ULID
	state         detect.State
	humanActive   bool
	humanCertain  bool
	process       procguard.Verdict
	baselineReady bool
	epoch         int64
	allowed       bool
	queueState    string
	expired       bool
}

type tmuxDeferred struct {
	state tmuxDeferState
	at    time.Time
	next  time.Time
	delay time.Duration
}

func (g *tmuxDeliveryGuard) skipUnchangedDefer(
	ctx context.Context,
	state detect.State,
	humanActive, humanCertain bool,
	process procguard.Verdict,
	baselineReady bool,
) (bool, error) {
	tx, err := g.executor.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("begin deferred tmux observation: %w", err)
	}
	defer rollback(tx)
	record, err := g.executor.loadDelivery(ctx, tx, g.candidate, g.instance.SessionID)
	if err != nil {
		return false, err
	}
	var epoch int64
	if err := tx.QueryRowContext(ctx, "SELECT epoch FROM channel_epoch WHERE id = 1").Scan(&epoch); err != nil {
		return false, fmt.Errorf("read deferred tmux control epoch: %w", err)
	}
	// TTL expiry and approvals can change effective permission without a
	// keyboard event; a fresh read-only preview prevents sleeping through it.
	allowed, err := g.executor.channel.PreviewQueuedDelivery(ctx, tx,
		g.candidate.MessageID, g.candidate.AgentID, g.candidate.DeliveryGeneration)
	if err != nil {
		return false, fmt.Errorf("preview deferred tmux controls: %w", err)
	}
	now := g.executor.clock.Now()
	g.deferState = tmuxDeferState{
		candidate: g.candidate, session: g.instance.SessionID, instance: g.instance.ID,
		state: state, humanActive: humanActive, humanCertain: humanCertain,
		process: process, baselineReady: baselineReady, epoch: epoch, allowed: allowed,
		queueState: record.queueState, expired: !record.expiresAt.After(now),
	}
	if g.deferState.expired || record.queueState != queueStateQueued {
		g.runtime.forgetDefer(g.candidate.AgentID)
		return false, nil
	}
	g.runtime.deferMu.Lock()
	defer g.runtime.deferMu.Unlock()
	previous, found := g.runtime.deferred[g.candidate.AgentID]
	return found && previous.state == g.deferState &&
		!now.Before(previous.at) && now.Before(previous.next), nil
}

func (r *tmuxAttachRuntime) rememberDefer(agent protocol.AgentID, state tmuxDeferState, now time.Time) {
	r.deferMu.Lock()
	defer r.deferMu.Unlock()
	delay := tmuxDeferInitial
	if previous, found := r.deferred[agent]; found && previous.state == state && !now.Before(previous.at) {
		delay = min(previous.delay*2, tmuxDeferMaximum)
	}
	if r.deferred == nil {
		r.deferred = make(map[protocol.AgentID]tmuxDeferred)
	}
	if _, found := r.deferred[agent]; !found && len(r.deferred) >= tmuxDeferLimit {
		// Losing scheduling hints only causes a fresh full evaluation. No
		// stale delivery permission or unbounded history is retained here.
		clear(r.deferred)
	}
	r.deferred[agent] = tmuxDeferred{state: state, at: now, next: now.Add(delay), delay: delay}
}

func (r *tmuxAttachRuntime) forgetDefer(agent protocol.AgentID) {
	r.deferMu.Lock()
	defer r.deferMu.Unlock()
	delete(r.deferred, agent)
}
