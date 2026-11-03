package renotify

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

// Item is one pending message for one turn of the loop. Facts arrive
// loaded by the caller: schedule, guards and detector state are read
// inputs, never probed here.
type Item struct {
	MessageID protocol.ULID
	AgentID   protocol.AgentID
	Facts     fallback.Facts
}

// Runner carries out one turn of the backup loop. Decide defaults to
// fallback.Decide. The three effects run inside one transaction per
// acted item: first the renotice when the slot is due, then the
// operator admission plus the durable escalation mark when the
// deadline passed. AdmitOperator must key the admission so a retry is
// idempotent; MarkEscalated must be a no-op when already marked.
// Operator names the configured operator agent the admission goes to.
type Runner struct {
	DB            *sql.DB
	Decide        func(fallback.Facts) fallback.Decision
	Operator      protocol.AgentID
	Notify        func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error
	AdmitOperator func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error
	MarkEscalated func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error
}

func (r *Runner) decide(f fallback.Facts) fallback.Decision {
	if r.Decide != nil {
		return r.Decide(f)
	}
	return fallback.Decide(f)
}

// RunOnce evaluates every item at now and carries out the Decisions.
// It returns the earliest Next across items, or the zero time when no
// item asks to be looked at again. A dropped attempt ends there:
// nothing is triggered and nothing escalates for it.
func (r *Runner) RunOnce(ctx context.Context, now time.Time, items []Item) (time.Time, error) {
	if r.DB == nil {
		return time.Time{}, fmt.Errorf("renotify: nil database")
	}
	var wake time.Time
	for _, item := range items {
		facts := item.Facts
		facts.Now = now
		d := r.decide(facts)
		if !d.Next.IsZero() && (wake.IsZero() || d.Next.Before(wake)) {
			wake = d.Next
		}
		if d.Action == fallback.ActionDrop {
			continue
		}
		if d.Action != fallback.ActionNotify && !d.Escalate {
			continue
		}
		tx, err := r.DB.BeginTx(ctx, nil)
		if err != nil {
			return wake, fmt.Errorf("renotify: begin turn: %w", err)
		}
		err = r.act(ctx, tx, item, d)
		if err != nil {
			_ = tx.Rollback()
			return wake, err
		}
		if err := tx.Commit(); err != nil {
			return wake, fmt.Errorf("renotify: commit turn: %w", err)
		}
	}
	return wake, nil
}

func (r *Runner) act(ctx context.Context, tx *sql.Tx, item Item, d fallback.Decision) error {
	if d.Action == fallback.ActionNotify {
		if r.Notify == nil {
			return fmt.Errorf("renotify: nil notify func")
		}
		if err := r.Notify(ctx, tx, item.MessageID, item.AgentID); err != nil {
			return fmt.Errorf("renotify: trigger renotice: %w", err)
		}
	}
	if d.Escalate && r.canEscalate() {
		if err := r.AdmitOperator(ctx, tx, item.MessageID, item.AgentID, r.Operator); err != nil {
			return fmt.Errorf("renotify: admit operator notice: %w", err)
		}
		if err := r.MarkEscalated(ctx, tx, item.MessageID, item.AgentID); err != nil {
			return fmt.Errorf("renotify: mark escalation: %w", err)
		}
	}
	return nil
}

// canEscalate reports whether the escalation path is bound. A missing
// operator or unbound effect is a missing fact: the turn skips quietly
// instead of erroring every round.
func (r *Runner) canEscalate() bool {
	return r.AdmitOperator != nil && r.MarkEscalated != nil && r.Operator.Valid()
}
