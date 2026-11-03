package renotify

import (
	"context"
	"database/sql"
	"errors"
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
// fallback.Decide. The effects never share one transaction: the
// renotice commits first when the slot is due, then the operator
// admission runs in its own transaction, and finally the durable
// escalation mark runs in another. AdmitOperator must key the
// admission so a retry is idempotent; MarkEscalated must be a no-op
// when already marked. Operator names the configured operator agent
// the admission goes to.
type Runner struct {
	DB            *sql.DB
	Decide        func(fallback.Facts) fallback.Decision
	Operator      protocol.AgentID
	Notify        func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error
	AdmitOperator func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error
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
// nothing is triggered and nothing escalates for it. A failure in one
// item never stops the turn: it is recorded, the loop continues with
// the rest, and the returned error joins every item error while wake
// still covers all items.
func (r *Runner) RunOnce(ctx context.Context, now time.Time, items []Item) (time.Time, error) {
	if r.DB == nil {
		return time.Time{}, fmt.Errorf("renotify: nil database")
	}
	var wake time.Time
	var errs []error
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
		if d.Action == fallback.ActionNotify {
			if err := r.renotice(ctx, item); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if d.Escalate && r.canEscalate() {
			if err := r.escalate(ctx, item); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return wake, errors.Join(errs...)
}

// renotice files the next generation through the normal delivery path
// and commits it before any escalation runs.
func (r *Runner) renotice(ctx context.Context, item Item) error {
	if r.Notify == nil {
		return fmt.Errorf("renotify: nil notify func")
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("renotify: begin turn: %w", err)
	}
	if err := r.Notify(ctx, tx, item.MessageID, item.AgentID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("renotify: trigger renotice: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("renotify: commit turn: %w", err)
	}
	return nil
}

// escalate admits the operator notice in its own transaction and then
// marks the escalation durable in another one. The admission never
// runs inside an open write transaction of this process: the admit
// path opens its own immediate transaction, which would deadlock
// against the turn's. A failure between the two retries the admission
// with the same idempotency key, so the operator still gets exactly
// one notice.
func (r *Runner) escalate(ctx context.Context, item Item) error {
	if err := r.AdmitOperator(ctx, item.MessageID, item.AgentID, r.Operator); err != nil {
		return fmt.Errorf("renotify: admit operator notice: %w", err)
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("renotify: begin mark: %w", err)
	}
	if err := r.MarkEscalated(ctx, tx, item.MessageID, item.AgentID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("renotify: mark escalation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("renotify: commit mark: %w", err)
	}
	return nil
}

// canEscalate reports whether the escalation path is bound. A missing
// operator or unbound effect is a missing fact: the turn skips quietly
// instead of erroring every round.
func (r *Runner) canEscalate() bool {
	return r.AdmitOperator != nil && r.MarkEscalated != nil && r.Operator.Valid()
}
