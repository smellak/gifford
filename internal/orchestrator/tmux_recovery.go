package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// TmuxPaneReconciler resolves this node's own unfinished write on a pane
// from fresh evidence, so the pane lock admits the next write. A pane whose
// record holds no such write is left unchanged and is not observed.
type TmuxPaneReconciler interface {
	ReconcileOwn(context.Context, tmuxinstance.Instance) error
}

// tmuxOwnReconcileFailedEvent names a pass whose own-write reconcile
// failed. The delivery itself still runs every pane-lock check.
const tmuxOwnReconcileFailedEvent = "tmux_own_reconcile_failed"

// tmuxOwnRecoveryReason settles the approval of an uncertain tmux delivery
// the node took over itself.
const tmuxOwnRecoveryReason = "node retried its own uncertain tmux delivery"

// reconcileOwn frees the pane lock from this node's own unfinished write
// before a pass. A failure is logged without its error text: the delivery
// that follows checks the lock again and stops on its own if it still
// blocks.
func (r *tmuxAttachRuntime) reconcileOwn(ctx context.Context, instance tmuxinstance.Instance) {
	if r == nil || r.reconciler == nil {
		return
	}
	if err := r.reconciler.ReconcileOwn(ctx, instance); err != nil && r.logger != nil {
		r.logger.LogAttrs(context.WithoutCancel(ctx), slog.LevelDebug,
			"Tmux own pane write could not be reconciled",
			slog.String("event", tmuxOwnReconcileFailedEvent))
	}
}

// tmuxRecoverable returns the uncertain attempt this delivery may retry on
// its own: the agent's only unresolved one, for this message, generation
// and session, still awaiting its pending orchestrator approval. Anything
// else stays with a person.
func (e *Executor) tmuxRecoverable(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
) (*daemon.Attempt, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin tmux recovery lookup: %w", err)
	}
	defer rollback(tx)
	prior, found, err := e.attempts.SoleUncertain(ctx, tx, candidate.AgentID)
	if err != nil {
		return nil, fmt.Errorf("find tmux uncertain attempt: %w", err)
	}
	if !found || !tmuxRecoveryMatches(prior, candidate, instance.SessionID) {
		return nil, nil
	}
	pending, err := e.tmuxRecoveryPending(ctx, tx, prior)
	if err != nil || !pending {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tmux recovery lookup: %w", err)
	}
	return &prior, nil
}

func tmuxRecoveryMatches(
	prior daemon.Attempt,
	candidate daemon.QueueDelivery,
	session protocol.OpaqueID,
) bool {
	return prior.MessageID == candidate.MessageID && prior.AgentID == candidate.AgentID &&
		prior.DeliveryGeneration == candidate.DeliveryGeneration &&
		prior.AdapterSessionID == session && prior.UncertainApprovalID.Valid() &&
		prior.ResolutionApprovalID == ""
}

// tmuxRecoveryPending reports whether the attempt's approval is still the
// pending, unexpired orchestrator approval for its message and agent.
func (e *Executor) tmuxRecoveryPending(
	ctx context.Context,
	tx *sql.Tx,
	prior daemon.Attempt,
) (bool, error) {
	request, err := e.approvals.ApprovalTx(ctx, tx, prior.UncertainApprovalID)
	if errors.Is(err, channel.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load tmux recovery approval: %w", err)
	}
	return request.State == protocol.ApprovalStatePending &&
		request.Source == protocol.ApprovalSourceOrchestrator &&
		request.ObjectType == protocol.ApprovalObjectTypeMessage &&
		request.ObjectID == string(prior.MessageID) && request.AgentID == prior.AgentID &&
		(request.ExpiresAt.IsZero() || request.ExpiresAt.After(e.clock.Now())), nil
}

// SubmitOnly reports whether Revalidate found exactly this delivery's
// notice already typed in the box. The transport then presses only Enter.
func (g *tmuxDeliveryGuard) SubmitOnly() bool {
	return g.submitOnly
}

// ownNoticeRule treats a box that holds exactly this delivery's notice as
// free for Enter only. Only the exact notice, read certain by the
// verifier, qualifies; any other text keeps the box busy. The agent half of
// the rule is untouched, so a busy or moving pane still holds the Enter.
func (g *tmuxDeliveryGuard) ownNoticeRule(
	state detect.State,
	rule tmuxScreenRule,
	screen []byte,
	observation tmuxattach.PaneObservation,
) tmuxScreenRule {
	g.submitOnly = false
	if !state.Certain || !state.BoxBusy || !g.holdsOwnNotice(screen, observation) {
		return rule
	}
	g.submitOnly = true
	g.detectorFallback = tmuxFallbackOwnSubmit
	rule.boxFree, rule.boxDetail = true, ""
	return rule
}

func (g *tmuxDeliveryGuard) holdsOwnNotice(
	screen []byte,
	observation tmuxattach.PaneObservation,
) bool {
	return tmuxHoldsNotice(g.runtime, g.instance, screen,
		observation.Cols, observation.Rows, g.notice)
}

// tmuxHoldsNotice reports whether the pane's box holds exactly notice and
// nothing else, as the pre-Enter verifier reads it.
func tmuxHoldsNotice(
	runtime *tmuxAttachRuntime,
	instance tmuxinstance.Instance,
	screen []byte,
	cols, rows int,
	notice protocol.NoticeLine,
) bool {
	harness, ok := tmuxDetectorHarness(instance.Harness)
	if !ok || cols <= 0 || rows <= 0 || notice == "" {
		return false
	}
	certain, exact := detect.VerifyNotice(harness, runtime.detectorTable, screen,
		cols, rows, string(notice))
	return certain && exact
}

// blockingUncertain reports an unresolved uncertain attempt that holds
// this write. A recovery is held only when its own attempt stopped being
// the agent's sole pending one: someone resolved it, another one appeared
// or its approval is no longer pending.
func (g *tmuxDeliveryGuard) blockingUncertain(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
) (bool, error) {
	if g.recovery == nil {
		return g.executor.attempts.HasUncertain(ctx, tx, record.AgentID)
	}
	prior, found, err := g.executor.attempts.SoleUncertain(ctx, tx, record.AgentID)
	if err != nil {
		return false, err
	}
	if !found || prior.DeliveryID != g.recovery.DeliveryID ||
		!tmuxRecoveryMatches(prior, g.candidate, g.instance.SessionID) {
		return true, nil
	}
	pending, err := g.executor.tmuxRecoveryPending(ctx, tx, prior)
	if err != nil || !pending {
		return true, err
	}
	g.recovery = &prior
	return false, nil
}

// takeOverTmuxUncertain resolves the uncertain attempt a recovery replaces
// and settles its approval as the node's own, in the transaction that
// creates the recovery. A recovery that ends uncertain raises its own
// approval in turn.
func (e *Executor) takeOverTmuxUncertain(ctx context.Context, tx *sql.Tx, prior daemon.Attempt) error {
	if err := e.attempts.ResolveUncertain(ctx, tx, prior.DeliveryID, prior.UncertainApprovalID); err != nil {
		return fmt.Errorf("resolve recovered tmux attempt: %w", err)
	}
	if _, err := e.approvals.SettleOrchestratorApproval(
		ctx, tx, prior.UncertainApprovalID, tmuxOwnRecoveryReason,
	); err != nil {
		return fmt.Errorf("settle recovered tmux approval: %w", err)
	}
	return nil
}
