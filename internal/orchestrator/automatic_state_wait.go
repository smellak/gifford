package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	automaticStateWaitWindow = 10 * time.Second
	automaticStateWaitLimit  = 1024
)

type automaticStateWaitOrigin uint8

const (
	automaticStateWaitPrecheck automaticStateWaitOrigin = iota
	automaticStateWaitAfterTriage
)

type automaticStateWaitEntry struct {
	started time.Time
	order   uint64
	origin  automaticStateWaitOrigin
}

// holdForAutomaticState returns waiting when current human input or the state
// analysis window consumed this pass, and cleanup when the elapsed-window
// normal path must drop a resolved identity afterwards.
func (e *Executor) holdForAutomaticState(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) (waiting bool, cleanup bool, err error) {
	realCandidate, humanBlocked, needsAnalysis := e.automaticStatePrecheck(ctx, candidate)
	switch {
	case humanBlocked:
		// Human input has no timeout window. Leave the row QUEUED for the
		// next worker scan and let expiry remain its only bound. Any older
		// analysis mark must not shorten a later, fresh state wait.
		e.forgetAutomaticStateWait(candidate)
		return true, false, nil
	case !realCandidate:
		e.forgetAutomaticStateWait(candidate)
		return false, false, nil
	case !needsAnalysis:
		// Keep an existing mark until the pass finishes. tx A may see that
		// this state expired during TRIAGE and promote the same mark instead
		// of opening a fresh window. cleanup drops an ordinary precheck mark
		// when the pass still has eligible state.
		if e.automaticStateWaitTracked(candidate) {
			return false, true, nil
		}
		e.forgetAutomaticStateWait(candidate)
		return false, false, nil
	case !e.withinAutomaticStateWindow(candidate, e.clock.Now()):
		return false, true, nil
	default:
		if err := e.states.Demand(candidate.AgentID); err != nil {
			return false, false, fmt.Errorf("demand automatic state: %w", err)
		}
		return true, false, nil
	}
}

// automaticStatePrecheck is a read-only precheck before TRIAGE and tx A. Its
// first result says the exact queue identity is still a real automatic
// candidate; its second says current human input blocks this pass; its third
// says only fresh analysis can move that candidate on. It cannot authorize a
// delivery: every control, guard and state fact is read again by tx A, tx C
// and T5 before an injection can commit.
func (e *Executor) automaticStatePrecheck(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) (realCandidate, humanBlocked, needsAnalysis bool) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, false, false
	}
	defer rollback(tx)
	now := e.clock.Now().UTC()
	snapshot, linked := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil || !record.expiresAt.After(now) {
		return false, false, false
	}
	if !linked || snapshot.State != agentlink.StateUp || !snapshot.HasReport ||
		snapshot.SessionID != record.sessionID || snapshot.LastReport.AgentID != record.AgentID ||
		snapshot.LastReport.SessionID != record.sessionID ||
		record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModePTYProxy || !record.deliverable {
		return false, false, false
	}
	var pendingApproval bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM approval_requests
		WHERE object_type = 'message' AND object_id = ? AND agent_id = ? AND state = 'PENDING'
	)`, candidate.MessageID, candidate.AgentID).Scan(&pendingApproval); err != nil || pendingApproval {
		return false, false, false
	}
	uncertain, err := e.attempts.HasUncertain(ctx, tx, candidate.AgentID)
	if err != nil || uncertain {
		return false, false, false
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(
		ctx, tx, candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration,
	)
	if err != nil || committed {
		return false, false, false
	}
	allowed, err := e.channel.PreviewQueuedDelivery(
		ctx, tx, candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration,
	)
	if err != nil || !allowed {
		return false, false, false
	}
	if !e.automaticProcessReady(ctx, tx, record) {
		return false, false, false
	}
	if automaticHumanInputBlocks(snapshot.LastReport) {
		return true, true, false
	}
	if snapshot.LastReport.DraftState != protocol.DraftStateClosed {
		// UNKNOWN remains the ordinary guard-DEFER path. Only the three
		// affirmative in-progress draft states are a transient human wait.
		return false, false, false
	}
	state, err := e.states.ReadLatestForAutomatic(
		ctx, tx, candidate.AgentID, record.sessionID, now, e.expectedStateProfile(),
	)
	if errors.Is(err, ErrStateObservationNotFound) {
		return true, false, true
	}
	if err != nil {
		return true, false, false
	}
	return true, false, automaticStateReasonNeedsAnalysis(state.EligibilityReason)
}

func automaticHumanInputBlocks(report protocol.InputGuardReport) bool {
	switch report.DraftState {
	case protocol.DraftStateOpen, protocol.DraftStatePasting, protocol.DraftStateSubmitting:
		return true
	default:
		return report.ActivityState == protocol.ActivityStateActive
	}
}

// automaticProcessReady is the no-write preview of the process guard. tx A
// still owns binding or permanently blocking the process identity.
func (e *Executor) automaticProcessReady(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
) bool {
	if !record.sessionID.Valid() {
		return false
	}
	binding, found, err := loadProcessBinding(ctx, tx, record.sessionID)
	if err != nil || (found && binding.state == processBindingStateBlocked) {
		return false
	}
	tree, err := e.processes.Inspect(record.process)
	if err != nil {
		return false
	}
	parent := ProcessToken{PID: record.process.PID, StartTime: record.process.ProcessStartTime}
	if tree.Root != parent {
		return false
	}
	tree = e.withoutRegisteredEndpoint(record, tree)
	auxiliaryName, err := e.allowedAuxiliaryExecutable(ctx, tx, record)
	if err != nil {
		return false
	}
	if found {
		ok, _, _ := validStoredProcessCore(binding, record, tree, parent)
		if !ok {
			return false
		}
		_, detail := validateProcessLayout(tree, &binding, auxiliaryName)
		return detail == ""
	}
	_, detail := validateProcessLayout(tree, nil, auxiliaryName)
	return detail == ""
}

func automaticStateReasonNeedsAnalysis(reason string) bool {
	switch reason {
	case StateEligibilityExpired,
		StateEligibilityStale,
		StateEligibilityObservationNewer,
		StateEligibilitySessionChanged,
		StateEligibilityGuardChanged,
		StateEligibilityUnknownPhase,
		StateEligibilityNoModelEvidence,
		StateEligibilityProfileChanged,
		StateEligibilityCallInvalid:
		return true
	case StateEligibilityEligible,
		StateEligibilityNotIdle,
		StateEligibilityBelowThreshold,
		StateEligibilityDeliveryUnsafe,
		StateEligibilityHumanInput,
		StateEligibilityPersistence,
		StateEligibilityUnhealthy,
		StateEligibilitySessionInactive,
		StateEligibilityNativeInformative:
		return false
	default:
		return false
	}
}

// withinAutomaticStateWindow remembers the first waitable selection of one
// delivery identity. Evicting the oldest entry at the bound can only grant a
// later fresh wait; it can never authorize a delivery.
func (e *Executor) withinAutomaticStateWindow(
	candidate daemon.QueueDelivery,
	now time.Time,
) bool {
	return e.withinAutomaticStateWindowFrom(candidate, now, automaticStateWaitPrecheck)
}

func (e *Executor) withinAutomaticStateWindowAfterTriage(
	candidate daemon.QueueDelivery,
	now time.Time,
) bool {
	return e.withinAutomaticStateWindowFrom(candidate, now, automaticStateWaitAfterTriage)
}

func (e *Executor) withinAutomaticStateWindowFrom(
	candidate daemon.QueueDelivery,
	now time.Time,
	origin automaticStateWaitOrigin,
) bool {
	e.stateWaitMu.Lock()
	defer e.stateWaitMu.Unlock()
	if entry, ok := e.stateWait[candidate]; ok {
		if origin == automaticStateWaitAfterTriage && entry.origin != origin {
			entry.origin = origin
			e.stateWait[candidate] = entry
		}
		return now.Sub(entry.started) < automaticStateWaitWindow
	}
	if e.stateWait == nil {
		e.stateWait = make(map[daemon.QueueDelivery]automaticStateWaitEntry)
	}
	if len(e.stateWait) >= automaticStateWaitLimit {
		var oldest daemon.QueueDelivery
		var oldestOrder uint64
		found := false
		for identity, entry := range e.stateWait {
			if !found || entry.order < oldestOrder {
				oldest, oldestOrder, found = identity, entry.order, true
			}
		}
		delete(e.stateWait, oldest)
	}
	e.stateWaitOrder++
	e.stateWait[candidate] = automaticStateWaitEntry{
		started: now,
		order:   e.stateWaitOrder,
		origin:  origin,
	}
	return true
}

func (e *Executor) automaticStateWaitTracked(candidate daemon.QueueDelivery) bool {
	e.stateWaitMu.Lock()
	defer e.stateWaitMu.Unlock()
	_, ok := e.stateWait[candidate]
	return ok
}

// cleanupAutomaticStateWait drops resolved identities. An unresolved QUEUED
// row keeps its original mark across state observations and reconnects.
func (e *Executor) cleanupAutomaticStateWait(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) {
	e.stateWaitMu.Lock()
	entry, tracked := e.stateWait[candidate]
	e.stateWaitMu.Unlock()
	if !tracked {
		return
	}
	realCandidate, humanBlocked, needsAnalysis := e.automaticStatePrecheck(ctx, candidate)
	if humanBlocked {
		e.forgetAutomaticStateWait(candidate)
		return
	}
	if realCandidate && (needsAnalysis || entry.origin == automaticStateWaitAfterTriage) {
		return
	}
	e.forgetAutomaticStateWait(candidate)
}

func (e *Executor) forgetAutomaticStateWait(candidate daemon.QueueDelivery) {
	e.stateWaitMu.Lock()
	delete(e.stateWait, candidate)
	e.stateWaitMu.Unlock()
}
