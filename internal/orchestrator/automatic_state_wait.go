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

type automaticStateWaitEntry struct {
	started time.Time
	order   uint64
}

// holdForAutomaticState returns waiting when this pass was consumed by the
// analysis window, and cleanup when the elapsed-window normal path must drop
// a resolved identity afterwards.
func (e *Executor) holdForAutomaticState(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) (waiting bool, cleanup bool, err error) {
	realCandidate, needsAnalysis := e.automaticStatePrecheck(ctx, candidate)
	switch {
	case !realCandidate || !needsAnalysis:
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
// candidate; its second says only fresh analysis can move that candidate on.
// It cannot authorize a delivery: every control, guard and state fact is read
// again by tx A, tx C and T5 before an injection can commit.
func (e *Executor) automaticStatePrecheck(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) (bool, bool) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, false
	}
	defer rollback(tx)
	now := e.clock.Now().UTC()
	snapshot, linked := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil || !record.expiresAt.After(now) {
		return false, false
	}
	if !linked || snapshot.State != agentlink.StateUp || !snapshot.HasReport ||
		snapshot.SessionID != record.sessionID || snapshot.LastReport.AgentID != record.AgentID ||
		snapshot.LastReport.SessionID != record.sessionID ||
		record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModePTYProxy || !record.deliverable {
		return false, false
	}
	var pendingApproval bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM approval_requests
		WHERE object_type = 'message' AND object_id = ? AND agent_id = ? AND state = 'PENDING'
	)`, candidate.MessageID, candidate.AgentID).Scan(&pendingApproval); err != nil || pendingApproval {
		return false, false
	}
	uncertain, err := e.attempts.HasUncertain(ctx, tx, candidate.AgentID)
	if err != nil || uncertain {
		return false, false
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(
		ctx, tx, candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration,
	)
	if err != nil || committed {
		return false, false
	}
	allowed, err := e.channel.PreviewQueuedDelivery(
		ctx, tx, candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration,
	)
	if err != nil || !allowed {
		return false, false
	}
	if snapshot.LastReport.DraftState != protocol.DraftStateClosed ||
		snapshot.LastReport.ActivityState == protocol.ActivityStateActive ||
		!e.automaticProcessReady(ctx, tx, record) {
		return false, false
	}
	state, err := e.states.ReadLatestForAutomatic(
		ctx, tx, candidate.AgentID, record.sessionID, now, e.stateProfile,
	)
	if errors.Is(err, ErrStateObservationNotFound) {
		return true, true
	}
	if err != nil {
		return true, false
	}
	return true, automaticStateReasonNeedsAnalysis(state.EligibilityReason)
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
	if found {
		ok, _, _ := validStoredBinding(binding, record, tree, parent)
		return ok && len(tree.Descendants) == 1 && tree.Descendants[0] == binding.child
	}
	return len(tree.DirectChildren) == 1 && len(tree.Descendants) == 1 &&
		tree.DirectChildren[0] == tree.Descendants[0]
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
	e.stateWaitMu.Lock()
	defer e.stateWaitMu.Unlock()
	if entry, ok := e.stateWait[candidate]; ok {
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
	e.stateWait[candidate] = automaticStateWaitEntry{started: now, order: e.stateWaitOrder}
	return true
}

// cleanupAutomaticStateWait drops resolved identities. An unresolved QUEUED
// row keeps its original mark across state observations and reconnects.
func (e *Executor) cleanupAutomaticStateWait(
	ctx context.Context,
	candidate daemon.QueueDelivery,
) {
	e.stateWaitMu.Lock()
	_, tracked := e.stateWait[candidate]
	e.stateWaitMu.Unlock()
	if !tracked {
		return
	}
	realCandidate, needsAnalysis := e.automaticStatePrecheck(ctx, candidate)
	if realCandidate && needsAnalysis {
		return
	}
	e.forgetAutomaticStateWait(candidate)
}

func (e *Executor) forgetAutomaticStateWait(candidate daemon.QueueDelivery) {
	e.stateWaitMu.Lock()
	delete(e.stateWait, candidate)
	e.stateWaitMu.Unlock()
}
