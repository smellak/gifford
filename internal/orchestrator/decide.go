package orchestrator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/triage"
)

// This file holds the automatic decision of 5.6–5.8. The deterministic
// rules decide what is allowed; the model only chooses between
// INJECT_PTY_PROMPT and DEFER when every rule and guard already allows
// injecting. No model call runs with a transaction open: tx A gates,
// guards and fixes the allowed actions, the call runs after its commit,
// and tx C revalidates everything tx A saw before it acts. The commit of
// an automatic delivery then needs the decision's state once more, after
// the prepare.

const (
	// triageTimeout bounds one triage, above the gateway's 5 s TRIAGE
	// deadline.
	triageTimeout = 10 * time.Second
	// chooseTimeout bounds one CHOOSE_ORCHESTRATION_ACTION call, above the
	// gateway's 3 s deadline.
	chooseTimeout = 5 * time.Second
	// staleCap is how many STALE decisions in a row one recipient
	// generation may reach before it is deferred without a model call.
	staleCap = 3
	// maxDecisionModelLen bounds decision_model (contract maxLength); the
	// value is ASCII, so bytes are characters.
	maxDecisionModelLen = 128
)

// Approval notes of an automatic DEFER. They are fixed text: no message
// body, screen or model reason ever reaches a note.
const (
	notePrefix        = "automatic delivery deferred: "
	noteChannel       = "channel decision is "
	noteUnclassified  = "message not classified"
	noteRequiresHuman = "message requires a human"
	noteModelFailed   = "decision model failed"
	noteModelDeferred = "decision model chose DEFER"
	noteStale         = "state kept changing during the decision"
	noteNoModel       = "no decision model"
)

// Inferrer runs one model contract. *gateway.Gateway satisfies it.
type Inferrer interface {
	Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error)
}

// Triager returns the triage verdict of one queued delivery, classifying
// its message first when it has none and could be delivered now.
// *triage.Triager satisfies it.
type Triager interface {
	Triage(ctx context.Context, d triage.Delivery) (triage.Verdict, bool, error)
}

// AutomaticStates reads the receiver state an automatic decision may use,
// through the decision's own transaction. *StateProducer satisfies it. The
// analyst route for tmux also submits through it: locked captures are the
// only observations a tmux session ever gets.
type AutomaticStates interface {
	Demand(agent protocol.AgentID) error
	Submit(report protocol.StateObservationReport) error
	// HasQueuedObservation reports whether an exact uncommitted observation
	// is still waiting for, or in, analysis.
	HasQueuedObservation(
		agent protocol.AgentID,
		session protocol.OpaqueID,
		observation protocol.OpaqueID,
	) bool
	ReadLatestForAutomatic(
		ctx context.Context,
		q stateQueryer,
		agent protocol.AgentID,
		session protocol.OpaqueID,
		now time.Time,
		expected *StateAnalysisProfile,
	) (StateRead, error)
}

// triageResult is what the triage gave for one delivery.
type triageResult struct {
	verdict triage.Verdict
	// found is false when the message has no verdict: the triage found
	// the delivery not deliverable now and called nothing.
	found bool
	// failed means the triage itself failed.
	failed bool
}

// automaticView is what one transaction sees of a queued delivery.
type automaticView struct {
	now      time.Time
	snapshot agentlink.Snapshot
	record   deliveryRecord
	guards   []protocol.GuardResult
	stateID  protocol.ULID
	// state is set only when the automatic decision is wired.
	state    StateRead
	manifest string
	// lifecycle is the session's sessions.updated_at, the revision the
	// link registry checks.
	lifecycle string
	queuedAt  time.Time
	committed bool
	staleRun  int
	// detectorOK and detector are DeterministicPTYPolicy's verdict and the
	// screen it read, set only when the automatic decision is not wired.
	detectorOK     bool
	detectorReason string
	detector       detectorReference
	// corroborable and corroborated are set when the automatic decision
	// is wired: the analyst state misses only its threshold, and the
	// detector corroborates it.
	corroborable bool
	corroborated bool
}

// decisionVector is what tx A saw and tx C must see again. The
// generation is the candidate's own, the control vector is checked by
// channel.Revalidate, and the guards are evaluated again besides.
type decisionVector struct {
	sessionID       protocol.OpaqueID
	lifecycle       string
	guardVersion    int64
	draftGeneration int64
	state           automaticStateReference
	manifestSHA256  string
	verdict         triage.Verdict
	detector        detectorReference
}

// allowedRule is the result of the allowed-actions rules.
type allowedRule struct {
	actions []protocol.OrchestratorAction
	// note is the approval note of a single DEFER.
	note string
	// gateOnly keeps the gate's decision and nothing else: the gate let
	// pass a delivery the triage did not find deliverable, so the next
	// pass classifies it.
	gateOnly bool
}

// automaticStateReference identifies the inference behind a state row. PTY
// refreshes may commit a new row for unchanged evidence, so their shared
// Glimmer call and evidence generations are the stable identity. Rows without
// model evidence retain the exact row identity used before this distinction.
type automaticStateReference struct {
	stateID             protocol.ULID
	stateVersion        int64
	glimmerCallID       protocol.OpaqueID
	expiresAt           time.Time
	outputGeneration    int64
	eventGeneration     int64
	inferenceProfile    protocol.InferenceProfile
	selectionGeneration int64
}

func automaticStateReferenceOf(state StateRead) automaticStateReference {
	return automaticStateReference{
		stateID:             state.Snapshot.StateID,
		stateVersion:        state.Snapshot.StateVersion,
		glimmerCallID:       state.GlimmerCallID,
		expiresAt:           state.ExpiresAt,
		outputGeneration:    state.OutputGeneration,
		eventGeneration:     state.EventGeneration,
		inferenceProfile:    state.InferenceProfile,
		selectionGeneration: state.SelectionGeneration,
	}
}

// sameInference is the one state-equivalence rule used both after CHOOSE and
// after PREPARE. Expiry remains anchored to the observation the model saw.
func (r automaticStateReference) sameInference(other automaticStateReference) bool {
	if r.glimmerCallID == "" || other.glimmerCallID == "" {
		return r.glimmerCallID == "" && other.glimmerCallID == "" &&
			r.stateID == other.stateID && r.stateVersion == other.stateVersion
	}
	return r.glimmerCallID == other.glimmerCallID &&
		r.expiresAt.Equal(other.expiresAt) &&
		r.outputGeneration == other.outputGeneration &&
		r.eventGeneration == other.eventGeneration &&
		r.inferenceProfile == other.inferenceProfile &&
		r.selectionGeneration == other.selectionGeneration
}

// automaticExpectation is the inference an automatic decision used. Its
// commit goes ahead only while the latest state is eligible and represents
// that same inference.
type automaticExpectation struct {
	state automaticStateReference
	// deterministic marks a DeterministicPTYPolicy decision: its commit
	// checks the detector on the same screen and manifest instead.
	// corroborated marks a CorroboratedPTYPolicy one: its commit checks
	// both the same inference and the detector.
	deterministic bool
	corroborated  bool
	detector      detectorReference
	manifest      string
}

// pendingChoice is a decision tx A left to the model.
type pendingChoice struct {
	candidate daemon.QueueDelivery
	decided   channel.Decision
	vector    decisionVector
	allowed   []protocol.OrchestratorAction
	guards    []protocol.GuardResult
	stateID   protocol.ULID
	request   protocol.InferRequest
	requestOK bool
	// deterministic is a DeterministicPTYPolicy choice: INJECT without a
	// model, carried out only if tx C finds the same quiet screen.
	// corroborated is a model choice under CorroboratedPTYPolicy.
	deterministic bool
	corroborated  bool
}

// modelChoice is a CHOOSE answer that passed every check.
type modelChoice struct {
	action        protocol.OrchestratorAction
	probabilities map[protocol.OrchestratorAction]float64
	callID        protocol.OpaqueID
	model         string
}

// choiceCallOutcome distinguishes an answer the model chose from a call
// failure the worker may retry. An invalid local request never reached
// Glimmer, and an open circuit must stay with the human.
type choiceCallOutcome uint8

const (
	choiceCallUnsent choiceCallOutcome = iota
	choiceCallValid
	choiceCallFailed
	choiceCallCircuitOpen
)

// decisionRow is one orchestrator_decisions row.
type decisionRow struct {
	record        deliveryRecord
	stateID       protocol.ULID
	allowed       []protocol.OrchestratorAction
	selected      protocol.OrchestratorAction
	probabilities map[protocol.OrchestratorAction]float64
	callID        protocol.OpaqueID
	model         string
	guards        []protocol.GuardResult
	channel       protocol.ChannelDecisionID
	status        protocol.ExecutionStatus
	// policy overrides the executor's policy version when set.
	policy string
}

// choosing reports whether the automatic decision of 5.6–5.8 is wired.
// Without it no automatic evaluation delivers, as in 5.3c.
func (e *Executor) choosing() bool {
	return e.model != nil && e.triager != nil && e.states != nil &&
		(e.stateProfile != nil || e.selection != nil)
}

func (e *Executor) expectedStateProfile() *StateAnalysisProfile {
	if e.selection == nil {
		return e.stateProfile
	}
	selected := e.selection.CurrentModelSelection()
	if selected.Validate() != nil ||
		selected.AnalysisPolicyVersion != StateAnalysisPolicyVersion {
		return nil
	}
	return &StateAnalysisProfile{
		SelectionGeneration: selected.Generation,
		Revision:            selected.ProfileRevision, SHA256: selected.ProfileSHA256,
		CalibrationRevision: selected.CalibrationRevision,
		CalibrationSHA256:   selected.CalibrationSHA256,
		Temperature:         selected.CalibrationTemperature, IdleThreshold: selected.IdleThreshold,
	}
}

// triageFor triages candidate before any transaction opens. A delivery
// with durable COMMITTED evidence is a duplicate whatever its triage, so
// it is never sent to the model; neither is anything when that check
// fails.
func (e *Executor) triageFor(ctx context.Context, candidate daemon.QueueDelivery) triageResult {
	if !e.choosing() {
		return triageResult{}
	}
	committed, err := e.committedBeforeTriage(ctx, candidate)
	if err != nil {
		return triageResult{failed: true}
	}
	if committed {
		return triageResult{}
	}
	callCtx, cancel := context.WithTimeout(ctx, triageTimeout)
	defer cancel()
	verdict, found, err := e.triager.Triage(callCtx, triage.Delivery{
		MessageID: candidate.MessageID, AgentID: candidate.AgentID,
		DeliveryGeneration: candidate.DeliveryGeneration,
	})
	if err != nil {
		return triageResult{failed: true}
	}
	return triageResult{verdict: verdict, found: found}
}

// committedBeforeTriage reads, in a read-only transaction closed before
// any call, whether candidate has durable COMMITTED evidence.
func (e *Executor) committedBeforeTriage(ctx context.Context, candidate daemon.QueueDelivery) (bool, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("begin duplicate check: %w", err)
	}
	defer rollback(tx)
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	if err != nil {
		return false, fmt.Errorf("check committed delivery evidence: %w", err)
	}
	return committed, nil
}

// committedAttempt is an attempt of record's exact identity with durable
// COMMITTED evidence: the evidence not_duplicate reads.
func committedAttempt(ctx context.Context, tx *sql.Tx, record deliveryRecord) (protocol.OpaqueID, error) {
	var deliveryID protocol.OpaqueID
	err := tx.QueryRowContext(ctx, `SELECT attempt.delivery_id
		FROM delivery_attempts AS attempt
		LEFT JOIN sessions AS session ON session.session_id = attempt.adapter_session_id
		WHERE attempt.message_id = ? AND attempt.agent_id = ?
			AND attempt.delivery_generation = ? AND (
				EXISTS (SELECT 1 FROM delivery_result_evidence AS evidence
					WHERE evidence.delivery_id = attempt.delivery_id
						AND evidence.result = 'COMMITTED')
				OR (attempt.state = 'COMMITTED' AND session.capture_mode = ?)
			)
		ORDER BY attempt.created_at, attempt.delivery_id LIMIT 1`,
		record.MessageID, record.AgentID, record.DeliveryGeneration,
		protocol.CaptureModeTmuxAttach).Scan(&deliveryID)
	if err != nil {
		return "", fmt.Errorf("select committed delivery attempt: %w", err)
	}
	return deliveryID, nil
}

// decideAutomatic is tx A. It records the gate's decision, evaluates the
// guards and applies the allowed-actions rules. A single allowed action
// is decided and carried out here. Two are left to the model: tx A then
// commits only the gate's decision and returns what tx C must find again.
func (e *Executor) decideAutomatic(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	verdict triageResult,
) (*pendingChoice, bool, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin automatic evaluation: %w", err)
	}
	defer rollback(tx)
	snapshot, _ := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return nil, false, err
	}
	decided, err := e.channel.AuthorizePTYDelivery(
		ctx, tx, record.channelMessage(), candidate.DeliveryGeneration, "",
	)
	if err != nil {
		return nil, false, fmt.Errorf("authorize automatic PTY delivery: %w", err)
	}
	guards := []protocol.GuardResult{}
	if decided.Allowed() {
		guards, err = e.evaluateGuards(ctx, tx, record, snapshot, false, "", "")
		if err != nil {
			return nil, false, err
		}
	}
	view, err := e.observe(ctx, tx, record, snapshot, decided.Allowed(), guards)
	if errors.Is(err, ErrStateObservationNotFound) {
		// No observation means no decision row and no invented state. The
		// bounded worker cadence will try again after a real snapshot arrives.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if verdict.found {
		// The stored row, as tx C will read it again, is the verdict used.
		verdict.verdict, verdict.found, err = triage.Lookup(ctx, tx, candidate.MessageID)
		if err != nil {
			return nil, false, fmt.Errorf("read triage verdict: %w", err)
		}
	}
	rule := e.allowedActions(view, decided, verdict)
	if (!e.choosing() || view.corroborable) && shouldWaitForDetector(view, rule) {
		// Rolled back: the next worker pass decides again.
		e.detectorWaits.set(candidate.AgentID, firstPublicHold(view.guards))
		return nil, false, nil
	}
	e.detectorWaits.forget(candidate.AgentID)
	if e.choosing() && shouldWaitForAutomaticHumanInput(view, rule) {
		// Everything tx A observed is rolled back. A later worker pass will
		// revisit the still-QUEUED row after the human input closes.
		e.forgetAutomaticStateWait(candidate)
		return nil, false, nil
	}
	if shouldWaitForAutomaticState(view, rule) &&
		e.withinAutomaticStateWindowAfterTriage(candidate, view.now) {
		return nil, true, nil
	}
	if e.choosing() && shouldWaitForAnalystState(view, rule) &&
		(!automaticStateReasonNeedsAnalysis(view.state.EligibilityReason) ||
			e.withinAnalystReanalysis(candidate, view.now)) {
		// The agent is busy, asking or waiting, or its analysis is not
		// current: a passing state. Rolled back, with the hold named for
		// the operator; the next worker pass decides again. A transient
		// phase waits until the delivery expires; a stale analysis is
		// demanded again within analystReanalysisLimit, then the human
		// decides, as before.
		e.detectorWaits.set(candidate.AgentID, analystHold(view.state))
		if automaticStateReasonNeedsAnalysis(view.state.EligibilityReason) {
			if err := e.states.Demand(candidate.AgentID); err != nil {
				return nil, false, fmt.Errorf("demand automatic state: %w", err)
			}
		}
		return nil, false, nil
	}
	if rule.gateOnly {
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit automatic gate decision: %w", err)
		}
		return nil, false, nil
	}
	if len(rule.actions) == 1 {
		return nil, false, e.decideAlone(ctx, tx, view, decided, rule)
	}
	pending := &pendingChoice{
		candidate: candidate, decided: decided, vector: view.vector(verdict.verdict),
		allowed: rule.actions, guards: view.guards, stateID: view.stateID,
		deterministic: !e.choosing(), corroborated: view.corroborated,
	}
	if !pending.deterministic {
		pending.request, pending.requestOK = e.chooseRequest(view, decided, verdict.verdict, rule.actions)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit automatic gate decision: %w", err)
	}
	return pending, false, nil
}

// shouldWaitForAnalystState recognizes a guard DEFER of the analyst route
// that only waits on the agent: the sole failed guard is p_idle_threshold,
// and the versioned state policy refused for a passing reason, the agent in
// a transient phase or an analysis that is not current. Such a DEFER opens
// no approval: an agent that is thinking, running a tool, asking or waiting
// will be idle again, and a stale analysis is renewed. Every other refusal,
// an exited agent or unsafe delivery among them, still goes to the human.
func shouldWaitForAnalystState(view automaticView, rule allowedRule) bool {
	if len(rule.actions) != 1 || rule.actions[0] != protocol.OrchestratorActionDefer ||
		rule.note != firstFailedGuard(view.guards) || !analystStatePasses(view.state) {
		return false
	}
	failed := 0
	for _, result := range view.guards {
		switch result.Outcome {
		case protocol.GuardOutcomePass:
		case protocol.GuardOutcomeFail:
			if result.Guard != protocol.GuardNamePIdleThreshold {
				return false
			}
			failed++
		default:
			return false
		}
	}
	return failed == 1
}

// analystStatePasses reports whether a state's ineligibility passes by
// itself: the agent is in a transient phase, or the analysis is stale,
// expired or already superseded by a newer observation.
func analystStatePasses(state StateRead) bool {
	switch state.EligibilityReason {
	case StateEligibilityStale, StateEligibilityExpired, StateEligibilityObservationNewer:
		return true
	case StateEligibilityNotIdle:
		switch state.Snapshot.AgentPhase {
		case protocol.AgentPhaseThinking, protocol.AgentPhaseRunningTool,
			protocol.AgentPhaseWaitingApproval, protocol.AgentPhaseAskingQuestion,
			protocol.AgentPhaseBackgroundWork:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// analystHold names, for the operator, why the analyst route waits: the
// public guard hold and, for a transient phase, the phase.
func analystHold(state StateRead) string {
	hold := PublicHold(pIdleGuard(state))
	if state.EligibilityReason == StateEligibilityNotIdle {
		return hold + " (" + string(state.Snapshot.AgentPhase) + ")"
	}
	return hold
}

// shouldWaitForAutomaticHumanInput recognizes only a guard DEFER caused by
// affirmative current human input plus p_idle_threshold. It is the tx A race
// counterpart of the pre-TRIAGE human wait; every unrelated guard must pass.
func shouldWaitForAutomaticHumanInput(view automaticView, rule allowedRule) bool {
	if len(rule.actions) != 1 || rule.actions[0] != protocol.OrchestratorActionDefer ||
		!automaticHumanInputBlocks(view.snapshot.LastReport) ||
		rule.note != firstFailedGuard(view.guards) {
		return false
	}
	foundIdle, foundHuman := false, false
	failed := make(map[protocol.GuardName]struct{}, 3)
	for _, result := range view.guards {
		if result.Outcome == protocol.GuardOutcomePass {
			continue
		}
		if result.Outcome != protocol.GuardOutcomeFail {
			return false
		}
		if _, duplicate := failed[result.Guard]; duplicate {
			return false
		}
		failed[result.Guard] = struct{}{}
		switch result.Guard {
		case protocol.GuardNamePIdleThreshold:
			foundIdle = true
		case protocol.GuardNameHumanDraftClosed, protocol.GuardNameHumanActivityNotActive:
			foundHuman = true
		default:
			return false
		}
	}
	return foundIdle && foundHuman
}

// shouldWaitForAutomaticState recognizes only the guard DEFER that a fresh
// state analysis can resolve. allowedActions remains the authority for rule
// precedence; every other hard guard must already pass.
func shouldWaitForAutomaticState(view automaticView, rule allowedRule) bool {
	if len(rule.actions) != 1 || rule.actions[0] != protocol.OrchestratorActionDefer ||
		!automaticStateReasonNeedsAnalysis(view.state.EligibilityReason) {
		return false
	}
	foundIdle := false
	for _, result := range view.guards {
		if result.Guard == protocol.GuardNamePIdleThreshold {
			if foundIdle || result.Outcome != protocol.GuardOutcomeFail {
				return false
			}
			foundIdle = true
			continue
		}
		if result.Outcome != protocol.GuardOutcomePass {
			return false
		}
	}
	return foundIdle && rule.note == firstFailedGuard(view.guards)
}

// decideAlone records and carries out the one allowed action in tx A.
func (e *Executor) decideAlone(
	ctx context.Context,
	tx *sql.Tx,
	view automaticView,
	decided channel.Decision,
	rule allowedRule,
) error {
	selected := rule.actions[0]
	switch selected {
	case protocol.OrchestratorActionExpire:
		if err := expireQueue(ctx, tx, view.record, view.now); err != nil {
			return err
		}
	case protocol.OrchestratorActionDropDuplicate:
		// Durable COMMITTED evidence proves the commit happened: the row,
		// still QUEUED in this transaction, is closed as delivered through
		// the attempt that holds the evidence, and nothing is sent again.
		// The attempts themselves do not change.
		deliveryID, err := committedAttempt(ctx, tx, view.record)
		if err != nil {
			return err
		}
		if err := transitionAttemptQueue(ctx, tx, deliveryID, view.record, queueStateQueued,
			queueStateDelivered, view.now, false); err != nil {
			return err
		}
	default:
		// DEFER opens its approval below; the rules leave no other single
		// action.
	}
	if _, err := e.insertDecision(ctx, tx, decisionRow{
		record: view.record, stateID: view.stateID, allowed: rule.actions, selected: selected,
		guards: view.guards, channel: decided.ID(), status: protocol.ExecutionStatusExecuted,
	}); err != nil {
		return err
	}
	if selected == protocol.OrchestratorActionDefer {
		if err := e.ensureAutomaticApproval(ctx, tx, view.record, rule.note); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit automatic evaluation: %w", err)
	}
	e.forgetAutomaticStateWait(view.record.QueueDelivery)
	if selected == protocol.OrchestratorActionDefer {
		e.wakeNotifications()
	}
	return nil
}

// observe reads what the allowed-actions rules and the vector need in tx.
// With the automatic decision wired, p_idle_threshold becomes the state
// producer's verdict: it passes only on an automatically eligible state.
func (e *Executor) observe(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	snapshot agentlink.Snapshot,
	allowed bool,
	guards []protocol.GuardResult,
) (automaticView, error) {
	view := automaticView{
		now: e.clock.Now().UTC(), snapshot: snapshot, record: record,
		guards: slices.Clone(guards),
	}
	if !e.choosing() {
		stateID, err := LatestRealStateID(ctx, tx, record.AgentID, record.sessionID, view.now)
		if err != nil {
			return automaticView{}, err
		}
		view.stateID = stateID
	} else {
		state, err := e.states.ReadLatestForAutomatic(ctx, tx, record.AgentID, record.sessionID,
			view.now, e.expectedStateProfile())
		if err != nil {
			return automaticView{}, fmt.Errorf("read automatic state: %w", err)
		}
		view.state, view.stateID = state, state.Snapshot.StateID
		if allowed {
			view.guards = withGuard(view.guards, pIdleGuard(state))
			view.corroborable = corroborable(state)
		}
	}
	if !allowed {
		return view, nil
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return automaticView{}, fmt.Errorf("check committed delivery evidence: %w", err)
	}
	view.committed = committed
	var queuedAt string
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		record.MessageID, record.AgentID, record.DeliveryGeneration).Scan(&queuedAt); err != nil {
		return automaticView{}, fmt.Errorf("select delivery queue time: %w", err)
	}
	view.queuedAt, err = eventstore.ParseTime(queuedAt)
	if err != nil {
		return automaticView{}, fmt.Errorf("parse delivery queue time: %w", err)
	}
	var manifest, lifecycle sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT manifest_json, updated_at FROM sessions
		WHERE session_id = ? AND agent_id = ?`, record.sessionID, record.AgentID).Scan(&manifest, &lifecycle)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return automaticView{}, fmt.Errorf("select session manifest: %w", err)
	}
	view.manifest, view.lifecycle = manifest.String, lifecycle.String
	if view.staleRun, err = staleRun(ctx, tx, record, queuedAt); err != nil {
		return automaticView{}, err
	}
	if !e.choosing() {
		// Without a model, p_idle_threshold is DeterministicPTYPolicy.
		eligible, reason := e.detectorEligible(snapshot, view.manifest, view.now)
		view.detectorOK, view.detectorReason = eligible, reason
		view.detector = detectorReferenceOf(snapshot.Detector)
		view.guards = withGuard(view.guards, detectorGuard(eligible, reason))
	} else if view.corroborable {
		// An analyst state below its threshold passes when the detector
		// corroborates it on a measured build, as on the tmux route.
		eligible, reason := e.detectorEligible(snapshot, view.manifest, view.now)
		view.detectorOK, view.detectorReason = eligible, reason
		view.detector = detectorReferenceOf(snapshot.Detector)
		if eligible {
			view.corroborated = true
			view.guards = withGuard(view.guards, guard(protocol.GuardNamePIdleThreshold, true, ""))
			view.guards = withGuard(view.guards, corroborationGuard())
		}
	}
	return view, nil
}

// vector is what tx C must find again. The manifest is compared by
// content besides the lifecycle revision: a capability change does not
// move sessions.updated_at, and a resumed session keeps its id.
func (v automaticView) vector(verdict triage.Verdict) decisionVector {
	sum := sha256.Sum256([]byte(v.manifest))
	return decisionVector{
		sessionID: v.snapshot.SessionID, lifecycle: v.lifecycle,
		guardVersion:    v.snapshot.LastReport.InputGuardVersion,
		draftGeneration: v.snapshot.LastReport.DraftGeneration,
		state:           automaticStateReferenceOf(v.state),
		manifestSHA256:  hex.EncodeToString(sum[:]), verdict: verdict,
		detector: v.detector,
	}
}

// equal compares two vectors; the verdict times are compared as instants.
func (v decisionVector) equal(o decisionVector) bool {
	if !v.state.sameInference(o.state) {
		return false
	}
	a, b := v, o
	a.state, b.state = automaticStateReference{}, automaticStateReference{}
	a.verdict.CreatedAt, b.verdict.CreatedAt = time.Time{}, time.Time{}
	return a == b && v.verdict.CreatedAt.Equal(o.verdict.CreatedAt)
}

// allowedActions applies the allowed-actions rules in order; the first
// that applies wins: the channel, expiry, a duplicate, the triage, the
// guards, the STALE cap. An action is allowed only if it has an executor
// and passes its guards, and the model is asked only with two of them.
func (e *Executor) allowedActions(view automaticView, decided channel.Decision, verdict triageResult) allowedRule {
	deferOnly := func(note string) allowedRule {
		return allowedRule{actions: []protocol.OrchestratorAction{protocol.OrchestratorActionDefer}, note: note}
	}
	switch {
	case !decided.Allowed():
		return deferOnly(noteChannel + string(decided.Outcome()))
	case !view.record.expiresAt.After(view.now):
		return allowedRule{actions: []protocol.OrchestratorAction{protocol.OrchestratorActionExpire}}
	case view.committed:
		return allowedRule{actions: []protocol.OrchestratorAction{protocol.OrchestratorActionDropDuplicate}}
	}
	if e.choosing() {
		switch {
		case verdict.failed:
			return deferOnly(noteUnclassified)
		case !verdict.found:
			return allowedRule{gateOnly: true}
		case !verdict.verdict.Classified():
			return deferOnly(noteUnclassified)
		case verdict.verdict.Label == protocol.TriageLabelRequiresHuman:
			return deferOnly(noteRequiresHuman)
		}
	}
	if failed := firstFailedGuard(view.guards); failed != "" {
		return deferOnly(failed)
	}
	if !e.choosing() && !view.detectorOK {
		// Unreachable while p_idle_threshold fails without an eligible
		// detector verdict; kept so no future guard change can let the
		// model path start half wired.
		return deferOnly(noteNoModel)
	}
	if view.staleRun >= staleCap {
		return deferOnly(noteStale)
	}
	return allowedRule{actions: []protocol.OrchestratorAction{
		protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
	}}
}

// pIdleGuard retains the historical wire name p_idle_threshold, but its
// outcome is the complete versioned state-eligibility policy. In the narrow
// detector-corroborated tmux scope that policy uses the validated IDLE label
// plus same-capture detector evidence without claiming p crossed theta.
func pIdleGuard(state StateRead) protocol.GuardResult {
	return guard(protocol.GuardNamePIdleThreshold, state.AutomaticEligible,
		"versioned state policy is not eligible: "+state.EligibilityReason)
}

// withGuard replaces the result of g's guard, or appends it.
func withGuard(guards []protocol.GuardResult, g protocol.GuardResult) []protocol.GuardResult {
	for i := range guards {
		if guards[i].Guard == g.Guard {
			guards[i] = g
			return guards
		}
	}
	return append(guards, g)
}

// staleRun counts the STALE decisions in a row that end the decisions of
// record's generation, which started when its queue row was created.
func staleRun(ctx context.Context, tx *sql.Tx, record deliveryRecord, since string) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT execution_status FROM orchestrator_decisions
		WHERE message_id = ? AND agent_id = ? AND created_at >= ?
		ORDER BY created_at DESC, decision_id DESC LIMIT ?`,
		record.MessageID, record.AgentID, since, staleCap)
	if err != nil {
		return 0, fmt.Errorf("select recent decisions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	run := 0
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return 0, fmt.Errorf("scan recent decision: %w", err)
		}
		if status != string(protocol.ExecutionStatusStale) {
			break
		}
		run++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read recent decisions: %w", err)
	}
	return run, nil
}

// chooseRequest builds the CHOOSE_ORCHESTRATION_ACTION request of view,
// with the allowed actions in a fresh random order. The screen is left
// out: no source brings it to the node yet. ok is false when no valid
// request can be built, which fails closed like a failed call.
func (e *Executor) chooseRequest(
	view automaticView,
	decided channel.Decision,
	verdict triage.Verdict,
	allowed []protocol.OrchestratorAction,
) (protocol.InferRequest, bool) {
	var manifest protocol.CapabilityManifest
	if json.Unmarshal([]byte(view.manifest), &manifest) != nil || manifest.Validate() != nil {
		return protocol.InferRequest{}, false
	}
	order, err := e.shuffle(allowed)
	if err != nil {
		return protocol.InferRequest{}, false
	}
	effective := decided.Effective()
	autoContinue := effective.FromAutoContinue
	request := protocol.OrchestrationRequest{
		TaskType: protocol.ModelContractChooseOrchestrationAction,
		Agent: protocol.OrchestrationAgent{
			ID: view.record.AgentID, Harness: manifest.Harness, Adapter: manifest.AdapterType,
			Capabilities: manifest.Capabilities,
		},
		State: protocol.OrchestrationState{
			HumanDraft: view.state.Snapshot.HumanDraftState, AgentPhase: view.state.Snapshot.AgentPhase,
			PIdle: view.state.IdleProbability, Attention: view.state.Snapshot.AttentionState,
		},
		Message: &protocol.PendingObject{
			Priority: view.record.priority, Triage: verdict.Label,
			AgeMS: max(0, view.now.Sub(view.queuedAt).Milliseconds()),
		},
		Channel: protocol.OrchestrationChannel{
			DecisionID: decided.ID(), Outcome: decided.Outcome(),
			Global: effective.GlobalMode, Link: effective.LinkMode, AutoContinue: &autoContinue,
		},
		AllowedActions: order,
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return protocol.InferRequest{}, false
	}
	infer := protocol.InferRequest{
		Contract: protocol.ModelContractChooseOrchestrationAction, AgentID: view.record.AgentID,
		Payload: protocol.Payload(payload),
	}
	if infer.Validate() != nil {
		return protocol.InferRequest{}, false
	}
	return infer, true
}

// shuffle returns actions in a uniformly random order drawn from the
// executor's entropy, so the letters change from call to call.
func (e *Executor) shuffle(actions []protocol.OrchestratorAction) ([]protocol.OrchestratorAction, error) {
	out := slices.Clone(actions)
	e.idMu.Lock()
	defer e.idMu.Unlock()
	for i := len(out) - 1; i > 0; i-- {
		n, err := rand.Int(e.entropy, big.NewInt(int64(i+1)))
		if err != nil {
			return nil, fmt.Errorf("shuffle allowed actions: %w", err)
		}
		j := int(n.Int64())
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// choose asks the model with no transaction open and classifies why no valid
// answer was returned. Only a call that reached Glimmer and failed is
// retryable; a request that could not be built and an open circuit stay with
// the human.
func (e *Executor) choose(ctx context.Context, pending *pendingChoice) (modelChoice, choiceCallOutcome) {
	if !pending.requestOK {
		return modelChoice{}, choiceCallUnsent
	}
	callCtx, cancel := context.WithTimeout(ctx, chooseTimeout)
	defer cancel()
	result, err := e.model.Infer(callCtx, pending.request)
	if errors.Is(err, gateway.ErrCircuitOpen) {
		return modelChoice{}, choiceCallCircuitOpen
	}
	choice, ok := readChoice(result, err, pending.allowed)
	if !ok {
		return modelChoice{}, choiceCallFailed
	}
	return choice, choiceCallValid
}

// readChoice accepts an answer only if its call succeeded and its label
// is the single most probable of actions this request allowed. The model
// can never widen allowed: an action outside it fails the call.
func readChoice(
	result protocol.InferResult,
	err error,
	allowed []protocol.OrchestratorAction,
) (modelChoice, bool) {
	if err != nil || result.Contract != protocol.ModelContractChooseOrchestrationAction ||
		result.CallRecord.Status != protocol.GlimmerCallStatusOK || !result.CallRecord.CallID.Valid() ||
		len(result.Probabilities) == 0 {
		return modelChoice{}, false
	}
	action := protocol.OrchestratorAction(result.Label)
	if !slices.Contains(allowed, action) {
		return modelChoice{}, false
	}
	probabilities := make(map[protocol.OrchestratorAction]float64, len(result.Probabilities))
	for label, p := range result.Probabilities {
		key := protocol.OrchestratorAction(label)
		if !slices.Contains(allowed, key) || math.IsNaN(p) || p < 0 || p > 1 {
			return modelChoice{}, false
		}
		probabilities[key] = p
	}
	for other, p := range probabilities {
		if other != action && p >= probabilities[action] {
			return modelChoice{}, false
		}
	}
	return modelChoice{
		action: action, probabilities: probabilities, callID: result.CallRecord.CallID,
		model: decisionModel(result.CallRecord),
	}, true
}

// decisionModel names the model of a call as repo@revision, or nothing
// when that is not a bounded identifier.
func decisionModel(record protocol.GlimmerCallRecord) string {
	model := protocol.ModelMetadataValue(record.BaseModelRepo + "@" + record.BaseModelRevision)
	if !model.Valid() || len(model) > maxDecisionModelLen {
		return ""
	}
	return string(model)
}

// finishChoice is tx C. A failed call defers to the human. A valid choice
// is carried out only if the delivery is still exactly QUEUED, the
// channel revalidates, every guard passes again and the vector of tx A is
// unchanged; otherwise the decision is STALE and nothing is done, so the
// next pass goes back to the gate.
func (e *Executor) finishChoice(
	ctx context.Context,
	pending *pendingChoice,
	choice modelChoice,
	outcome choiceCallOutcome,
) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin automatic revalidation: %w", err)
	}
	defer rollback(tx)
	candidate := pending.candidate
	var queueState string
	err = tx.QueryRowContext(ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&queueState)
	if err != nil {
		return fmt.Errorf("reload automatic delivery: %w", err)
	}
	stale := func(record deliveryRecord, guards []protocol.GuardResult) error {
		return e.finishStale(ctx, tx, pending, choice, outcome == choiceCallValid, record, guards)
	}
	if queueState != queueStateQueued {
		return stale(deliveryRecord{QueueDelivery: candidate}, pending.guards)
	}
	snapshot, _ := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return err
	}
	if outcome != choiceCallValid {
		status := protocol.ExecutionStatusExecuted
		switch outcome {
		case choiceCallFailed:
			status = protocol.ExecutionStatusFailed
		case choiceCallCircuitOpen:
			status = protocol.ExecutionStatusAborted
		case choiceCallUnsent, choiceCallValid:
		}
		return e.finishDefer(ctx, tx, pending, modelChoice{action: protocol.OrchestratorActionDefer},
			record, pending.guards, noteModelFailed, status)
	}
	message := record.channelMessage()
	err = e.channel.Revalidate(ctx, tx, pending.decided, message, candidate.DeliveryGeneration)
	var changed *channel.RevalidationError
	if errors.As(err, &changed) {
		return stale(record, pending.guards)
	}
	if err != nil {
		return fmt.Errorf("revalidate automatic PTY delivery: %w", err)
	}
	guards, err := e.evaluateGuards(ctx, tx, record, snapshot, false, "", "")
	if err != nil {
		return err
	}
	view, err := e.observe(ctx, tx, record, snapshot, true, guards)
	if errors.Is(err, ErrStateObservationNotFound) || errors.Is(err, ErrStateObservationInvalid) {
		return stale(record, guards)
	}
	if err != nil {
		return err
	}
	verdict, found, err := triage.Lookup(ctx, tx, candidate.MessageID)
	if err != nil {
		return fmt.Errorf("reload triage verdict: %w", err)
	}
	if pending.deterministic {
		// No triage ran in tx A: its vector holds no verdict.
		verdict, found = triage.Verdict{}, true
		if e.choosing() {
			return stale(record, view.guards)
		}
	}
	if !found || !view.vector(verdict).equal(pending.vector) || firstFailedGuard(view.guards) != "" ||
		view.committed || !view.record.expiresAt.After(view.now) {
		return stale(record, view.guards)
	}
	if choice.action == protocol.OrchestratorActionDefer {
		return e.finishDefer(ctx, tx, pending, choice, record, view.guards, noteModelDeferred,
			protocol.ExecutionStatusExecuted)
	}
	return e.finishInject(ctx, tx, pending, choice, view, message)
}

// finishStale records the choice as STALE and does nothing else.
func (e *Executor) finishStale(
	ctx context.Context,
	tx *sql.Tx,
	pending *pendingChoice,
	choice modelChoice,
	ok bool,
	record deliveryRecord,
	guards []protocol.GuardResult,
) error {
	if !ok {
		choice = modelChoice{action: protocol.OrchestratorActionDefer}
	}
	if _, err := e.insertDecision(ctx, tx, choiceRow(pending, choice, record, guards,
		protocol.ExecutionStatusStale)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit stale automatic decision: %w", err)
	}
	e.forgetAutomaticStateWait(pending.candidate)
	return nil
}

// finishDefer records a DEFER and opens the human approval.
func (e *Executor) finishDefer(
	ctx context.Context,
	tx *sql.Tx,
	pending *pendingChoice,
	choice modelChoice,
	record deliveryRecord,
	guards []protocol.GuardResult,
	note string,
	status protocol.ExecutionStatus,
) error {
	if _, err := e.insertDecision(ctx, tx, choiceRow(pending, choice, record, guards,
		status)); err != nil {
		return err
	}
	if err := e.ensureAutomaticApproval(ctx, tx, record, note); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit automatic deferral: %w", err)
	}
	e.forgetAutomaticStateWait(pending.candidate)
	e.wakeNotifications()
	return nil
}

// finishInject reserves the delivery like an approved order and runs the
// two-phase protocol, which revalidates once more before its COMMIT.
func (e *Executor) finishInject(
	ctx context.Context,
	tx *sql.Tx,
	pending *pendingChoice,
	choice modelChoice,
	view automaticView,
	message channel.Message,
) error {
	record, report := view.record, view.snapshot.LastReport
	if err := captureProcessBindingVersion(ctx, tx, &record); err != nil {
		return err
	}
	if _, err := e.approvals.SupersedeOrchestratorApproval(
		ctx, tx, record.MessageID, record.AgentID,
	); err != nil {
		return fmt.Errorf("supersede automatic approval: %w", err)
	}
	persisted, err := e.insertDecision(ctx, tx, choiceRow(pending, choice, record, view.guards,
		protocol.ExecutionStatusPending))
	if err != nil {
		return err
	}
	deliveryID, err := e.mintOpaqueID("dlv", view.now)
	if err != nil {
		return err
	}
	if err := e.claimDelivery(ctx, tx, record); err != nil {
		return err
	}
	attempt, err := e.attempts.Create(ctx, tx, daemon.AttemptSpec{
		DeliveryID: deliveryID, MessageID: record.MessageID, AgentID: record.AgentID,
		DeliveryGeneration: record.DeliveryGeneration, AdapterSessionID: record.sessionID,
		OrchestratorDecisionID: persisted.decisionID, ChannelDecisionID: pending.decided.ID(),
		DraftGeneration: report.DraftGeneration, InputGuardVersion: report.InputGuardVersion,
		ControlVersions: pending.decided.Versions(),
	})
	if err != nil {
		return fmt.Errorf("create automatic delivery attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit automatic delivery plan: %w", err)
	}
	e.forgetAutomaticStateWait(pending.candidate)
	return e.execute(ctx, executionPlan{
		record: record, decision: pending.decided, attempt: attempt, message: message,
		ref: protocol.MessageRef{
			MessageID: record.MessageID, FromAgent: record.fromAgent,
			Priority: record.priority, ThreadID: record.threadID,
		},
		report: report,
		automatic: &automaticExpectation{
			state:         automaticStateReferenceOf(view.state),
			deterministic: pending.deterministic, corroborated: pending.corroborated,
			detector: view.detector, manifest: view.manifest,
		},
	})
}

// automaticStateHolds is the commit's check of an automatic plan, in the
// transaction that marks COMMIT_SENT, after the prepare: the decision's
// inference must still be represented by the latest valid state for the
// session and profile, and that latest state must be eligible. Any failure to
// read it holds nothing, so the prepared delivery is cancelled.
func (e *Executor) automaticStateHolds(ctx context.Context, tx *sql.Tx, plan executionPlan) bool {
	if plan.automatic.deterministic {
		return !e.choosing() && e.detectorHolds(ctx, tx, plan)
	}
	if !e.choosing() {
		return false
	}
	state, err := e.states.ReadLatestForAutomatic(ctx, tx, plan.record.AgentID, plan.record.sessionID,
		e.clock.Now().UTC(), e.expectedStateProfile())
	if err != nil || !automaticStateReferenceOf(state).sameInference(plan.automatic.state) {
		return false
	}
	if plan.automatic.corroborated {
		return corroborable(state) && e.detectorHolds(ctx, tx, plan)
	}
	return state.AutomaticEligible
}

// choiceRow is the decision row of a model choice, or of a DEFER that
// cites no call.
func choiceRow(
	pending *pendingChoice,
	choice modelChoice,
	record deliveryRecord,
	guards []protocol.GuardResult,
	status protocol.ExecutionStatus,
) decisionRow {
	row := decisionRow{
		record: record, stateID: pending.stateID, allowed: pending.allowed, selected: choice.action,
		probabilities: choice.probabilities, callID: choice.callID, model: choice.model,
		guards: guards, channel: pending.decided.ID(), status: status,
	}
	switch {
	case pending.deterministic:
		row.policy = DeterministicPTYPolicy
	case pending.corroborated:
		row.policy = CorroboratedPTYPolicy
	}
	return row
}

// ensureAutomaticApproval opens, or keeps, the human approval of a DEFER.
func (e *Executor) ensureAutomaticApproval(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	note string,
) error {
	if _, _, err := e.approvals.EnsureOrchestratorApproval(
		ctx, tx, record.MessageID, record.AgentID, truncate(notePrefix+note, 500),
	); err != nil {
		return fmt.Errorf("ensure automatic approval: %w", err)
	}
	return nil
}

// checkDecisionRow enforces the OrchestratorDecision.Validate rules that
// fall on the row: the selected action stays inside the allowed set, every
// probability key stays inside it, and the channel link stays readable by
// decision.get. A contradiction fails instead of persisting.
func checkDecisionRow(row decisionRow) error {
	if !slices.Contains(row.allowed, row.selected) {
		return fmt.Errorf("orchestrator decision selected action %q: must be in allowed_actions",
			row.selected)
	}
	allowed := make(map[protocol.OrchestratorAction]bool, len(row.allowed))
	for _, action := range row.allowed {
		allowed[action] = true
	}
	for action := range row.probabilities {
		if !allowed[action] {
			return fmt.Errorf("orchestrator decision probabilities[%s]: outside allowed_actions",
				action)
		}
	}
	if !row.channel.Valid() {
		return fmt.Errorf("orchestrator decision channel: invalid channel decision id")
	}
	return nil
}

// insertDecision writes one orchestrator_decisions row. Probabilities, a
// call id and a model are stored only together, as the contract requires.
func (e *Executor) insertDecision(ctx context.Context, tx *sql.Tx, row decisionRow) (persistedEvaluation, error) {
	now := e.clock.Now().UTC()
	allowedJSON, err := json.Marshal(row.allowed)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("encode allowed actions: %w", err)
	}
	guards := row.guards
	if guards == nil {
		guards = []protocol.GuardResult{}
	}
	guardsJSON, err := json.Marshal(guards)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("encode hard guards: %w", err)
	}
	if previous, same, err := e.unchangedDefer(ctx, tx, row, string(allowedJSON)); err != nil {
		return persistedEvaluation{}, err
	} else if same {
		return previous, nil
	}
	// The row carries the Validate rules that fall on it: the selected
	// action stays inside the allowed set, every probability key stays
	// inside it, and the channel link stays readable by decision.get. A
	// contradiction fails here instead of persisting.
	if err := checkDecisionRow(row); err != nil {
		return persistedEvaluation{}, err
	}
	decisionID, err := e.mintULID(now)
	if err != nil {
		return persistedEvaluation{}, err
	}
	var probabilities, callID, model sql.NullString
	if len(row.probabilities) > 0 && row.callID.Valid() {
		raw, err := json.Marshal(row.probabilities)
		if err != nil {
			return persistedEvaluation{}, fmt.Errorf("encode decision probabilities: %w", err)
		}
		probabilities = sql.NullString{String: string(raw), Valid: true}
		callID = sql.NullString{String: string(row.callID), Valid: true}
		model = sql.NullString{String: row.model, Valid: row.model != ""}
	}
	policy := e.policy
	if row.policy != "" {
		policy = row.policy
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO orchestrator_decisions (
		decision_id, agent_id, message_id, artifact_id, state_id,
		allowed_actions_json, selected_action, probabilities_json, hard_guards_json,
		policy_version, decision_model, glimmer_call_id, channel_decision_id,
		created_at, execution_status
	) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		decisionID, row.record.AgentID, row.record.MessageID, row.stateID, string(allowedJSON),
		row.selected, probabilities, string(guardsJSON), policy, model, callID, row.channel,
		eventstore.FormatTime(now), row.status)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("insert orchestrator decision: %w", err)
	}
	return persistedEvaluation{decisionID: decisionID}, nil
}
