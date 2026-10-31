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
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/triage"
)

// This file holds the automatic decision of 5.6–5.8. The deterministic
// rules decide what is allowed; the model only chooses between
// INJECT_PTY_PROMPT and DEFER when every rule and guard already allows
// injecting. No model call runs with a transaction open: tx A gates,
// guards and fixes the allowed actions, the call runs after its commit,
// and tx C revalidates everything tx A saw before it acts.

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
// through the decision's own transaction. *StateProducer satisfies it.
type AutomaticStates interface {
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
}

// decisionVector is what tx A saw and tx C must see again. The
// generation is the candidate's own, the control vector is checked by
// channel.Revalidate, and the guards are evaluated again besides.
type decisionVector struct {
	sessionID       protocol.OpaqueID
	lifecycle       string
	guardVersion    int64
	draftGeneration int64
	stateID         protocol.ULID
	stateVersion    int64
	manifestSHA256  string
	verdict         triage.Verdict
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
}

// modelChoice is a CHOOSE answer that passed every check.
type modelChoice struct {
	action        protocol.OrchestratorAction
	probabilities map[protocol.OrchestratorAction]float64
	callID        protocol.OpaqueID
	model         string
}

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
}

// choosing reports whether the automatic decision of 5.6–5.8 is wired.
// Without it no automatic evaluation delivers, as in 5.3c.
func (e *Executor) choosing() bool {
	return e.model != nil && e.triager != nil && e.states != nil && e.stateProfile != nil
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
		JOIN delivery_result_evidence AS evidence ON evidence.delivery_id = attempt.delivery_id
		WHERE attempt.message_id = ? AND attempt.agent_id = ?
			AND attempt.delivery_generation = ? AND evidence.result = 'COMMITTED'
		ORDER BY attempt.created_at, attempt.delivery_id LIMIT 1`,
		record.MessageID, record.AgentID, record.DeliveryGeneration).Scan(&deliveryID)
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
) (*pendingChoice, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin automatic evaluation: %w", err)
	}
	defer rollback(tx)
	snapshot, _ := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return nil, err
	}
	decided, err := e.channel.AuthorizePTYDelivery(ctx, tx, record.channelMessage(), candidate.DeliveryGeneration)
	if err != nil {
		return nil, fmt.Errorf("authorize automatic PTY delivery: %w", err)
	}
	guards := []protocol.GuardResult{}
	if decided.Allowed() {
		guards, err = e.evaluateGuards(ctx, tx, record, snapshot, false, "", "")
		if err != nil {
			return nil, err
		}
	}
	view, err := e.observe(ctx, tx, record, snapshot, decided.Allowed(), guards)
	if errors.Is(err, ErrStateObservationNotFound) {
		// No observation means no decision row and no invented state. The
		// bounded worker cadence will try again after a real snapshot arrives.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if verdict.found {
		// The stored row, as tx C will read it again, is the verdict used.
		verdict.verdict, verdict.found, err = triage.Lookup(ctx, tx, candidate.MessageID)
		if err != nil {
			return nil, fmt.Errorf("read triage verdict: %w", err)
		}
	}
	rule := e.allowedActions(view, decided, verdict)
	if rule.gateOnly {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit automatic gate decision: %w", err)
		}
		return nil, nil
	}
	if len(rule.actions) == 1 {
		return nil, e.decideAlone(ctx, tx, view, decided, rule)
	}
	pending := &pendingChoice{
		candidate: candidate, decided: decided, vector: view.vector(verdict.verdict),
		allowed: rule.actions, guards: view.guards, stateID: view.stateID,
	}
	pending.request, pending.requestOK = e.chooseRequest(view, decided, verdict.verdict, rule.actions)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit automatic gate decision: %w", err)
	}
	return pending, nil
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
			view.now, e.stateProfile)
		if err != nil {
			return automaticView{}, fmt.Errorf("read automatic state: %w", err)
		}
		view.state, view.stateID = state, state.Snapshot.StateID
		if allowed {
			view.guards = withGuard(view.guards, pIdleGuard(state))
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
		stateID:         v.state.Snapshot.StateID, stateVersion: v.state.Snapshot.StateVersion,
		manifestSHA256: hex.EncodeToString(sum[:]), verdict: verdict,
	}
}

// equal compares two vectors; the verdict times are compared as instants.
func (v decisionVector) equal(o decisionVector) bool {
	a, b := v, o
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
	if !e.choosing() {
		// Unreachable while p_idle_threshold fails without a model; kept so
		// no future guard change can let the model path start half wired.
		return deferOnly(noteNoModel)
	}
	if view.staleRun >= staleCap {
		return deferOnly(noteStale)
	}
	return allowedRule{actions: []protocol.OrchestratorAction{
		protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
	}}
}

// pIdleGuard is p_idle_threshold for an automatic decision.
func pIdleGuard(state StateRead) protocol.GuardResult {
	return guard(protocol.GuardNamePIdleThreshold, state.AutomaticEligible,
		"state is not eligible: "+state.EligibilityReason)
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

// choose asks the model with no transaction open. ok is false on any
// failure: no request, a failed call, or an answer that does not pass.
func (e *Executor) choose(ctx context.Context, pending *pendingChoice) (modelChoice, bool) {
	if !pending.requestOK {
		return modelChoice{}, false
	}
	callCtx, cancel := context.WithTimeout(ctx, chooseTimeout)
	defer cancel()
	result, err := e.model.Infer(callCtx, pending.request)
	return readChoice(result, err, pending.allowed)
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
	ok bool,
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
		return e.finishStale(ctx, tx, pending, choice, ok, record, guards)
	}
	if queueState != queueStateQueued {
		return stale(deliveryRecord{QueueDelivery: candidate}, pending.guards)
	}
	snapshot, _ := e.linkSnapshot(ctx, tx, candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return err
	}
	if !ok {
		return e.finishDefer(ctx, tx, pending, modelChoice{action: protocol.OrchestratorActionDefer},
			record, pending.guards, noteModelFailed)
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
	if !found || !view.vector(verdict).equal(pending.vector) || firstFailedGuard(view.guards) != "" ||
		view.committed || !view.record.expiresAt.After(view.now) {
		return stale(record, view.guards)
	}
	if choice.action == protocol.OrchestratorActionDefer {
		return e.finishDefer(ctx, tx, pending, choice, record, view.guards, noteModelDeferred)
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
) error {
	if _, err := e.insertDecision(ctx, tx, choiceRow(pending, choice, record, guards,
		protocol.ExecutionStatusExecuted)); err != nil {
		return err
	}
	if err := e.ensureAutomaticApproval(ctx, tx, record, note); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit automatic deferral: %w", err)
	}
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
	return e.execute(ctx, executionPlan{
		record: record, decision: pending.decided, attempt: attempt, message: message,
		ref: protocol.MessageRef{
			MessageID: record.MessageID, FromAgent: record.fromAgent,
			Priority: record.priority, ThreadID: record.threadID,
		},
		report: report,
	})
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
	return decisionRow{
		record: record, stateID: pending.stateID, allowed: pending.allowed, selected: choice.action,
		probabilities: choice.probabilities, callID: choice.callID, model: choice.model,
		guards: guards, channel: pending.decided.ID(), status: status,
	}
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

// insertDecision writes one orchestrator_decisions row. Probabilities, a
// call id and a model are stored only together, as the contract requires.
func (e *Executor) insertDecision(ctx context.Context, tx *sql.Tx, row decisionRow) (persistedEvaluation, error) {
	now := e.clock.Now().UTC()
	decisionID, err := e.mintULID(now)
	if err != nil {
		return persistedEvaluation{}, err
	}
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
	_, err = tx.ExecContext(ctx, `INSERT INTO orchestrator_decisions (
		decision_id, agent_id, message_id, artifact_id, state_id,
		allowed_actions_json, selected_action, probabilities_json, hard_guards_json,
		policy_version, decision_model, glimmer_call_id, channel_decision_id,
		created_at, execution_status
	) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		decisionID, row.record.AgentID, row.record.MessageID, row.stateID, string(allowedJSON),
		row.selected, probabilities, string(guardsJSON), e.policy, model, callID, row.channel,
		eventstore.FormatTime(now), row.status)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("insert orchestrator decision: %w", err)
	}
	return persistedEvaluation{decisionID: decisionID}, nil
}
