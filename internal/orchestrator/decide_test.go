package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/triage"
)

const (
	choiceCallID    protocol.OpaqueID = "call-choose-1"
	choiceTriageID  protocol.OpaqueID = "call-triage-1"
	choiceRepo                        = "example/base-model"
	choiceRevision                    = "rev-1"
	choiceModelName                   = choiceRepo + "@" + choiceRevision
	choiceInject                      = protocol.OrchestratorActionInjectPTYPrompt
	choiceDefer                       = protocol.OrchestratorActionDefer
	choiceChoose                      = protocol.ModelContractChooseOrchestrationAction
	choiceTriage                      = protocol.ModelContractTriageMessage
	queueQueued                       = "QUEUED"
)

// choiceProfile is the state analysis profile the tests wire.
var choiceProfile = StateAnalysisProfile{
	Revision: "profile-1", SHA256: protocol.SHA256Hex(strings.Repeat("a", 64)),
	CalibrationRevision: "calibration-1",
	CalibrationSHA256:   protocol.SHA256Hex(strings.Repeat("b", 64)),
	Temperature:         1.5, IdleThreshold: 0.9,
}

// choiceModel is the one model transport of a test: it answers TRIAGE and
// CHOOSE, keeps every request, and during each call checks that no write
// transaction is open and runs the test's change of the world.
type choiceModel struct {
	mu       sync.Mutex
	requests []protocol.InferRequest
	barrier  *sql.DB
	open     []error
	during   func(protocol.InferRequest)
	label    protocol.TriageLabel
	action   protocol.OrchestratorAction
	answer   func(protocol.InferRequest) (protocol.InferResult, error)
}

func (m *choiceModel) Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error) {
	m.mu.Lock()
	m.requests = append(m.requests, req)
	barrier, during, answer, label, action := m.barrier, m.during, m.answer, m.label, m.action
	m.mu.Unlock()
	if barrier != nil {
		// BEGIN IMMEDIATE with no busy wait fails while any other write
		// transaction is open on the node database.
		tx, err := barrier.BeginTx(ctx, nil)
		if err == nil {
			err = tx.Rollback()
		}
		m.mu.Lock()
		m.open = append(m.open, err)
		m.mu.Unlock()
	}
	if during != nil {
		during(req)
	}
	if answer != nil {
		return answer(req)
	}
	if req.Contract == choiceTriage {
		return triageAnswer(label), nil
	}
	return chooseAnswer(action), nil
}

func (m *choiceModel) calls(contract protocol.ModelContract) []protocol.InferRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []protocol.InferRequest
	for _, req := range m.requests {
		if req.Contract == contract {
			out = append(out, req)
		}
	}
	return out
}

func (m *choiceModel) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func (m *choiceModel) barrierResults() []error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.open)
}

func (m *choiceModel) set(update func(*choiceModel)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	update(m)
}

func callRecord(
	contract protocol.ModelContract,
	id protocol.OpaqueID,
	probabilities map[string]float64,
) protocol.GlimmerCallRecord {
	digest := protocol.SHA256Hex(strings.Repeat("c", 64))
	return protocol.GlimmerCallRecord{
		CallID: id, Contract: contract, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: choiceRepo, BaseModelRevision: choiceRevision, Runtime: "fake",
		RedactionPolicyVersion: "v1", InputSHA256: digest, OutputSHA256: digest,
		Probabilities: probabilities, Status: protocol.GlimmerCallStatusOK, CreatedAt: executorTestTime,
	}
}

func chooseAnswer(action protocol.OrchestratorAction) protocol.InferResult {
	other := choiceDefer
	if action == choiceDefer {
		other = choiceInject
	}
	probabilities := map[string]float64{string(action): 0.8, string(other): 0.2}
	return protocol.InferResult{
		Contract: choiceChoose, InferenceProfile: protocol.InferenceProfileBase,
		Label: string(action), Probabilities: probabilities,
		CallRecord: callRecord(choiceChoose, choiceCallID, probabilities),
	}
}

func triageAnswer(label protocol.TriageLabel) protocol.InferResult {
	probabilities := map[string]float64{string(label): 1}
	return protocol.InferResult{
		Contract: choiceTriage, InferenceProfile: protocol.InferenceProfileBase,
		Label: string(label), Probabilities: probabilities,
		CallRecord: callRecord(choiceTriage, choiceTriageID, probabilities),
	}
}

// choiceTriager stores the verdict a test chooses as a real
// message_triage row, which tx C reads again.
type choiceTriager struct {
	mu      sync.Mutex
	db      *sql.DB
	label   protocol.TriageLabel
	outcome triage.Outcome
	absent  bool
	err     error
	calls   int
	during  func()
	// returned, when set, is answered instead of the stored row.
	returned *triage.Verdict
}

func (f *choiceTriager) Triage(ctx context.Context, d triage.Delivery) (triage.Verdict, bool, error) {
	f.mu.Lock()
	f.calls++
	label, outcome, absent, err, during, returned :=
		f.label, f.outcome, f.absent, f.err, f.during, f.returned
	f.mu.Unlock()
	if err != nil || absent {
		return triage.Verdict{}, false, err
	}
	if during != nil {
		during()
	}
	var stored, call any
	if outcome == triage.OutcomeClassified {
		stored, call = string(label), string(choiceTriageID)
	}
	if _, err := f.db.ExecContext(ctx, `INSERT INTO message_triage
		(message_id, label, outcome, glimmer_call_id, created_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (message_id) DO NOTHING`, string(d.MessageID), stored, string(outcome), call,
		eventstore.FormatTime(executorTestTime)); err != nil {
		return triage.Verdict{}, false, err
	}
	if returned != nil {
		return *returned, true, nil
	}
	return triage.Lookup(ctx, f.db, d.MessageID)
}

func (f *choiceTriager) set(update func(*choiceTriager)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	update(f)
}

func (f *choiceTriager) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// choiceStates reads the real latest state row and gives it the
// eligibility a test chooses; onRead changes what one read returns.
type choiceStates struct {
	mu        sync.Mutex
	eligible  bool
	reason    string
	err       error
	reads     int
	demands   []protocol.AgentID
	demandErr error
	onRead    func(state *StateRead)
}

func (f *choiceStates) Demand(agent protocol.AgentID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.demands = append(f.demands, agent)
	return f.demandErr
}

func (f *choiceStates) ReadLatestForAutomatic(
	ctx context.Context,
	q stateQueryer,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	now time.Time,
	expected *StateAnalysisProfile,
) (StateRead, error) {
	f.mu.Lock()
	f.reads++
	eligible, reason, err, onRead := f.eligible, f.reason, f.err, f.onRead
	f.mu.Unlock()
	if err != nil {
		return StateRead{}, err
	}
	if expected == nil || *expected != choiceProfile {
		return StateRead{}, errors.New("the executor passed another state profile")
	}
	state, err := ReadLatestState(ctx, q, agent, session, now, expected)
	if err != nil {
		return StateRead{}, err
	}
	state.AutomaticEligible, state.EligibilityReason = eligible, reason
	if eligible {
		state.EligibilityReason = StateEligibilityEligible
		state.Snapshot.AgentPhase = protocol.AgentPhaseIdleAtPrompt
		state.IdleProbability = 0.99
	}
	if onRead != nil {
		onRead(&state)
	}
	return state, nil
}

func (f *choiceStates) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

func (f *choiceStates) demandCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.demands)
}

func (f *choiceStates) set(update func(*choiceStates)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	update(f)
}

type choiceFixture struct {
	*executorFixture
	model   *choiceModel
	triager *choiceTriager
	states  *choiceStates
	barrier *sql.DB
}

// newChoiceFixture is the executor fixture with the automatic decision
// wired to fakes: a classified NORMAL triage, an eligible state, and a
// model that chooses INJECT_PTY_PROMPT.
func newChoiceFixture(t *testing.T) *choiceFixture {
	t.Helper()
	return choiceFakes(t, newExecutorFixture(t))
}

// choiceFakes wires those fakes into the fixture's executor.
func choiceFakes(t *testing.T, fixture *executorFixture) *choiceFixture {
	t.Helper()
	barrier := openBarrier(t, fixture.store.DB())
	f := &choiceFixture{
		executorFixture: fixture, barrier: barrier,
		model: &choiceModel{
			barrier: barrier, label: protocol.TriageLabelNormal, action: choiceInject,
		},
		triager: &choiceTriager{
			db: fixture.store.DB(), label: protocol.TriageLabelNormal, outcome: triage.OutcomeClassified,
		},
		states: &choiceStates{eligible: true},
	}
	f.wire(fixture.executor)
	return f
}

func (f *choiceFixture) wire(e *Executor) {
	profile := choiceProfile
	e.model, e.triager, e.states, e.stateProfile = f.model, f.triager, f.states, &profile
}

// openBarrier opens the node database again with no busy wait.
func openBarrier(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()
	var seq int
	var name, file string
	require.NoError(t, db.QueryRowContext(t.Context(), "PRAGMA database_list").Scan(&seq, &name, &file))
	barrier, err := sql.Open("sqlite", file+"?_pragma=busy_timeout(0)&_txlock=immediate")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, barrier.Close()) })
	return barrier
}

func (f *choiceFixture) setReport(update func(*protocol.InputGuardReport)) {
	f.links.mu.Lock()
	defer f.links.mu.Unlock()
	update(&f.links.snapshot.LastReport)
}

type decisionSeen struct {
	allowed       []protocol.OrchestratorAction
	selected      protocol.OrchestratorAction
	status        protocol.ExecutionStatus
	probabilities map[protocol.OrchestratorAction]float64
	callID        string
	model         string
	stateID       string
	policy        string
	guards        []protocol.GuardResult
}

func decisionsFor(t *testing.T, db *sql.DB, message protocol.ULID) []decisionSeen {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT allowed_actions_json, selected_action,
		execution_status, probabilities_json, glimmer_call_id, decision_model, state_id,
		policy_version, hard_guards_json FROM orchestrator_decisions
		WHERE message_id = ? ORDER BY created_at, decision_id`, message)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []decisionSeen
	for rows.Next() {
		var d decisionSeen
		var allowed, guards string
		var probabilities, callID, model sql.NullString
		require.NoError(t, rows.Scan(&allowed, &d.selected, &d.status, &probabilities, &callID,
			&model, &d.stateID, &d.policy, &guards))
		require.NoError(t, json.Unmarshal([]byte(allowed), &d.allowed))
		require.NoError(t, json.Unmarshal([]byte(guards), &d.guards))
		if probabilities.Valid {
			require.NoError(t, json.Unmarshal([]byte(probabilities.String), &d.probabilities))
		}
		d.callID, d.model = callID.String, model.String
		out = append(out, d)
	}
	require.NoError(t, rows.Err())
	return out
}

func lastDecision(t *testing.T, db *sql.DB, message protocol.ULID) decisionSeen {
	t.Helper()
	decisions := decisionsFor(t, db, message)
	require.NotEmpty(t, decisions)
	return decisions[len(decisions)-1]
}

func pendingNotes(t *testing.T, db *sql.DB, message protocol.ULID) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT COALESCE(request_note, '')
		FROM approval_requests WHERE object_id = ? AND state = 'PENDING' ORDER BY rowid`, message)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var notes []string
	for rows.Next() {
		var note string
		require.NoError(t, rows.Scan(&note))
		notes = append(notes, note)
	}
	require.NoError(t, rows.Err())
	return notes
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), query, args...).Scan(&n))
	return n
}

func attemptCount(t *testing.T, db *sql.DB, message protocol.ULID) int {
	t.Helper()
	return countRows(t, db, "SELECT count(*) FROM delivery_attempts WHERE message_id = ?", message)
}

// decidedAttempts counts the attempts an orchestrator decision of message
// planned.
func decidedAttempts(t *testing.T, db *sql.DB, message protocol.ULID) int {
	t.Helper()
	return countRows(t, db, `SELECT count(*) FROM delivery_attempts WHERE orchestrator_decision_id IN
		(SELECT decision_id FROM orchestrator_decisions WHERE message_id = ?)`, message)
}

func gateDecisionCount(t *testing.T, db *sql.DB, message protocol.ULID) int {
	t.Helper()
	return countRows(t, db, "SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", message)
}

func queueStateOf(t *testing.T, db *sql.DB, candidate daemon.QueueDelivery) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&state))
	return state
}

// seedCommittedEvidence leaves candidate QUEUED behind an UNCERTAIN
// attempt whose late COMMITTED result the Proxy reported: the commit did
// happen. The UNCERTAIN episode is resolved, as after a human recovery.
func seedCommittedEvidence(t *testing.T, f *executorFixture, candidate daemon.QueueDelivery, id protocol.OpaqueID) {
	t.Helper()
	seedExecutorAttempt(t, f, candidate, id, daemon.AttemptCommitSent)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, f.executor.attempts.MarkUncertain(t.Context(), tx, id, daemon.ReasonTimedOut))
	_, err = tx.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'QUEUED',
		claimed_by = NULL WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	require.NoError(t, err)
	request, _, err := f.controller.EnsureOrchestratorApproval(t.Context(), tx,
		candidate.MessageID, candidate.AgentID, "commit outcome uncertain")
	require.NoError(t, err)
	require.NoError(t, f.executor.attempts.BindUncertainApproval(t.Context(), tx, id, request.ApprovalID))
	require.NoError(t, f.executor.attempts.ResolveUncertain(t.Context(), tx, id, request.ApprovalID))
	_, err = f.controller.ResolveOrchestratorApproval(t.Context(), tx, protocol.ResolveApprovalParams{
		ApprovalID: request.ApprovalID, Option: executorTestDeliverNow, Reason: "recovered by a human",
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	stamp := eventstore.FormatTime(f.clock.Now())
	_, err = tx.ExecContext(t.Context(), `INSERT INTO delivery_result_evidence (
		delivery_id, agent_id, adapter_session_id, result, frame_id, reply_to,
		proxy_observed_at, received_at, observed_draft_generation,
		observed_input_guard_version, writer_lease_id, cancel_reason, control_versions_json
	) VALUES (?, ?, ?, 'COMMITTED', 7, 3, ?, ?, 2, 3, ?, NULL, '{"global":0}')`,
		id, candidate.AgentID, executorTestSession, stamp, stamp, "lease-"+string(id))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// The barrier the fake model checks fails while a write transaction is
// open, so a barrier that passed during a call proves none was.
func TestChoiceBarrierSeesAnOpenWriteTransaction(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = f.barrier.BeginTx(t.Context(), nil)
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	probe, err := f.barrier.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, probe.Rollback())
}

// With every rule and guard passing, the model chooses between
// INJECT_PTY_PROMPT and DEFER with no write transaction open, and its
// INJECT is carried out as a delivery plan whose decision records the
// set, the probabilities, the call, the model, the policy and the state.
func TestAutomaticDecisionAsksTheModelWithNoTransactionOpen(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 1)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	calls := f.model.calls(choiceChoose)
	require.Len(t, calls, 1)
	require.Len(t, f.model.barrierResults(), 1)
	for _, err := range f.model.barrierResults() {
		require.NoError(t, err, "a write transaction was open during the call")
	}
	require.Equal(t, executorTestAgent, calls[0].AgentID)
	require.NoError(t, calls[0].Validate())
	var request protocol.OrchestrationRequest
	require.NoError(t, json.Unmarshal(calls[0].Payload, &request))
	require.ElementsMatch(t, []protocol.OrchestratorAction{choiceInject, choiceDefer}, request.AllowedActions)
	require.Equal(t, protocol.TriageLabelNormal, request.Message.Triage)
	require.Equal(t, protocol.PriorityNormal, request.Message.Priority)
	require.Equal(t, protocol.DecisionOutcomeAllow, request.Channel.Outcome)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, request.State.AgentPhase)
	require.InDelta(t, 0.99, request.State.PIdle, 1e-12)
	require.Equal(t, protocol.AdapterTypeGenericPTY, request.Agent.Adapter)
	require.Empty(t, request.Screen, "no screen source reaches the node")

	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, choiceInject, decision.selected)
	require.Equal(t, []protocol.OrchestratorAction{choiceInject, choiceDefer}, decision.allowed)
	require.Equal(t, map[protocol.OrchestratorAction]float64{choiceInject: 0.8, choiceDefer: 0.2},
		decision.probabilities)
	require.Equal(t, string(choiceCallID), decision.callID)
	require.Equal(t, choiceModelName, decision.model)
	require.Equal(t, defaultPolicyVersion, decision.policy)
	stateID, err := LatestRealStateID(t.Context(), f.store.DB(), executorTestAgent, executorTestSession,
		f.clock.Now())
	require.NoError(t, err)
	require.Equal(t, string(stateID), decision.stateID)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
	// The fixture has no link client: the plan is made, then fails before
	// any Proxy byte and returns the row to the queue.
	require.Equal(t, protocol.ExecutionStatusFailed, decision.status)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 1, f.triager.count())
}

// A model DEFER is carried out as one: a decision that cites the call and
// a human approval with a fixed note.
func TestAutomaticDecisionCarriesOutTheModelsDefer(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.model.set(func(m *choiceModel) { m.action = choiceDefer })
	candidate := f.seedDelivery(t, 2)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, choiceDefer, decision.selected)
	require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
	require.Equal(t, string(choiceCallID), decision.callID)
	require.NotEmpty(t, decision.probabilities)
	require.Equal(t, []string{notePrefix + noteModelDeferred}, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
}

// tx A reads the verdict back from message_triage, the row tx C reads
// again, and decides on that row, not on what the triage returned.
func TestAutomaticDecisionUsesTheStoredVerdict(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 9)
	f.triager.set(func(tr *choiceTriager) {
		tr.label = protocol.TriageLabelRequiresHuman
		tr.returned = &triage.Verdict{
			MessageID: candidate.MessageID, Label: protocol.TriageLabelNormal,
			Outcome: triage.OutcomeClassified, GlimmerCallID: choiceTriageID, CreatedAt: executorTestTime,
		}
	})
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Zero(t, f.model.total())
	require.Equal(t, []string{notePrefix + noteRequiresHuman}, pendingNotes(t, f.store.DB(), candidate.MessageID))
}

// Whenever the rules leave a single action, the model is never called:
// the action is decided and carried out in tx A, as in 5.3c.
func TestAutomaticDecisionWithOneActionNeverAsksTheModel(t *testing.T) {
	t.Parallel()

	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	link := func(mode protocol.LinkMode) func(*testing.T, *choiceFixture, daemon.QueueDelivery) {
		return func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			t.Helper()
			_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
				FromAgent: executorTestSender, ToAgent: executorTestAgent, Mode: mode,
				Plane: protocol.PlaneMessage,
				ExpectedVersions: protocol.ControlVersions{
					"link:" + string(executorTestSender) + "→" + string(executorTestAgent) + ":message": 0,
				},
			}, actor)
			require.NoError(t, err)
		}
	}
	for name, tc := range map[string]struct {
		arrange func(*testing.T, *choiceFixture, daemon.QueueDelivery)
		action  protocol.OrchestratorAction
		note    string
		queue   string
	}{
		"a guard fails": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.setReport(func(r *protocol.InputGuardReport) { r.DraftState = protocol.DraftStateOpen })
			},
			action: choiceDefer, note: "human_draft_closed: human draft is not CLOSED", queue: queueQueued,
		},
		"the state is not eligible": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.states.set(func(s *choiceStates) { s.eligible, s.reason = false, StateEligibilityNotIdle })
			},
			action: choiceDefer, note: "p_idle_threshold: state is not eligible: NOT_IDLE_AT_PROMPT",
			queue: queueQueued,
		},
		"the message requires a human": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.triager.set(func(tr *choiceTriager) { tr.label = protocol.TriageLabelRequiresHuman })
			},
			action: choiceDefer, note: noteRequiresHuman, queue: queueQueued,
		},
		"the message is not classified": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.triager.set(func(tr *choiceTriager) { tr.outcome = triage.OutcomeTimeout })
			},
			action: choiceDefer, note: noteUnclassified, queue: queueQueued,
		},
		"the triage fails": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.triager.set(func(tr *choiceTriager) { tr.err = errors.New("triage store failed") })
			},
			action: choiceDefer, note: noteUnclassified, queue: queueQueued,
		},
		"the channel is off": {
			arrange: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				t.Helper()
				_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
					Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
				}, actor)
				require.NoError(t, err)
			},
			action: choiceDefer, note: noteChannel, queue: queueQueued,
		},
		"the link holds": {
			arrange: link(protocol.LinkModeHold),
			action:  choiceDefer, note: noteChannel, queue: queueQueued,
		},
		"the link requires approval": {
			arrange: link(protocol.LinkModeRequireApproval),
			action:  choiceDefer, note: noteChannel, queue: queueQueued,
		},
		"the delivery expired": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.clock.mu.Lock()
				f.clock.now = f.clock.now.Add(2 * time.Hour)
				f.clock.mu.Unlock()
			},
			action: protocol.OrchestratorActionExpire, queue: queueStateExpired,
		},
		"the delivery already committed": {
			arrange: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
				t.Helper()
				seedCommittedEvidence(t, f.executorFixture, candidate, "dlv-committed")
			},
			action: protocol.OrchestratorActionDropDuplicate, queue: "DELIVERED",
		},
		// The rules apply in order: expiry comes before the duplicate.
		"the delivery expired after it committed": {
			arrange: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
				t.Helper()
				seedCommittedEvidence(t, f.executorFixture, candidate, "dlv-committed")
				f.clock.mu.Lock()
				f.clock.now = f.clock.now.Add(2 * time.Hour)
				f.clock.mu.Unlock()
			},
			action: protocol.OrchestratorActionExpire, queue: queueStateExpired,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			candidate := f.seedDelivery(t, 3)
			tc.arrange(t, f, candidate)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Empty(t, f.model.calls(choiceChoose), "one allowed action never asks the model")
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, []protocol.OrchestratorAction{tc.action}, decision.allowed)
			require.Equal(t, tc.action, decision.selected)
			require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
			require.Empty(t, decision.callID)
			require.Empty(t, decision.probabilities)
			require.Equal(t, tc.queue, queueStateOf(t, f.store.DB(), candidate))
			notes := pendingNotes(t, f.store.DB(), candidate.MessageID)
			if tc.note == "" {
				require.Empty(t, notes)
				return
			}
			require.Len(t, notes, 1)
			require.True(t, strings.HasPrefix(notes[0], notePrefix+tc.note), notes[0])
		})
	}
}

// A delivery with durable COMMITTED evidence is closed as delivered with
// no model call, triage included: the commit already happened. The
// decision says it is a deduplication, no attempt is created or
// rewritten, and a second pass over the closed row writes nothing.
func TestAutomaticDecisionDropsADuplicateWithoutAnyCall(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 4)
	seedCommittedEvidence(t, f.executorFixture, candidate, "dlv-late")
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Zero(t, f.model.total())
	require.Zero(t, f.triager.count(), "a duplicate is never triaged")
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.OrchestratorActionDropDuplicate, decision.selected)
	require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(decision.guards, protocol.GuardNameNotDuplicate))
	require.Contains(t, guardDetail(decision.guards, protocol.GuardNameNotDuplicate), "COMMITTED evidence")
	require.Equal(t, "DELIVERED", queueStateOf(t, f.store.DB(), candidate))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM delivery_attempts WHERE delivery_id = 'dlv-late'").Scan(&state))
	require.Equal(t, string(daemon.AttemptUncertain), state, "the attempt keeps its history")
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Zero(t, countRows(t, f.store.DB(), `SELECT count(*) FROM delivery_attempts
		WHERE orchestrator_decision_id IN (SELECT decision_id FROM orchestrator_decisions
		WHERE selected_action = 'DROP_DUPLICATE')`), "no attempt is tied to the deduplication")

	decisions := len(decisionsFor(t, f.store.DB(), candidate.MessageID))
	require.Error(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Len(t, decisionsFor(t, f.store.DB(), candidate.MessageID), decisions)
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Zero(t, f.model.total())
}

func guardDetail(guards []protocol.GuardResult, name protocol.GuardName) string {
	for _, result := range guards {
		if result.Guard == name {
			return result.Detail
		}
	}
	return ""
}

// The gate let pass a delivery the triage found not deliverable, so it
// has no verdict: only the gate's decision is kept, with no model call,
// no decision and no approval, and the next pass classifies it.
func TestAutomaticDecisionWithoutVerdictKeepsOnlyTheGateDecision(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.triager.set(func(tr *choiceTriager) { tr.absent = true })
	candidate := f.seedDelivery(t, 5)
	gates := gateDecisionCount(t, f.store.DB(), candidate.MessageID)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Zero(t, f.model.total())
	require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, gates+1, gateDecisionCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
}

// With no real state observation yet, nothing is written at all, the
// gate's decision included: the orchestrator never invents a state.
func TestAutomaticDecisionWithoutStateWritesNothing(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.states.set(func(s *choiceStates) { s.err = ErrStateObservationNotFound })
	candidate := f.seedDelivery(t, 6)
	gates := gateDecisionCount(t, f.store.DB(), candidate.MessageID)
	snapshots := countRows(t, f.store.DB(), "SELECT count(*) FROM state_snapshots")
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Empty(t, f.model.calls(choiceChoose))
	require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, gates, gateDecisionCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, snapshots, countRows(t, f.store.DB(), "SELECT count(*) FROM state_snapshots"))
}

// Anything tx A saw that changes while the model thinks makes the choice
// STALE: no attempt, no approval, the row left as it is. Each case
// changes one element of the vector, or breaks a guard, during the call.
func TestAutomaticDecisionGoesStaleWhenWhatTxASawChanges(t *testing.T) {
	t.Parallel()

	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	for name, tc := range map[string]struct {
		change func(*testing.T, *choiceFixture, daemon.QueueDelivery)
		queue  string
	}{
		"the state version": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.states.set(func(s *choiceStates) {
				s.onRead = func(state *StateRead) { state.Snapshot.StateVersion++ }
			})
		}},
		"the state row": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.states.set(func(s *choiceStates) {
				s.onRead = func(state *StateRead) { state.Snapshot.StateID = "01JB000000000000000000STAT" }
			})
		}},
		"the state eligibility": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.states.set(func(s *choiceStates) { s.eligible, s.reason = false, StateEligibilityHumanInput })
		}},
		"the input guard version": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.setReport(func(r *protocol.InputGuardReport) { r.InputGuardVersion++ })
		}},
		"the draft generation": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.setReport(func(r *protocol.InputGuardReport) { r.DraftGeneration++ })
		}},
		"the capabilities, with no lifecycle change": {change: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			t.Helper()
			changeCapabilities(t, f.executorFixture)
		}},
		"a channel control": {change: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			t.Helper()
			_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
				Mode: protocol.ChannelModePaused, ExpectedVersion: 0,
			}, actor)
			require.NoError(t, err)
		}},
		"the queue row": {change: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
			t.Helper()
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED',
				claimed_by = 'human-order' WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID)
			require.NoError(t, err)
		}, queue: "CLAIMED"},
		"a new child process": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.processes.mu.Lock()
			f.processes.tree.Descendants = append(f.processes.tree.Descendants, ProcessToken{PID: 900, StartTime: 901})
			f.processes.mu.Unlock()
		}},
		"late committed evidence": {change: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
			t.Helper()
			seedCommittedEvidence(t, f.executorFixture, candidate, "dlv-raced")
		}},
		"the state, gone": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.states.set(func(s *choiceStates) { s.err = ErrStateObservationNotFound })
		}},
		"the state, no longer valid": {change: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			f.states.set(func(s *choiceStates) { s.err = ErrStateObservationInvalid })
		}},
		"the session lifecycle, with the same session": {change: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
			t.Helper()
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET updated_at = ?
				WHERE session_id = ?`, eventstore.FormatTime(f.clock.Now().Add(time.Second)), executorTestSession)
			require.NoError(t, err)
		}},
		"the triage row, written again with the same verdict": {change: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
			t.Helper()
			db := f.store.DB()
			_, err := db.ExecContext(t.Context(), `DELETE FROM message_triage WHERE message_id = ?`,
				candidate.MessageID)
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), `INSERT INTO message_triage
				(message_id, label, outcome, glimmer_call_id, created_at)
				VALUES (?, 'NORMAL', 'classified', ?, ?)`, candidate.MessageID, string(choiceTriageID),
				eventstore.FormatTime(executorTestTime.Add(time.Second)))
			require.NoError(t, err)
		}},
		"the delivery's expiry, reached": {change: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
			t.Helper()
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET expires_at = ?
				WHERE message_id = ? AND agent_id = ?`, eventstore.FormatTime(f.clock.Now()),
				candidate.MessageID, candidate.AgentID)
			require.NoError(t, err)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			candidate := f.seedDelivery(t, 7)
			var once sync.Once
			f.model.set(func(m *choiceModel) {
				m.during = func(req protocol.InferRequest) {
					if req.Contract == choiceChoose {
						once.Do(func() { tc.change(t, f, candidate) })
					}
				}
			})
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Len(t, f.model.calls(choiceChoose), 1)
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, protocol.ExecutionStatusStale, decision.status)
			require.Equal(t, choiceInject, decision.selected, "the model's choice is kept, not carried out")
			require.Equal(t, string(choiceCallID), decision.callID)
			require.NotEmpty(t, decision.probabilities)
			require.Len(t, decisionsFor(t, f.store.DB(), candidate.MessageID), 1)
			require.Zero(t, decidedAttempts(t, f.store.DB(), candidate.MessageID), "no attempt for the choice")
			require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
			queue := tc.queue
			if queue == "" {
				queue = queueQueued
			}
			require.Equal(t, queue, queueStateOf(t, f.store.DB(), candidate))
		})
	}
}

// changeCapabilities changes the session's capabilities in place, as a
// capability change does: updated_at stays as it was.
func changeCapabilities(t *testing.T, f *executorFixture) {
	t.Helper()
	db := f.store.DB()
	var raw, updated string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT manifest_json, updated_at FROM sessions
		WHERE session_id = ?`, executorTestSession).Scan(&raw, &updated))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(raw), &manifest))
	manifest.Capabilities.MCP = !manifest.Capabilities.MCP
	require.NoError(t, manifest.Validate())
	changed, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET manifest_json = ? WHERE session_id = ?`,
		string(changed), executorTestSession)
	require.NoError(t, err)
	var after string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT updated_at FROM sessions
		WHERE session_id = ?`, executorTestSession).Scan(&after))
	require.Equal(t, updated, after)
}

// A STALE decision sends the delivery back to the gate: the next pass
// gates again and, with nothing changed, carries out the new choice.
func TestAutomaticDecisionReturnsToTheGateAfterStale(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.model.set(func(m *choiceModel) { m.action = choiceDefer })
	candidate := f.seedDelivery(t, 8)
	var once sync.Once
	f.model.set(func(m *choiceModel) {
		m.during = func(req protocol.InferRequest) {
			if req.Contract == choiceChoose {
				once.Do(func() { f.setReport(func(r *protocol.InputGuardReport) { r.InputGuardVersion++ }) })
			}
		}
	})
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	gates := gateDecisionCount(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.ExecutionStatusStale, lastDecision(t, f.store.DB(), candidate.MessageID).status)

	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, gates+1, gateDecisionCount(t, f.store.DB(), candidate.MessageID), "the gate runs again")
	require.Len(t, f.model.calls(choiceChoose), 2)
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
	require.Equal(t, choiceDefer, decision.selected)
	require.Equal(t, []string{notePrefix + noteModelDeferred}, pendingNotes(t, f.store.DB(), candidate.MessageID))
}

// Three STALE decisions in a row for one recipient generation end the
// model's turns: the fourth pass defers to the human without a call.
func TestAutomaticDecisionDefersAfterThreeStaleInARow(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 9)
	f.model.set(func(m *choiceModel) {
		m.during = func(req protocol.InferRequest) {
			if req.Contract == choiceChoose {
				f.setReport(func(r *protocol.InputGuardReport) { r.InputGuardVersion++ })
			}
		}
	})
	for range staleCap {
		require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
		require.Equal(t, protocol.ExecutionStatusStale, lastDecision(t, f.store.DB(), candidate.MessageID).status)
	}
	require.Len(t, f.model.calls(choiceChoose), staleCap)
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))

	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Len(t, f.model.calls(choiceChoose), staleCap, "the capped pass never calls")
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
	require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
	require.Equal(t, []string{notePrefix + noteStale}, pendingNotes(t, f.store.DB(), candidate.MessageID))
}

// Every failure of the call or of its answer fails closed: DEFER with a
// fixed note and an approval, a decision that cites no call, the row left
// QUEUED, no attempt, and no state snapshot made up for it. The model
// can never widen what was allowed.
func TestAutomaticDecisionFailsClosed(t *testing.T) {
	t.Parallel()

	choose := func(label string, probabilities map[string]float64, edit func(*protocol.InferResult)) func(
		protocol.InferRequest,
	) (protocol.InferResult, error) {
		return func(protocol.InferRequest) (protocol.InferResult, error) {
			result := protocol.InferResult{
				Contract: choiceChoose, InferenceProfile: protocol.InferenceProfileBase,
				Label: label, Probabilities: probabilities,
				CallRecord: callRecord(choiceChoose, choiceCallID, probabilities),
			}
			if edit != nil {
				edit(&result)
			}
			return result, nil
		}
	}
	inject, deferred := string(choiceInject), string(choiceDefer)
	both := map[string]float64{inject: 0.8, deferred: 0.2}
	storeManifest := func(t *testing.T, f *choiceFixture, edit func(*protocol.CapabilityManifest)) {
		t.Helper()
		db := f.store.DB()
		var raw string
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT manifest_json FROM sessions
			WHERE session_id = ?`, executorTestSession).Scan(&raw))
		var manifest protocol.CapabilityManifest
		require.NoError(t, json.Unmarshal([]byte(raw), &manifest))
		edit(&manifest)
		changed, err := json.Marshal(manifest)
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), `UPDATE sessions SET manifest_json = ? WHERE session_id = ?`,
			string(changed), executorTestSession)
		require.NoError(t, err)
	}
	for name, tc := range map[string]struct {
		answer func(protocol.InferRequest) (protocol.InferResult, error)
		// unsent cases break the request before it is built.
		unsent func(*testing.T, *choiceFixture)
	}{
		"the call fails": {answer: func(protocol.InferRequest) (protocol.InferResult, error) {
			return protocol.InferResult{}, errors.New("glimmer call timed out")
		}},
		"a label outside the allowed set": {answer: choose("EXPIRE",
			map[string]float64{"EXPIRE": 0.9, deferred: 0.1}, nil)},
		"a native delivery": {answer: choose(string(protocol.OrchestratorActionDeliverNative),
			map[string]float64{string(protocol.OrchestratorActionDeliverNative): 0.9, deferred: 0.1}, nil)},
		"a tie at the top":         {answer: choose(inject, map[string]float64{inject: 0.5, deferred: 0.5}, nil)},
		"a label that is not most": {answer: choose(inject, map[string]float64{inject: 0.3, deferred: 0.7}, nil)},
		"a probability outside the set": {answer: choose(inject,
			map[string]float64{inject: 0.8, "EXPIRE": 0.2}, nil)},
		"a probability above one": {answer: choose(inject,
			map[string]float64{inject: 1.2, deferred: 0.1}, nil)},
		"a probability below zero": {answer: choose(inject,
			map[string]float64{inject: 0.8, deferred: -0.2}, nil)},
		"a probability that is not a number": {answer: choose(inject,
			map[string]float64{inject: math.NaN(), deferred: 0.2}, nil)},
		"no probabilities": {answer: choose(inject, nil, nil)},
		"a failed call record": {answer: choose(inject, both, func(r *protocol.InferResult) {
			r.CallRecord.Status = protocol.GlimmerCallStatusError
		})},
		"no call id": {answer: choose(inject, both, func(r *protocol.InferResult) {
			r.CallRecord.CallID = ""
		})},
		"another contract": {answer: choose(inject, both, func(r *protocol.InferResult) {
			r.Contract = choiceTriage
		})},
		"no request can be built": {unsent: func(t *testing.T, f *choiceFixture) {
			t.Helper()
			_, err := f.store.DB().ExecContext(t.Context(),
				`UPDATE sessions SET manifest_json = '{}' WHERE session_id = ?`, executorTestSession)
			require.NoError(t, err)
		}},
		"a manifest that does not validate": {unsent: func(t *testing.T, f *choiceFixture) {
			t.Helper()
			storeManifest(t, f, func(m *protocol.CapabilityManifest) { m.SchemaVersion = 0 })
		}},
		"a request that does not validate": {unsent: func(_ *testing.T, f *choiceFixture) {
			f.states.set(func(s *choiceStates) {
				s.onRead = func(state *StateRead) { state.IdleProbability = 1.5 }
			})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			if tc.answer != nil {
				f.model.set(func(m *choiceModel) {
					answer := tc.answer
					m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
						if req.Contract == choiceTriage {
							return triageAnswer(protocol.TriageLabelNormal), nil
						}
						return answer(req)
					}
				})
			}
			candidate := f.seedDelivery(t, 10)
			if tc.unsent != nil {
				tc.unsent(t, f)
			}
			snapshots := countRows(t, f.store.DB(), "SELECT count(*) FROM state_snapshots")
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			if tc.unsent != nil {
				require.Empty(t, f.model.calls(choiceChoose), "an invalid request is never sent")
			}
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, choiceDefer, decision.selected)
			require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
			require.Equal(t, []protocol.OrchestratorAction{choiceInject, choiceDefer}, decision.allowed)
			require.Empty(t, decision.callID, "a failure cites no call")
			require.Empty(t, decision.probabilities)
			require.Empty(t, decision.model)
			require.Equal(t, []string{notePrefix + noteModelFailed}, pendingNotes(t, f.store.DB(), candidate.MessageID))
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
			require.Equal(t, snapshots, countRows(t, f.store.DB(), "SELECT count(*) FROM state_snapshots"))
		})
	}
}

// The allowed actions go to the model in a fresh order each call, and a
// choice is read back by name, whatever letter it had.
func TestAutomaticDecisionShufflesTheActionsAndReadsTheChoiceByName(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.executor.entropy = rand.NewChaCha8([32]byte{1, 2, 3})
	f.model.set(func(m *choiceModel) {
		m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
			if req.Contract == choiceTriage {
				return triageAnswer(protocol.TriageLabelNormal), nil
			}
			var request protocol.OrchestrationRequest
			if err := json.Unmarshal(req.Payload, &request); err != nil {
				return protocol.InferResult{}, err
			}
			// The model always answers its first letter, whatever it is.
			return chooseAnswer(request.AllowedActions[0]), nil
		}
	})
	orders := map[protocol.OrchestratorAction]int{}
	for i := range 16 {
		candidate := f.seedDelivery(t, 20+i)
		require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
		calls := f.model.calls(choiceChoose)
		var request protocol.OrchestrationRequest
		require.NoError(t, json.Unmarshal(calls[len(calls)-1].Payload, &request))
		first := request.AllowedActions[0]
		orders[first]++
		require.Equal(t, first, lastDecision(t, f.store.DB(), candidate.MessageID).selected)
	}
	require.Len(t, orders, 2, "both orders occur: %v", orders)
}

// The configuration seam: the automatic decision is wired only with the
// model, the triage, the state reader and the profile together.
func TestExecutorOptionsWireTheAutomaticDecision(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	profile := choiceProfile
	base := ExecutorOptions{
		DB: f.store.DB(), Channel: f.executor.channel, Approvals: f.controller, Links: f.links,
		Clock: f.clock, Model: f.model, Triage: f.triager, States: f.states, StateProfile: &profile,
	}
	wired, err := NewExecutor(base)
	require.NoError(t, err)
	require.True(t, wired.choosing())
	for name, drop := range map[string]func(*ExecutorOptions){
		"no model":   func(o *ExecutorOptions) { o.Model = nil },
		"no triage":  func(o *ExecutorOptions) { o.Triage = nil },
		"no states":  func(o *ExecutorOptions) { o.States = nil },
		"no profile": func(o *ExecutorOptions) { o.StateProfile = nil },
	} {
		opts := base
		drop(&opts)
		executor, err := NewExecutor(opts)
		require.NoError(t, err, name)
		require.False(t, executor.choosing(), name)
	}
}

// Unwired, an automatic evaluation never triages and never calls a model,
// and p_idle_threshold keeps failing as in 5.3c.
func TestAutomaticDecisionUnwiredNeverCallsAModel(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	f.executor.model, f.executor.triager = nil, nil
	candidate := f.seedDelivery(t, 11)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Zero(t, f.model.total())
	require.Zero(t, f.triager.count())
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
	require.Equal(t, "no orchestration model is available",
		guardDetail(decision.guards, protocol.GuardNamePIdleThreshold))
	require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1)
}

// The request tells the model how long the row has been queued.
func TestAutomaticDecisionSendsTheMessageAge(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 14)
	f.clock.mu.Lock()
	f.clock.now = f.clock.now.Add(1500 * time.Millisecond)
	f.clock.mu.Unlock()
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	calls := f.model.calls(choiceChoose)
	require.Len(t, calls, 1)
	var request protocol.OrchestrationRequest
	require.NoError(t, json.Unmarshal(calls[0].Payload, &request))
	require.Equal(t, int64(1500), request.Message.AgeMS)
}

// decision_model is repo@revision only while it fits the column's 128
// characters; past that the choice stands and the model is left out.
func TestAutomaticDecisionBoundsTheDecisionModel(t *testing.T) {
	t.Parallel()

	suffix := "@" + choiceRevision
	for name, tc := range map[string]struct {
		repo  string
		model string
	}{
		"at the bound":   {repo: strings.Repeat("a", maxDecisionModelLen-len(suffix))},
		"past the bound": {repo: strings.Repeat("a", maxDecisionModelLen-len(suffix)+1)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			f.model.set(func(m *choiceModel) {
				m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
					if req.Contract == choiceTriage {
						return triageAnswer(protocol.TriageLabelNormal), nil
					}
					result := chooseAnswer(choiceDefer)
					result.CallRecord.BaseModelRepo = tc.repo
					return result, nil
				}
			})
			candidate := f.seedDelivery(t, 11)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, choiceDefer, decision.selected)
			require.Equal(t, string(choiceCallID), decision.callID)
			if name == "at the bound" {
				require.Equal(t, tc.repo+suffix, decision.model)
				require.Len(t, decision.model, maxDecisionModelLen)
				return
			}
			require.Empty(t, decision.model)
		})
	}
}

// A failed call whose row moved meanwhile is STALE like any other: it
// opens no approval and cites no call.
func TestAutomaticDecisionFailedCallOnAMovedRowIsStale(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 12)
	f.model.set(func(m *choiceModel) {
		m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
			if req.Contract == choiceTriage {
				return triageAnswer(protocol.TriageLabelNormal), nil
			}
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED',
				claimed_by = 'human-order' WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID)
			require.NoError(t, err)
			return protocol.InferResult{}, errors.New("glimmer call timed out")
		}
	})
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.ExecutionStatusStale, decision.status)
	require.Equal(t, choiceDefer, decision.selected)
	require.Empty(t, decision.callID)
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, "CLAIMED", queueStateOf(t, f.store.DB(), candidate))
}

// The STALE run starts with the queue row: STALE decisions about the
// same message made before the row was queued do not count.
func TestAutomaticDecisionCountsTheStaleRunOfThisRowOnly(t *testing.T) {
	t.Parallel()

	f := newChoiceFixture(t)
	candidate := f.seedDelivery(t, 13)
	stateID, err := LatestRealStateID(t.Context(), f.store.DB(), executorTestAgent, executorTestSession,
		f.clock.Now())
	require.NoError(t, err)
	before := eventstore.FormatTime(f.clock.Now().Add(-time.Second))
	for i := range staleCap {
		_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO orchestrator_decisions (
			decision_id, agent_id, message_id, artifact_id, state_id, allowed_actions_json,
			selected_action, probabilities_json, hard_guards_json, policy_version, decision_model,
			glimmer_call_id, channel_decision_id, created_at, execution_status
		) VALUES (?, ?, ?, NULL, ?, '["INJECT_PTY_PROMPT","DEFER"]', 'INJECT_PTY_PROMPT', NULL, '[]',
			?, NULL, NULL, ?, ?, 'STALE')`,
			fmt.Sprintf("01JB0000000000000000000ST%d", i), executorTestAgent, candidate.MessageID,
			stateID, defaultPolicyVersion, "chd_01ARZ3NDEKTSV4RRFFQ69G5FAV", before)
		require.NoError(t, err)
	}
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Len(t, f.model.calls(choiceChoose), 1, "an earlier row's STALE run does not cap this one")
}

// seedReadableDelivery queues a NORMAL message from the sender to the
// fixture's agent, with the profile-none envelope the hub stores, whose
// body is body. Only the envelope holds the text.
func seedReadableDelivery(t *testing.T, f *executorFixture, sequence int, body string) daemon.QueueDelivery {
	t.Helper()
	db := f.store.DB()
	id := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	created := f.clock.Now()
	expires := created.Add(time.Hour)
	client := protocol.OpaqueID(fmt.Sprintf("client-%d", sequence))
	threadRef := protocol.ThreadRef(fmt.Sprintf("thr_01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	thread := protocol.ThreadID(fmt.Sprintf("thread-%d", sequence))
	digest := protocol.Digest("sha256:" + strings.Repeat("0", 64))
	recipients := []protocol.AgentID{executorTestAgent}
	logical := protocol.LogicalMessage{
		MessageID: id, ClientMsgID: client, ThreadID: thread, FromAgent: executorTestSender,
		ToAgents: recipients, OriginNode: executorTestNode, DestinationNode: executorTestNode,
		Subject: "parser review", Body: body, Priority: protocol.PriorityNormal,
		DeliveryMode: protocol.DeliveryModeWhenSafe, CreatedAt: created, ExpiresAt: expires,
		Sensitivity: protocol.MessageSensitivityInternal, ThreadSequence: 1, RouteDigest: digest,
	}
	require.NoError(t, logical.Validate())
	plain, err := json.Marshal(logical)
	require.NoError(t, err)
	sum := sha256.Sum256(plain)
	wire := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion, MessageID: id,
		ClientMsgID: client, OriginNode: executorTestNode, OriginAgent: executorTestSender,
		DestinationNode: executorTestNode, RecipientAgents: recipients, CreatedAt: created,
		ExpiresAt: expires, Priority: protocol.PriorityNormal, ThreadRef: threadRef, RouteDigest: digest,
		ContentType: protocol.WireContentType, EncryptionProfile: protocol.EncryptionProfileNone,
		SenderSigningKeyID: "ed25519:agent-sender:v1", RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext: plain, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(sum[:])),
		Signature: make([]byte, 64),
	}
	require.NoError(t, wire.Validate())
	envelope, err := json.Marshal(wire)
	require.NoError(t, err)
	stamp, until := eventstore.FormatTime(created), eventstore.FormatTime(expires)
	_, err = db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'PENDING', ?, ?, ?, 0, ?)`,
		id, client, executorTestNode, executorTestSender, executorTestNode, threadRef, thread,
		stamp, until, stamp, digest)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO message_envelopes
		(message_id, envelope, protocol, version, client_msg_id, origin_node,
		 origin_agent, destination_node, recipient_agents_json, created_at,
		 expires_at, priority, thread_ref, encryption_profile,
		 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
		 route_digest, wire_control_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NORMAL', ?, 'none', ?, ?, ?, 0, ?, 0)`,
		id, envelope, protocol.WireProtocol, protocol.WireVersion, client, executorTestNode,
		executorTestSender, executorTestNode, `["`+string(executorTestAgent)+`"]`, stamp, until,
		threadRef, wire.SenderSigningKeyID, wire.CiphertextSHA256, stamp, digest)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'QUEUED', 2, ?, ?, ?, 0, ?)`,
		id, executorTestAgent, executorTestSender, threadRef, stamp, until, stamp, digest)
	require.NoError(t, err)
	return daemon.QueueDelivery{MessageID: id, AgentID: executorTestAgent, DeliveryGeneration: 1}
}

// useRealTriage replaces the fake triage with internal/triage over the
// real channel's read-only preview and the fixture's model.
func useRealTriage(t *testing.T, f *choiceFixture) {
	t.Helper()
	ch, err := triage.NewChannel(f.executor.channel)
	require.NoError(t, err)
	tr, err := triage.New(f.store.DB(), f.model, ch, f.clock)
	require.NoError(t, err)
	f.executor.triager = tr
}

// requireNoCanary checks every value of every table but the envelopes.
func requireNoCanary(t *testing.T, db *sql.DB, canaries ...string) {
	t.Helper()
	tables := tableNames(t, db)
	require.Contains(t, tables, "orchestrator_decisions")
	for _, table := range tables {
		if table != "message_envelopes" {
			requireTableWithout(t, db, table, canaries)
		}
	}
}

func tableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	return tables
}

func requireTableWithout(t *testing.T, db *sql.DB, table string, canaries []string) {
	t.Helper()
	values, err := db.QueryContext(t.Context(), `SELECT * FROM "`+table+`"`)
	require.NoError(t, err)
	defer func() { _ = values.Close() }()
	columns, err := values.Columns()
	require.NoError(t, err)
	cells := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range cells {
		pointers[i] = &cells[i]
	}
	for values.Next() {
		require.NoError(t, values.Scan(pointers...))
		for i, cell := range cells {
			text := fmt.Sprint(cell)
			if raw, ok := cell.([]byte); ok {
				text = string(raw)
			}
			for _, canary := range canaries {
				require.NotContains(t, text, canary, "%s.%s", table, columns[i])
			}
		}
	}
	require.NoError(t, values.Err())
}

// The regression of 5.9: once the channel is OFF or HOLD, a delivery that
// was queued while it allowed reaches the model transport zero times,
// TRIAGE included, and nothing is delivered. Only tx A's gate decides;
// the triage's read-only preview decides and spends nothing.
func TestAutomaticDecisionOffOrHoldReachesTheModelZeroTimes(t *testing.T) {
	t.Parallel()

	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	for name, restrict := range map[string]func(*testing.T, *choiceFixture){
		"global off": func(t *testing.T, f *choiceFixture) {
			t.Helper()
			_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
				Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
			}, actor)
			require.NoError(t, err)
		},
		"link hold": func(t *testing.T, f *choiceFixture) {
			t.Helper()
			_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
				FromAgent: executorTestSender, ToAgent: executorTestAgent, Mode: protocol.LinkModeHold,
				Plane: protocol.PlaneMessage,
				ExpectedVersions: protocol.ControlVersions{
					"link:" + string(executorTestSender) + "→" + string(executorTestAgent) + ":message": 0,
				},
			}, actor)
			require.NoError(t, err)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			useRealTriage(t, f)
			candidate := seedReadableDelivery(t, f.executorFixture, 50, "please review the parser change")
			restrict(t, f)
			gates := gateDecisionCount(t, f.store.DB(), candidate.MessageID)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Zero(t, f.model.total(), "a non-ALLOW object reaches the model transport zero times")
			require.Zero(t, countRows(t, f.store.DB(), "SELECT count(*) FROM message_triage"))
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
			require.Equal(t, gates+1, gateDecisionCount(t, f.store.DB(), candidate.MessageID),
				"only tx A's gate decided")
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
			notes := pendingNotes(t, f.store.DB(), candidate.MessageID)
			require.Len(t, notes, 1)
			require.True(t, strings.HasPrefix(notes[0], notePrefix+noteChannel), notes[0])
		})
	}
}

// The body reaches the model only inside its TRIAGE request; neither the
// body nor a reason the model gives reaches the CHOOSE request, a table
// other than the envelopes, an approval note or an error, on every path.
func TestAutomaticDecisionKeepsTheCanariesOut(t *testing.T) {
	t.Parallel()

	const body = "CANARY-BODY-7f3a1c"
	const reason = "CANARY-REASON-91c2e4"
	withReason := func(action protocol.OrchestratorAction) protocol.InferResult {
		result := chooseAnswer(action)
		result.Evidence = []string{reason}
		return result
	}
	for name, tc := range map[string]struct {
		choose func() (protocol.InferResult, error)
		during func(*choiceFixture)
		status protocol.ExecutionStatus
	}{
		"inject": {
			choose: func() (protocol.InferResult, error) { return withReason(choiceInject), nil },
			status: protocol.ExecutionStatusFailed,
		},
		"defer": {
			choose: func() (protocol.InferResult, error) { return withReason(choiceDefer), nil },
			status: protocol.ExecutionStatusExecuted,
		},
		"failure": {
			choose: func() (protocol.InferResult, error) {
				return protocol.InferResult{}, errors.New("model said " + reason + " about " + body)
			},
			status: protocol.ExecutionStatusExecuted,
		},
		"stale": {
			choose: func() (protocol.InferResult, error) { return withReason(choiceInject), nil },
			during: func(f *choiceFixture) {
				f.setReport(func(r *protocol.InputGuardReport) { r.InputGuardVersion++ })
			},
			status: protocol.ExecutionStatusStale,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			useRealTriage(t, f)
			f.model.set(func(m *choiceModel) {
				m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
					if req.Contract == choiceTriage {
						result := triageAnswer(protocol.TriageLabelNormal)
						result.Evidence = []string{reason}
						return result, nil
					}
					if tc.during != nil {
						tc.during(f)
					}
					return tc.choose()
				}
			})
			candidate := seedReadableDelivery(t, f.executorFixture, 60, "Please look at this. "+body)
			err := f.executor.EvaluateAutomatic(t.Context(), candidate)
			require.NoError(t, err)

			triaged := f.model.calls(choiceTriage)
			require.Len(t, triaged, 1)
			require.Contains(t, string(triaged[0].Payload), body, "the body reaches the TRIAGE call")
			chosen := f.model.calls(choiceChoose)
			require.Len(t, chosen, 1)
			require.NotContains(t, string(chosen[0].Payload), body)
			require.NotContains(t, string(chosen[0].Payload), reason)
			require.Equal(t, tc.status, lastDecision(t, f.store.DB(), candidate.MessageID).status)
			for _, note := range pendingNotes(t, f.store.DB(), candidate.MessageID) {
				require.NotContains(t, note, body)
				require.NotContains(t, note, reason)
			}
			requireNoCanary(t, f.store.DB(), body, reason)
		})
	}
}
