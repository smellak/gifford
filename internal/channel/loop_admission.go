package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

type replyParent struct {
	messageID         protocol.ULID
	threadID          protocol.ThreadID
	fromAgent         protocol.AgentID
	recipients        []protocol.AgentID
	parentID          protocol.ULID
	loopID            protocol.LoopID
	loopGeneration    int64
	turnIndex         int64
	requestedAutonomy protocol.Autonomy
	requestedBudget   *protocol.RequestedBudget
	threadSequence    int64
	sequenceKnown     bool
}

type replyLoopPlan struct {
	loop               *protocol.Loop
	opening            bool
	expectedGeneration int64
}

// AdmitReply evaluates and reserves one local reply. Parent and loop metadata
// are loaded exclusively from durable rows; callers cannot supply a loop.
// appendEvents writes the returned loop events through the caller's existing
// transaction before this method returns.
func (e *Engine) AdmitReply(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	store func(context.Context, protocol.AdmissionResult) error,
	appendEvents func(context.Context, []ControlEvent) error,
) (protocol.AdmissionResult, error) {
	if tx == nil || store == nil || appendEvents == nil {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: transaction or store", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return protocol.AdmissionResult{}, err
	}
	if m.ParentID == "" {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: reply parent", ErrInvalidQuery)
	}
	if err := ensureNewAdmission(ctx, tx, m.ID); err != nil {
		return protocol.AdmissionResult{}, err
	}
	parent, err := selectReplyParent(ctx, tx, m.ParentID)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	if parent.threadID != m.ThreadID {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: reply thread differs from parent", ErrInvalidQuery)
	}
	if len(m.To) != 1 || m.To[0].Agent != parent.fromAgent ||
		!slices.Contains(parent.recipients, m.From) {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: reply does not reverse parent route", ErrInvalidQuery)
	}
	plan, err := resolveReplyLoop(ctx, tx, parent)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	if plan.loop != nil && (!slices.Contains(plan.loop.Participants, m.From) ||
		!slices.Contains(plan.loop.Participants, parent.fromAgent)) {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: reply endpoints are outside the active loop", ErrInvalidQuery)
	}
	m.loop = plan.loop
	m.openingLoop = plan.opening

	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	now := e.clock.Now().UTC()
	stage := protocol.DecisionStageContinuation
	if plan.opening {
		stage = protocol.DecisionStageAdmission
	}
	evaluated, err := evaluateRecipients(v, m, stage, now)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	var planned *protocol.Loop
	if plan.opening {
		loop, planErr := e.planImplicitLoop(m, parent, plan.expectedGeneration, evaluated[0].eff, now)
		if planErr != nil {
			return protocol.AdmissionResult{}, planErr
		}
		planned = &loop
		m.loop = planned
		evaluated, err = evaluateRecipients(v, m, stage, now)
		if err != nil {
			return protocol.AdmissionResult{}, err
		}
	}
	governing, err := e.prepareDecisionsWithApproval(evaluated, now, true)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return protocol.AdmissionResult{}, err
	}

	var events []ControlEvent
	if governing.Outcome() == Deny {
		events, err = e.replyDenialEvents(ctx, tx, m, evaluated[0].query, governing, now)
		if err != nil {
			return protocol.AdmissionResult{}, err
		}
		if err := appendEvents(ctx, events); err != nil {
			return protocol.AdmissionResult{}, fmt.Errorf("append loop events: %w", err)
		}
		return protocol.AdmissionResult{}, &DenyError{
			decisionID: governing.ID(),
			reason:     governing.Reason(),
		}
	}

	var loop protocol.Loop
	if plan.opening {
		loop = *planned
		if err = createLoopCAS(ctx, tx, loop, plan.expectedGeneration); err != nil {
			return protocol.AdmissionResult{}, err
		}
		events = append(events, loopCreatedEvent(loop))
		m.turnIndex = 1
	} else {
		loop, events, err = e.reserveReply(ctx, tx, &m, governing, now)
		if err != nil {
			return protocol.AdmissionResult{}, err
		}
	}
	m.loop = &loop

	var approvalID protocol.ApprovalID
	if governing.Outcome() == Approval {
		switch governing.Reason() {
		case protocol.DecisionReasonLoopStepTokenRequired,
			protocol.DecisionReasonAgentAutoContinueOff:
			approval, _, approvalErr := e.ensurePendingApproval(ctx, tx, pendingApproval{
				source: protocol.ApprovalSourceLoopStep, objectType: protocol.ApprovalObjectTypeMessage,
				objectID: string(m.ID), agentID: m.From, loopID: loop.LoopID,
				options: []protocol.ApprovalOption{"steps:1", "stop"},
			}, now)
			if approvalErr != nil {
				return protocol.AdmissionResult{}, approvalErr
			}
			approvalID = approval.ApprovalID
		default:
			return protocol.AdmissionResult{}, ErrApprovalUnavailable
		}
	}
	if err := appendEvents(ctx, events); err != nil {
		return protocol.AdmissionResult{}, fmt.Errorf("append loop events: %w", err)
	}
	result := admissionResult(governing, m, epoch)
	result.LoopID = loop.LoopID
	result.LoopGeneration = int64Value(loop.LoopGeneration)
	result.TurnIndex = int64Value(m.turnIndex)
	result.ApprovalRequestID = approvalID
	if err := result.Validate(); err != nil {
		return protocol.AdmissionResult{}, fmt.Errorf("reply admission result: %w", err)
	}
	if err := store(ctx, result); err != nil {
		return protocol.AdmissionResult{}, fmt.Errorf("%w: %s", ErrStore, err.Error())
	}
	if err := setMessageControl(ctx, tx, m.ID, tripleOf(governing, epoch)); err != nil {
		return protocol.AdmissionResult{}, err
	}
	return result, nil
}

func (e *Engine) replyDenialEvents(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	query Query,
	governing Decision,
	now time.Time,
) ([]ControlEvent, error) {
	if m.loop == nil || governing.Reason() != protocol.DecisionReasonLoopBudgetExhausted {
		return nil, nil
	}
	dimension := hardLimitDimension(query, m.loop, now)
	if dimension == "" {
		return nil, nil
	}
	return e.pauseLoopForBudget(ctx, tx, *m.loop, dimension, now)
}

func selectReplyParent(ctx context.Context, tx *sql.Tx, id protocol.ULID) (replyParent, error) {
	query, err := queryText("messages_select_reply_parent.sql")
	if err != nil {
		return replyParent{}, err
	}
	var parent replyParent
	var parentID, loopID, autonomy, budgetJSON sql.NullString
	var generation, turn, sequence sql.NullInt64
	err = tx.QueryRowContext(ctx, query, id).Scan(
		&parent.threadID, &parent.fromAgent, &parentID, &loopID, &generation, &turn,
		&autonomy, &budgetJSON, &sequence,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return replyParent{}, fmt.Errorf("%w: reply parent not found", ErrInvalidQuery)
	}
	if err != nil {
		return replyParent{}, fmt.Errorf("select reply parent: %w", err)
	}
	parent.messageID = id
	parent.parentID = protocol.ULID(parentID.String)
	parent.loopID = protocol.LoopID(loopID.String)
	parent.requestedAutonomy = protocol.Autonomy(autonomy.String)
	parent.sequenceKnown = sequence.Valid
	parent.threadSequence = sequence.Int64
	if generation.Valid {
		parent.loopGeneration = generation.Int64
	}
	if turn.Valid {
		parent.turnIndex = turn.Int64
	}
	if !parent.threadID.Valid() || !parent.fromAgent.Valid() ||
		parent.parentID != "" && !parent.parentID.Valid() ||
		parent.requestedAutonomy != "" && !parent.requestedAutonomy.Valid() ||
		parent.sequenceKnown && parent.threadSequence < 1 {
		return replyParent{}, fmt.Errorf("%w: invalid reply parent metadata", ErrInvalidLoop)
	}
	loopFields := 0
	if parent.loopID != "" {
		loopFields++
	}
	if generation.Valid {
		loopFields++
	}
	if turn.Valid {
		loopFields++
	}
	if loopFields != 0 && loopFields != 3 ||
		loopFields == 3 && (!parent.loopID.Valid() || parent.loopGeneration < 1 || parent.turnIndex < 1) {
		return replyParent{}, fmt.Errorf("%w: incoherent parent loop fields", ErrInvalidLoop)
	}
	if budgetJSON.Valid {
		parent.requestedBudget = &protocol.RequestedBudget{}
		if err := decodeJSON(budgetJSON.String, parent.requestedBudget); err != nil || parent.requestedBudget.Validate() != nil {
			return replyParent{}, fmt.Errorf("%w: invalid parent requested budget", ErrInvalidLoop)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_id FROM message_recipients
		WHERE message_id = ? ORDER BY ordinal`, id)
	if err != nil {
		return replyParent{}, fmt.Errorf("select reply parent recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	seen := make(map[protocol.AgentID]bool)
	for rows.Next() {
		var recipient protocol.AgentID
		if err := rows.Scan(&recipient); err != nil {
			return replyParent{}, fmt.Errorf("scan reply parent recipient: %w", err)
		}
		if !recipient.Valid() || recipient == parent.fromAgent || seen[recipient] {
			return replyParent{}, fmt.Errorf("%w: invalid parent recipient", ErrInvalidLoop)
		}
		seen[recipient] = true
		parent.recipients = append(parent.recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return replyParent{}, fmt.Errorf("read reply parent recipients: %w", err)
	}
	if len(parent.recipients) == 0 {
		return replyParent{}, fmt.Errorf("%w: parent has no recipients", ErrInvalidLoop)
	}
	return parent, nil
}

// bindMessageLoopState replaces caller-supplied loop state with the durable
// binding on the admitted message and the current owner-loop row. It also
// projects the object's held/pending state for delivery evaluation.
func bindMessageLoopState(ctx context.Context, tx *sql.Tx, m *Message) error {
	query, err := queryText("messages_select_loop_binding.sql")
	if err != nil {
		return err
	}
	var threadID, parentID, loopID sql.NullString
	var generation, turn sql.NullInt64
	var controlState protocol.ControlState
	var pending bool
	err = tx.QueryRowContext(ctx, query, m.ID).Scan(
		&threadID, &parentID, &loopID, &generation, &turn, &controlState, &pending,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: message loop binding not found", ErrInvalidQuery)
	}
	if err != nil {
		return fmt.Errorf("select message loop binding: %w", err)
	}
	if !controlState.Valid() || protocol.ThreadID(threadID.String) != m.ThreadID ||
		protocol.ULID(parentID.String) != m.ParentID {
		return fmt.Errorf("%w: message loop binding differs", ErrInvalidControl)
	}
	fields := 0
	if loopID.Valid {
		fields++
	}
	if generation.Valid {
		fields++
	}
	if turn.Valid {
		fields++
	}
	if fields != 0 && fields != 3 || m.ParentID == "" && fields != 0 ||
		m.ParentID != "" && fields != 3 {
		return fmt.Errorf("%w: incoherent message loop binding", ErrInvalidLoop)
	}
	m.loop = nil
	m.turnIndex = 0
	if fields == 3 {
		id := protocol.LoopID(loopID.String)
		if !id.Valid() || generation.Int64 < 1 || turn.Int64 < 1 {
			return fmt.Errorf("%w: invalid message loop binding", ErrInvalidLoop)
		}
		loop, found, err := selectLoopByID(ctx, tx, id)
		if err != nil {
			return err
		}
		if !found || loop.ThreadID != m.ThreadID || loop.LoopGeneration != generation.Int64 {
			return fmt.Errorf("%w: missing message loop generation", ErrInvalidLoop)
		}
		m.loop = &loop
		m.turnIndex = turn.Int64
	}
	m.objectHeld = controlState == protocol.ControlStateHeld
	m.approvalPending = pending
	return nil
}

func resolveReplyLoop(ctx context.Context, tx *sql.Tx, parent replyParent) (replyLoopPlan, error) {
	if parent.loopID != "" {
		loop, found, err := selectLoopByID(ctx, tx, parent.loopID)
		if err != nil {
			return replyLoopPlan{}, err
		}
		if !found || loop.LoopGeneration != parent.loopGeneration || loop.ThreadID != parent.threadID {
			return replyLoopPlan{}, fmt.Errorf("%w: parent loop generation", ErrInvalidLoop)
		}
		return replyLoopPlan{loop: &loop}, nil
	}
	if parent.parentID != "" {
		return replyLoopPlan{}, fmt.Errorf("%w: only a root without a loop may open one", ErrInvalidLoop)
	}
	bound, found, err := selectLoopByRootMessage(ctx, tx, parent.threadID, parent.messageID)
	if err != nil {
		return replyLoopPlan{}, err
	}
	if found {
		return replyLoopPlan{loop: &bound}, nil
	}
	if parent.sequenceKnown {
		closed, closedFound, closedErr := selectClosedLoopByThreadSequence(
			ctx, tx, parent.threadID, parent.threadSequence,
		)
		if closedErr != nil {
			return replyLoopPlan{}, closedErr
		}
		if closedFound {
			return replyLoopPlan{loop: &closed}, nil
		}
	} else {
		_, closedHistory, historyErr := selectLatestTerminalLoopByThread(ctx, tx, parent.threadID)
		if historyErr != nil {
			return replyLoopPlan{}, historyErr
		}
		if closedHistory {
			return replyLoopPlan{}, fmt.Errorf(
				"%w: root without thread_sequence crosses a closed loop boundary",
				ErrInvalidQuery,
			)
		}
	}
	active, found, err := selectActiveLoopByThread(ctx, tx, parent.threadID)
	if err != nil {
		return replyLoopPlan{}, err
	}
	if found {
		return replyLoopPlan{loop: &active}, nil
	}
	latest, found, err := selectLatestLoopByThread(ctx, tx, parent.threadID)
	if err != nil {
		return replyLoopPlan{}, err
	}
	if !found {
		return replyLoopPlan{opening: true}, nil
	}
	if !terminalLoopMode(latest.Mode) {
		return replyLoopPlan{}, fmt.Errorf("%w: latest loop is nonterminal but not active", ErrInvalidLoop)
	}
	closedAt, known, err := selectLoopClosureSequence(ctx, tx, latest.LoopID)
	if err != nil {
		return replyLoopPlan{}, err
	}
	if known && parent.sequenceKnown && parent.threadSequence > closedAt {
		return replyLoopPlan{opening: true, expectedGeneration: latest.LoopGeneration}, nil
	}
	// Bind a delayed old-root reply to the exact terminal generation so the
	// normal continuation decision records LOOP_STOPPED without resurrection.
	return replyLoopPlan{loop: &latest}, nil
}

func (e *Engine) planImplicitLoop(
	m Message,
	parent replyParent,
	expectedGeneration int64,
	effective Effective,
	now time.Time,
) (protocol.Loop, error) {
	budget, profile, err := e.budgets.Effective(e.profile, parent.requestedBudget)
	if err != nil {
		return protocol.Loop{}, err
	}
	mode := protocol.LoopModeAuto
	if parent.requestedAutonomy == protocol.AutonomyStep ||
		effective.Sender.AgentID != "" && !effective.Sender.AutoContinue {
		mode = protocol.LoopModeStep
	}
	id, err := e.mintLoopID(now)
	if err != nil {
		return protocol.Loop{}, err
	}
	participants := []protocol.AgentID{parent.fromAgent}
	for _, recipient := range parent.recipients {
		if !slices.Contains(participants, recipient) {
			participants = append(participants, recipient)
		}
	}
	if !slices.Contains(participants, m.From) {
		return protocol.Loop{}, fmt.Errorf("%w: reply sender is not a root participant", ErrInvalidLoop)
	}
	if len(participants) > 32 {
		return protocol.Loop{}, fmt.Errorf("%w: implicit loop has too many participants", ErrInvalidQuery)
	}
	loop := protocol.Loop{
		LoopID: id, ThreadID: m.ThreadID, LoopGeneration: expectedGeneration + 1,
		OwnerNode: m.FromNode, Origin: protocol.LoopOriginImplicit,
		Participants: participants, Mode: mode, EffectiveMode: mode,
		BudgetProfile: profile, RootMessageID: parent.messageID, Budget: budget,
		Counters:        protocol.LoopCounters{TurnCount: 1, MessageCount: 1, HopCount: m.HopCount},
		LastSenderAgent: m.From, ControlVersion: 1,
		StartedAt: now, ExpiresAt: now.Add(time.Duration(budget.MaxDurationMS) * time.Millisecond),
		LastProgressAt: now, CreatedBy: string(m.FromNode), UpdatedAt: now,
	}
	return loop, nil
}

func (e *Engine) reserveReply(
	ctx context.Context,
	tx *sql.Tx,
	m *Message,
	governing Decision,
	now time.Time,
) (protocol.Loop, []ControlEvent, error) {
	current := *m.loop
	next := current
	opens := opensTurn(Query{From: m.From}, &current)
	turn := current.Counters.TurnCount
	if opens {
		turn++
	}
	if turn < 1 || current.Counters.MessageCount == math.MaxInt64 {
		return protocol.Loop{}, nil, fmt.Errorf("%w: loop counter overflow", ErrInvalidLoop)
	}
	next.Counters.MessageCount++
	if opens {
		next.Counters.TurnCount++
	}
	if m.HopCount > next.Counters.HopCount {
		next.Counters.HopCount = m.HopCount
	}
	next.LastSenderAgent = m.From
	next.LastProgressAt = now
	next.UpdatedAt = now
	if next.ControlVersion == math.MaxInt64 {
		return protocol.Loop{}, nil, fmt.Errorf("%w: loop version overflow", ErrInvalidLoop)
	}
	next.ControlVersion++
	events := []ControlEvent{}
	softExhausted := governing.Outcome() == Hold &&
		governing.Reason() == protocol.DecisionReasonLoopBudgetExhausted &&
		current.Mode == protocol.LoopModeAuto && opens &&
		current.Counters.TurnCount >= current.Budget.MaxTurns
	if softExhausted {
		next.Mode = protocol.LoopModePaused
		next.EffectiveMode = protocol.LoopModePaused
		next.StateReason = protocol.LoopStateReasonBudgetExhausted
	}
	if err := updateLoopCAS(ctx, tx, next, current.ControlVersion); err != nil {
		return protocol.Loop{}, nil, err
	}
	m.turnIndex = turn
	if softExhausted {
		if _, _, err := e.ensureLoopBudgetApproval(ctx, tx, next, now); err != nil {
			return protocol.Loop{}, nil, err
		}
		events = append(events, loopBudgetEvent(next, protocol.BudgetDimensionMaxTurns))
	}
	return next, events, nil
}

func (e *Engine) pauseLoopForBudget(
	ctx context.Context,
	tx *sql.Tx,
	current protocol.Loop,
	dimension protocol.BudgetDimension,
	now time.Time,
) ([]ControlEvent, error) {
	if terminalLoopMode(current.Mode) || current.Mode == protocol.LoopModeDraining {
		return nil, nil
	}
	if current.Mode == protocol.LoopModePaused && current.StateReason == protocol.LoopStateReasonBudgetExhausted {
		_, _, err := e.ensureLoopBudgetApproval(ctx, tx, current, now)
		return nil, err
	}
	if current.ControlVersion == math.MaxInt64 {
		return nil, fmt.Errorf("%w: loop version overflow", ErrInvalidLoop)
	}
	next := current
	next.Mode = protocol.LoopModePaused
	next.EffectiveMode = protocol.LoopModePaused
	next.StateReason = protocol.LoopStateReasonBudgetExhausted
	next.ControlVersion++
	next.UpdatedAt = now
	if err := updateLoopCAS(ctx, tx, next, current.ControlVersion); err != nil {
		return nil, err
	}
	if _, _, err := e.ensureLoopBudgetApproval(ctx, tx, next, now); err != nil {
		return nil, err
	}
	return []ControlEvent{loopBudgetEvent(next, dimension)}, nil
}

type pendingApproval struct {
	source     protocol.ApprovalSource
	objectType protocol.ApprovalObjectType
	objectID   string
	agentID    protocol.AgentID
	loopID     protocol.LoopID
	options    []protocol.ApprovalOption
}

func (e *Engine) ensureLoopBudgetApproval(
	ctx context.Context,
	tx *sql.Tx,
	loop protocol.Loop,
	now time.Time,
) (protocol.ApprovalRequest, bool, error) {
	return e.ensurePendingApproval(ctx, tx, pendingApproval{
		source: protocol.ApprovalSourceLoopBudget, objectType: protocol.ApprovalObjectTypeLoop,
		objectID: string(loop.LoopID), loopID: loop.LoopID,
		options: []protocol.ApprovalOption{"extend:+5", "set_mode:step", "stop"},
	}, now)
}

func (e *Engine) ensurePendingApproval(
	ctx context.Context,
	tx *sql.Tx,
	want pendingApproval,
	now time.Time,
) (protocol.ApprovalRequest, bool, error) {
	query, err := queryText("approvals_select_pending_object.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, false, err
	}
	existing, err := scanApproval(tx.QueryRowContext(ctx, query, want.objectType, want.objectID, want.source))
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return protocol.ApprovalRequest{}, false, err
	}
	id, err := e.mintApprovalID(now)
	if err != nil {
		return protocol.ApprovalRequest{}, false, err
	}
	request := protocol.ApprovalRequest{
		ApprovalID: id, Source: want.source, ObjectType: want.objectType,
		ObjectID: want.objectID, AgentID: want.agentID, LoopID: want.loopID,
		Options: slices.Clone(want.options), State: protocol.ApprovalStatePending, CreatedAt: now,
	}
	if err := request.Validate(); err != nil {
		return protocol.ApprovalRequest{}, false, fmt.Errorf("build loop approval: %w", err)
	}
	options, err := json.Marshal(request.Options)
	if err != nil {
		return protocol.ApprovalRequest{}, false, fmt.Errorf("encode loop approval options: %w", err)
	}
	insert, err := queryText("approvals_insert.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, false, err
	}
	_, err = tx.ExecContext(ctx, insert,
		request.ApprovalID, request.Source, request.ObjectType, request.ObjectID,
		nullString(string(request.AgentID)), nullString(string(request.LoopID)), string(options),
		nil, nil, request.State, nil, nil, nil, formatChannelTime(now), nil)
	if err != nil {
		return protocol.ApprovalRequest{}, false, fmt.Errorf("insert loop approval: %w", err)
	}
	return request, true, nil
}

func loopCreatedEvent(loop protocol.Loop) ControlEvent {
	version := loop.ControlVersion
	generation := loop.LoopGeneration
	return ControlEvent{Type: protocol.EventTypeLoopCreated, Payload: protocol.ChannelEventPayload{
		ThreadID: loop.ThreadID, LoopID: loop.LoopID, LoopGeneration: &generation,
		LoopOrigin: loop.Origin, LoopMode: loop.Mode, Version: &version,
		ActorType: protocol.ActorTypeSystem, ActorID: loop.CreatedBy,
	}}
}

func loopBudgetEvent(loop protocol.Loop, dimension protocol.BudgetDimension) ControlEvent {
	version := loop.ControlVersion
	return ControlEvent{Type: protocol.EventTypeLoopBudgetExhausted, Payload: protocol.ChannelEventPayload{
		LoopID: loop.LoopID, BudgetDimension: dimension, Version: &version,
	}}
}

func int64Value(value int64) *int64 { return &value }
