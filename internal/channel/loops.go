package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// loopStore is the transaction-scoped store used by loop mutations. A
// *sql.Tx, *sql.Conn or *sql.DB satisfies it; callers that need atomicity pass
// the transaction that also owns admission.
type loopStore interface {
	Queryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Loop returns one governed communication loop.
func (c *Controller) Loop(ctx context.Context, id protocol.LoopID) (protocol.Loop, error) {
	if !id.Valid() {
		return protocol.Loop{}, fmt.Errorf("get loop: %w", ErrInvalidQuery)
	}
	loop, found, err := selectLoopByID(ctx, c.db, id)
	if err != nil {
		return protocol.Loop{}, err
	}
	if !found {
		return protocol.Loop{}, ErrNotFound
	}
	return loop, nil
}

// Loops returns the loops matching the optional protocol filters in durable
// creation order.
func (c *Controller) Loops(ctx context.Context, params protocol.ListLoopsParams) (protocol.LoopList, error) {
	if err := params.Validate(); err != nil {
		return protocol.LoopList{}, fmt.Errorf("list loops: %w", ErrInvalidQuery)
	}
	query, err := queryText("loops_select_all.sql")
	if err != nil {
		return protocol.LoopList{}, err
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return protocol.LoopList{}, fmt.Errorf("select communication loops: %w", err)
	}
	defer func() { _ = rows.Close() }()
	list := protocol.LoopList{Loops: []protocol.Loop{}}
	for rows.Next() {
		loop, err := scanLoop(rows)
		if err != nil {
			return protocol.LoopList{}, err
		}
		if params.Active && terminalLoopMode(loop.Mode) ||
			params.ThreadID != "" && loop.ThreadID != params.ThreadID ||
			params.AgentID != "" && !slices.Contains(loop.Participants, params.AgentID) {
			continue
		}
		list.Loops = append(list.Loops, loop)
	}
	if err := rows.Err(); err != nil {
		return protocol.LoopList{}, fmt.Errorf("read communication loops: %w", err)
	}
	return list, nil
}

// SetLoopMode changes one live loop using its owner control version.
func (c *Controller) SetLoopMode(
	ctx context.Context,
	params protocol.SetLoopModeParams,
	actor Actor,
) (protocol.Loop, error) {
	if err := params.Validate(); err != nil {
		return protocol.Loop{}, fmt.Errorf("set loop mode: %w", ErrInvalidQuery)
	}
	return c.mutateLoop(ctx, params.LoopID, params.ExpectedVersion, actor,
		func(ctx context.Context, conn *sql.Conn, current protocol.Loop, now time.Time) (protocol.Loop, []ControlEvent, error) {
			next := current
			next.Mode = params.Mode
			next.EffectiveMode = params.Mode
			next.StateReason = ""
			if params.Mode == protocol.LoopModePaused {
				next.StateReason = protocol.LoopStateReasonManual
			}
			next.ControlVersion++
			next.UpdatedAt = now
			if next.Mode == protocol.LoopModeAuto || next.Mode == protocol.LoopModeStep {
				if err := supersedePendingLoopModeApprovals(ctx, conn, next.LoopID, actor.ID, now, next.Mode); err != nil {
					return protocol.Loop{}, nil, err
				}
			}
			events := []ControlEvent{loopChangeEvent(protocol.EventTypeLoopModeChanged, current, next, actor, params.Reason)}
			if current.Mode != protocol.LoopModePaused && next.Mode == protocol.LoopModePaused {
				events = append(events, loopChangeEvent(protocol.EventTypeLoopPaused, current, next, actor, params.Reason))
			}
			if current.Mode == protocol.LoopModePaused && next.Mode != protocol.LoopModePaused {
				events = append(events, loopChangeEvent(protocol.EventTypeLoopResumed, current, next, actor, params.Reason))
			}
			return next, events, nil
		})
}

// ExtendLoopBudget grows the selected dimensions and resumes a loop that was
// automatically paused by budget exhaustion. A manual pause is preserved.
func (c *Controller) ExtendLoopBudget(
	ctx context.Context,
	params protocol.ExtendLoopBudgetParams,
	actor Actor,
) (protocol.Loop, error) {
	if err := params.Validate(); err != nil {
		return protocol.Loop{}, fmt.Errorf("extend loop budget: %w", ErrInvalidQuery)
	}
	return c.mutateLoop(ctx, params.LoopID, params.ExpectedVersion, actor,
		func(ctx context.Context, conn *sql.Conn, current protocol.Loop, now time.Time) (protocol.Loop, []ControlEvent, error) {
			next := current
			if err := addLoopBudget(&next.Budget, params); err != nil {
				return protocol.Loop{}, nil, err
			}
			next.BudgetProfile = protocol.BudgetProfileCustom
			next.ExpiresAt = next.StartedAt.Add(time.Duration(next.Budget.MaxDurationMS) * time.Millisecond)
			resumed := next.Mode == protocol.LoopModePaused && next.StateReason == protocol.LoopStateReasonBudgetExhausted
			if resumed {
				next.Mode = protocol.LoopModeAuto
				next.EffectiveMode = protocol.LoopModeAuto
				next.StateReason = ""
			}
			next.ControlVersion++
			next.UpdatedAt = now
			if err := supersedePendingLoopApprovals(ctx, conn, next.LoopID, actor.ID, now, false); err != nil {
				return protocol.Loop{}, nil, err
			}
			events := []ControlEvent{loopChangeEvent(protocol.EventTypeLoopBudgetExtended, current, next, actor, params.Reason)}
			if resumed {
				events = append(events, loopChangeEvent(protocol.EventTypeLoopResumed, current, next, actor, params.Reason))
			}
			return next, events, nil
		})
}

// StopLoop makes one live loop terminal. Native turn cancellation is a
// separate executor capability and is rejected until it can be verified.
func (c *Controller) StopLoop(
	ctx context.Context,
	params protocol.StopLoopParams,
	actor Actor,
) (protocol.Loop, error) {
	if err := params.Validate(); err != nil {
		return protocol.Loop{}, fmt.Errorf("stop loop: %w", ErrInvalidQuery)
	}
	if params.CancelTurn {
		return protocol.Loop{}, fmt.Errorf("cancel loop turn: %w", ErrUnsupported)
	}
	return c.mutateLoop(ctx, params.LoopID, params.ExpectedVersion, actor,
		func(ctx context.Context, conn *sql.Conn, current protocol.Loop, now time.Time) (protocol.Loop, []ControlEvent, error) {
			next := current
			next.Mode = protocol.LoopModeStopped
			next.EffectiveMode = protocol.LoopModeStopped
			next.StateReason = protocol.LoopStateReasonOwnerStop
			next.StoppedAt = now
			next.ControlVersion++
			next.UpdatedAt = now
			if err := supersedePendingLoopApprovals(ctx, conn, next.LoopID, actor.ID, now, true); err != nil {
				return protocol.Loop{}, nil, err
			}
			return next, []ControlEvent{loopChangeEvent(protocol.EventTypeLoopStopped, current, next, actor, params.Reason)}, nil
		})
}

type loopMutation func(context.Context, *sql.Conn, protocol.Loop, time.Time) (protocol.Loop, []ControlEvent, error)

func (c *Controller) mutateLoop(
	ctx context.Context,
	id protocol.LoopID,
	expectedVersion int64,
	actor Actor,
	change loopMutation,
) (protocol.Loop, error) {
	if actor.Type != protocol.ActorTypeHuman || actor.ID == "" || utf8.RuneCountInString(actor.ID) > 128 {
		return protocol.Loop{}, fmt.Errorf("loop actor: %w", ErrInvalidQuery)
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return protocol.Loop{}, fmt.Errorf("loop control connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return protocol.Loop{}, fmt.Errorf("begin immediate loop transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	current, found, err := selectLoopByID(ctx, conn, id)
	if err != nil {
		return protocol.Loop{}, err
	}
	if !found {
		return protocol.Loop{}, ErrNotFound
	}
	if current.ControlVersion != expectedVersion {
		return protocol.Loop{}, loopVersionConflict(current)
	}
	if current.OwnerNode != c.nodeID {
		return protocol.Loop{}, fmt.Errorf("loop owner: %w", ErrInvalidQuery)
	}
	if terminalLoopMode(current.Mode) {
		return protocol.Loop{}, fmt.Errorf("terminal loop is immutable: %w", ErrInvalidLoop)
	}
	if current.ControlVersion == math.MaxInt64 {
		return protocol.Loop{}, fmt.Errorf("loop version overflow: %w", ErrInvalidLoop)
	}
	next, events, err := change(ctx, conn, current, c.clock.Now().UTC())
	if err != nil {
		return protocol.Loop{}, err
	}
	if err := updateLoopCAS(ctx, conn, next, expectedVersion); err != nil {
		return protocol.Loop{}, err
	}
	for i := range events {
		if err := events[i].Payload.ValidateFor(events[i].Type); err != nil {
			return protocol.Loop{}, fmt.Errorf("loop event %d: %w", i, err)
		}
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return protocol.Loop{}, fmt.Errorf("commit loop transaction: %w", err)
	}
	committed = true
	if c.events != nil && len(events) > 0 {
		if err := c.events.EmitControlEvents(ctx, events); err != nil {
			return protocol.Loop{}, fmt.Errorf("emit loop control events: %w", err)
		}
	}
	return next, nil
}

func addLoopBudget(budget *protocol.LoopBudget, params protocol.ExtendLoopBudgetParams) error {
	add := func(current *int64, increment *int64, dimension string, maximum int64) error {
		if increment == nil {
			return nil
		}
		if *current > maximum-*increment {
			return fmt.Errorf("%s overflow: %w", dimension, ErrInvalidQuery)
		}
		*current += *increment
		return nil
	}
	if err := add(&budget.MaxTurns, params.AddTurns, "max_turns", math.MaxInt64); err != nil {
		return err
	}
	if err := add(&budget.MaxMessages, params.AddMessages, "max_messages", math.MaxInt64); err != nil {
		return err
	}
	maxDurationMS := math.MaxInt64 / int64(time.Millisecond)
	return add(&budget.MaxDurationMS, params.AddDurationMS, "max_duration_ms", maxDurationMS)
}

func supersedePendingLoopApprovals(
	ctx context.Context,
	conn *sql.Conn,
	loopID protocol.LoopID,
	actor string,
	now time.Time,
	all bool,
) error {
	name := "approvals_supersede_pending_loop_extension.sql"
	if all {
		name = "approvals_supersede_pending_loop.sql"
	}
	query, err := queryText(name)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, query, actor, formatChannelTime(now), loopID); err != nil {
		return fmt.Errorf("supersede pending loop approvals: %w", err)
	}
	return nil
}

func supersedePendingLoopModeApprovals(
	ctx context.Context,
	conn *sql.Conn,
	loopID protocol.LoopID,
	actor string,
	now time.Time,
	mode protocol.LoopMode,
) error {
	query, err := queryText("approvals_supersede_pending_loop_mode.sql")
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, query, actor, formatChannelTime(now), loopID, mode); err != nil {
		return fmt.Errorf("supersede pending loop mode approvals: %w", err)
	}
	return nil
}

func loopChangeEvent(
	typeName protocol.EventType,
	current protocol.Loop,
	next protocol.Loop,
	actor Actor,
	reason string,
) ControlEvent {
	version := next.ControlVersion
	oldValue, _ := json.Marshal(struct {
		Mode        protocol.LoopMode        `json:"mode"`
		StateReason protocol.LoopStateReason `json:"state_reason,omitempty"`
		Budget      protocol.LoopBudget      `json:"budget"`
	}{current.Mode, current.StateReason, current.Budget})
	newValue, _ := json.Marshal(struct {
		Mode        protocol.LoopMode        `json:"mode"`
		StateReason protocol.LoopStateReason `json:"state_reason,omitempty"`
		Budget      protocol.LoopBudget      `json:"budget"`
	}{next.Mode, next.StateReason, next.Budget})
	return ControlEvent{Type: typeName, Payload: protocol.ChannelEventPayload{
		ScopeType: protocol.ScopeTypeLoop, ScopeKey: string(next.LoopID), Plane: protocol.PlaneAll,
		OldValue: protocol.Value(oldValue), NewValue: protocol.Value(newValue), Version: &version,
		ActorType: actor.Type, ActorID: actor.ID, Reason: reason,
		LoopID: next.LoopID, LoopMode: next.Mode, StateReason: next.StateReason,
	}}
}

// selectLoopByID returns one loop and distinguishes absence from a damaged
// row. Reads never synthesize defaults for stored data.
func selectLoopByID(ctx context.Context, q Queryer, id protocol.LoopID) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_by_id.sql", string(id))
}

// selectActiveLoopByThread returns the unique nonterminal loop for a thread.
func selectActiveLoopByThread(ctx context.Context, q Queryer, thread protocol.ThreadID) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_active_by_thread.sql", string(thread))
}

// selectLatestLoopByThread returns the greatest generation, terminal or not.
func selectLatestLoopByThread(ctx context.Context, q Queryer, thread protocol.ThreadID) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_latest_by_thread.sql", string(thread))
}

// selectLatestTerminalLoopByThread returns the greatest closed generation.
func selectLatestTerminalLoopByThread(
	ctx context.Context,
	q Queryer,
	thread protocol.ThreadID,
) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_latest_terminal_by_thread.sql", string(thread))
}

// selectLoopByRootMessage returns the generation durably opened by root.
func selectLoopByRootMessage(
	ctx context.Context,
	q Queryer,
	thread protocol.ThreadID,
	root protocol.ULID,
) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_by_root_message.sql", string(thread), string(root))
}

// selectClosedLoopByThreadSequence returns the earliest closed generation
// whose conversation boundary contains sequence.
func selectClosedLoopByThreadSequence(
	ctx context.Context,
	q Queryer,
	thread protocol.ThreadID,
	sequence int64,
) (protocol.Loop, bool, error) {
	return selectOneLoop(ctx, q, "loops_select_closed_by_sequence.sql", string(thread), sequence)
}

func selectOneLoop(ctx context.Context, q Queryer, name string, args ...any) (protocol.Loop, bool, error) {
	query, err := queryText(name)
	if err != nil {
		return protocol.Loop{}, false, err
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return protocol.Loop{}, false, fmt.Errorf("select communication loop: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return protocol.Loop{}, false, fmt.Errorf("read communication loop: %w", err)
		}
		return protocol.Loop{}, false, nil
	}
	loop, err := scanLoop(rows)
	if err != nil {
		return protocol.Loop{}, false, err
	}
	if rows.Next() {
		return protocol.Loop{}, false, fmt.Errorf("query %s returned more than one row: %w", name, ErrInvalidLoop)
	}
	if err := rows.Err(); err != nil {
		return protocol.Loop{}, false, fmt.Errorf("read communication loop: %w", err)
	}
	return loop, true, nil
}

func scanLoop(row interface{ Scan(...any) error }) (protocol.Loop, error) {
	var loop protocol.Loop
	var loopID, threadID, ownerNode, origin, participantsJSON, mode string
	var stateReason, rootMessageID, lastSender, patternState sql.NullString
	var budgetProfile, startedAt, expiresAt, createdBy, updatedAt string
	var lastProgressAt, stoppedAt sql.NullString
	if err := row.Scan(
		&loopID, &threadID, &loop.LoopGeneration, &ownerNode, &origin,
		&participantsJSON, &mode, &stateReason, &budgetProfile, &rootMessageID,
		&loop.Budget.MaxTurns, &loop.Budget.MaxMessages, &loop.Budget.MaxDurationMS,
		&loop.Budget.MaxMessagesPerMinute, &loop.Budget.MaxHops,
		&loop.Budget.MaxConsecutiveSamePair, &loop.Budget.MaxRetries,
		&loop.Budget.MaxArtifactCount, &loop.Budget.MaxArtifactBytes,
		&loop.Counters.TurnCount, &loop.Counters.MessageCount, &loop.Counters.HopCount,
		&loop.Counters.ArtifactCount, &loop.Counters.ArtifactBytes, &loop.Counters.StepTokens,
		&lastSender, &patternState, &loop.ControlVersion, &startedAt, &expiresAt,
		&lastProgressAt, &stoppedAt, &createdBy, &updatedAt,
	); err != nil {
		return protocol.Loop{}, fmt.Errorf("scan communication loop: %w", err)
	}
	loop.LoopID = protocol.LoopID(loopID)
	loop.ThreadID = protocol.ThreadID(threadID)
	loop.OwnerNode = protocol.NodeID(ownerNode)
	loop.Origin = protocol.LoopOrigin(origin)
	loop.Mode = protocol.LoopMode(mode)
	loop.EffectiveMode = loop.Mode
	loop.StateReason = protocol.LoopStateReason(stateReason.String)
	loop.BudgetProfile = protocol.BudgetProfile(budgetProfile)
	loop.RootMessageID = protocol.ULID(rootMessageID.String)
	loop.LastSenderAgent = protocol.AgentID(lastSender.String)
	loop.CreatedBy = createdBy
	if err := decodeJSON(participantsJSON, &loop.Participants); err != nil {
		return protocol.Loop{}, fmt.Errorf("loop %s participants: %w: %w", loop.LoopID, err, ErrInvalidLoop)
	}
	var err error
	if loop.StartedAt, err = parseChannelTime(startedAt); err != nil {
		return protocol.Loop{}, fmt.Errorf("loop %s started_at: %w: %w", loop.LoopID, err, ErrInvalidLoop)
	}
	if loop.ExpiresAt, err = parseChannelTime(expiresAt); err != nil {
		return protocol.Loop{}, fmt.Errorf("loop %s expires_at: %w: %w", loop.LoopID, err, ErrInvalidLoop)
	}
	if loop.UpdatedAt, err = parseChannelTime(updatedAt); err != nil {
		return protocol.Loop{}, fmt.Errorf("loop %s updated_at: %w: %w", loop.LoopID, err, ErrInvalidLoop)
	}
	if lastProgressAt.Valid {
		if loop.LastProgressAt, err = parseChannelTime(lastProgressAt.String); err != nil {
			return protocol.Loop{}, fmt.Errorf("loop %s last_progress_at: %w: %w", loop.LoopID, err, ErrInvalidLoop)
		}
	}
	if stoppedAt.Valid {
		if loop.StoppedAt, err = parseChannelTime(stoppedAt.String); err != nil {
			return protocol.Loop{}, fmt.Errorf("loop %s stopped_at: %w: %w", loop.LoopID, err, ErrInvalidLoop)
		}
	}
	if patternState.Valid && !json.Valid([]byte(patternState.String)) {
		return protocol.Loop{}, fmt.Errorf("loop %s pattern_state_json: malformed JSON: %w", loop.LoopID, ErrInvalidLoop)
	}
	if err := loop.Validate(); err != nil {
		return protocol.Loop{}, fmt.Errorf("loop %s: %w: %w", loop.LoopID, err, ErrInvalidLoop)
	}
	return loop, nil
}

// createLoopCAS inserts exactly the next generation. expectedGeneration is
// zero for a thread with no history, otherwise it must name the latest
// terminal generation. An existing nonterminal generation always conflicts.
func createLoopCAS(ctx context.Context, q loopStore, loop protocol.Loop, expectedGeneration int64) error {
	if expectedGeneration < 0 {
		return fmt.Errorf("expected generation: %w", ErrInvalidLoop)
	}
	if err := validateStoredLoop(loop); err != nil {
		return err
	}
	if loop.Mode != protocol.LoopModeAuto && loop.Mode != protocol.LoopModeStep {
		return fmt.Errorf("new loop mode must be AUTO or STEP: %w", ErrInvalidLoop)
	}
	active, found, err := selectActiveLoopByThread(ctx, q, loop.ThreadID)
	if err != nil {
		return err
	}
	if found {
		return loopVersionConflict(active)
	}
	latest, found, err := selectLatestLoopByThread(ctx, q, loop.ThreadID)
	if err != nil {
		return err
	}
	nextGeneration := int64(1)
	if found {
		if latest.LoopGeneration != expectedGeneration {
			return loopVersionConflict(latest)
		}
		nextGeneration = latest.LoopGeneration + 1
	} else if expectedGeneration != 0 {
		return &VersionConflictError{CurrentGeneration: 0}
	}
	if loop.LoopGeneration != nextGeneration {
		return fmt.Errorf("loop_generation is %d, want %d: %w", loop.LoopGeneration, nextGeneration, ErrInvalidLoop)
	}
	return insertLoop(ctx, q, loop)
}

func insertLoop(ctx context.Context, q loopStore, loop protocol.Loop) error {
	params, err := loopInsertParams(loop)
	if err != nil {
		return err
	}
	query, err := queryText("loops_insert.sql")
	if err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, query, params...); err != nil {
		if conflict, ok := findLoopConflict(ctx, q, loop); ok {
			return conflict
		}
		return fmt.Errorf("insert communication loop: %w", err)
	}
	return nil
}

func loopInsertParams(loop protocol.Loop) ([]any, error) {
	if err := validateStoredLoop(loop); err != nil {
		return nil, err
	}
	participants, err := json.Marshal(loop.Participants)
	if err != nil {
		return nil, fmt.Errorf("encode loop participants: %w: %w", err, ErrInvalidLoop)
	}
	return []any{
		string(loop.LoopID), string(loop.ThreadID), loop.LoopGeneration,
		string(loop.OwnerNode), string(loop.Origin), string(participants),
		string(loop.Mode), nullString(string(loop.StateReason)), string(loop.BudgetProfile),
		nullString(string(loop.RootMessageID)), loop.Budget.MaxTurns, loop.Budget.MaxMessages,
		loop.Budget.MaxDurationMS, loop.Budget.MaxMessagesPerMinute, loop.Budget.MaxHops,
		loop.Budget.MaxConsecutiveSamePair, loop.Budget.MaxRetries, loop.Budget.MaxArtifactCount,
		loop.Budget.MaxArtifactBytes, loop.Counters.TurnCount, loop.Counters.MessageCount,
		loop.Counters.HopCount, loop.Counters.ArtifactCount, loop.Counters.ArtifactBytes,
		loop.Counters.StepTokens, nullString(string(loop.LastSenderAgent)), nil,
		loop.ControlVersion, formatChannelTime(loop.StartedAt), formatChannelTime(loop.ExpiresAt),
		nullTime(loop.LastProgressAt), nullTime(loop.StoppedAt), loop.CreatedBy,
		formatChannelTime(loop.UpdatedAt),
	}, nil
}

// updateLoopCAS changes only mutable owner-loop state and requires the next
// control version. Pattern state is deliberately left untouched by this 8.1a
// repository; 8.1c owns its representation and mutations.
func updateLoopCAS(ctx context.Context, q loopStore, next protocol.Loop, expectedVersion int64) error {
	if err := validateStoredLoop(next); err != nil {
		return err
	}
	if expectedVersion < 1 {
		return fmt.Errorf("expected control_version is %d: %w", expectedVersion, ErrInvalidLoop)
	}
	current, found, err := selectLoopByID(ctx, q, next.LoopID)
	if err != nil {
		return err
	}
	if !found {
		return &VersionConflictError{CurrentVersion: 0}
	}
	if current.ControlVersion != expectedVersion {
		return loopVersionConflict(current)
	}
	if terminalLoopMode(current.Mode) {
		if reflect.DeepEqual(current, next) {
			return nil
		}
		return fmt.Errorf("terminal loop is immutable: %w", ErrInvalidLoop)
	}
	if next.ControlVersion != expectedVersion+1 {
		return fmt.Errorf("control_version is %d, expected successor of %d: %w", next.ControlVersion, expectedVersion, ErrInvalidLoop)
	}
	if !sameLoopIdentity(current, next) {
		return fmt.Errorf("immutable loop identity changed: %w", ErrInvalidLoop)
	}
	if err := validateLoopTransition(current, next); err != nil {
		return err
	}
	query, err := queryText("loops_update_cas.sql")
	if err != nil {
		return err
	}
	params := []any{
		string(next.Mode), nullString(string(next.StateReason)), string(next.BudgetProfile),
		next.Budget.MaxTurns, next.Budget.MaxMessages, next.Budget.MaxDurationMS,
		next.Budget.MaxMessagesPerMinute, next.Budget.MaxHops,
		next.Budget.MaxConsecutiveSamePair, next.Budget.MaxRetries,
		next.Budget.MaxArtifactCount, next.Budget.MaxArtifactBytes,
		next.Counters.TurnCount, next.Counters.MessageCount, next.Counters.HopCount,
		next.Counters.ArtifactCount, next.Counters.ArtifactBytes, next.Counters.StepTokens,
		nullString(string(next.LastSenderAgent)), next.ControlVersion,
		formatChannelTime(next.ExpiresAt), nullTime(next.LastProgressAt),
		nullTime(next.StoppedAt), formatChannelTime(next.UpdatedAt),
		string(next.LoopID), expectedVersion,
	}
	result, err := q.ExecContext(ctx, query, params...)
	if err != nil {
		if conflict, ok := findLoopUpdateConflict(ctx, q, next, expectedVersion); ok {
			return conflict
		}
		return fmt.Errorf("update communication loop: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count updated communication loops: %w", err)
	}
	if changed != 1 {
		if conflict, ok := findLoopUpdateConflict(ctx, q, next, expectedVersion); ok {
			return conflict
		}
		return &VersionConflictError{CurrentVersion: 0}
	}
	return nil
}

func selectLoopClosureSequence(ctx context.Context, q Queryer, id protocol.LoopID) (int64, bool, error) {
	query, err := queryText("loops_select_closed_sequence.sql")
	if err != nil {
		return 0, false, err
	}
	rows, err := q.QueryContext(ctx, query, id)
	if err != nil {
		return 0, false, fmt.Errorf("select loop closure sequence: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("read loop closure sequence: %w", err)
		}
		return 0, false, nil
	}
	var sequence sql.NullInt64
	if err := rows.Scan(&sequence); err != nil {
		return 0, false, fmt.Errorf("scan loop closure sequence: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("read loop closure sequence: %w", err)
	}
	return sequence.Int64, sequence.Valid, nil
}

func validateStoredLoop(loop protocol.Loop) error {
	if loop.LocalOverride != "" || loop.EffectiveMode != loop.Mode {
		return fmt.Errorf("stored owner loop cannot contain a local override: %w", ErrInvalidLoop)
	}
	if err := loop.Validate(); err != nil {
		return fmt.Errorf("loop: %w: %w", err, ErrInvalidLoop)
	}
	return nil
}

func sameLoopIdentity(a, b protocol.Loop) bool {
	return a.LoopID == b.LoopID && a.ThreadID == b.ThreadID &&
		a.LoopGeneration == b.LoopGeneration && a.OwnerNode == b.OwnerNode &&
		a.Origin == b.Origin && slices.Equal(a.Participants, b.Participants) &&
		a.RootMessageID == b.RootMessageID && a.StartedAt.Equal(b.StartedAt) &&
		a.CreatedBy == b.CreatedBy
}

func validateLoopTransition(current, next protocol.Loop) error {
	if terminalLoopMode(current.Mode) && next.Mode != current.Mode {
		return fmt.Errorf("terminal loop cannot change mode: %w", ErrInvalidLoop)
	}
	if next.Counters.TurnCount < current.Counters.TurnCount ||
		next.Counters.MessageCount < current.Counters.MessageCount ||
		next.Counters.HopCount < current.Counters.HopCount ||
		next.Counters.ArtifactCount < current.Counters.ArtifactCount ||
		next.Counters.ArtifactBytes < current.Counters.ArtifactBytes {
		return fmt.Errorf("loop counters cannot decrease: %w", ErrInvalidLoop)
	}
	if next.UpdatedAt.Before(current.UpdatedAt) ||
		(!current.LastProgressAt.IsZero() && next.LastProgressAt.Before(current.LastProgressAt)) ||
		(!current.StoppedAt.IsZero() && !next.StoppedAt.Equal(current.StoppedAt)) {
		return fmt.Errorf("loop timestamps cannot move backward: %w", ErrInvalidLoop)
	}
	return nil
}

func terminalLoopMode(mode protocol.LoopMode) bool {
	return mode == protocol.LoopModeStopped || mode == protocol.LoopModeCompleted
}

func loopVersionConflict(loop protocol.Loop) *VersionConflictError {
	return &VersionConflictError{
		CurrentVersion:    loop.ControlVersion,
		CurrentGeneration: loop.LoopGeneration,
		CurrentLoopID:     loop.LoopID,
	}
}

func findLoopConflict(ctx context.Context, q Queryer, loop protocol.Loop) (*VersionConflictError, bool) {
	if current, found, err := selectLoopByID(ctx, q, loop.LoopID); err == nil && found {
		return loopVersionConflict(current), true
	}
	if !terminalLoopMode(loop.Mode) {
		if current, found, err := selectActiveLoopByThread(ctx, q, loop.ThreadID); err == nil && found {
			return loopVersionConflict(current), true
		}
	}
	if current, found, err := selectLatestLoopByThread(ctx, q, loop.ThreadID); err == nil && found &&
		current.LoopGeneration >= loop.LoopGeneration {
		return loopVersionConflict(current), true
	}
	return nil, false
}

func findLoopUpdateConflict(ctx context.Context, q Queryer, loop protocol.Loop, expectedVersion int64) (*VersionConflictError, bool) {
	if active, found, err := selectActiveLoopByThread(ctx, q, loop.ThreadID); err == nil && found && active.LoopID != loop.LoopID {
		return loopVersionConflict(active), true
	}
	current, found, err := selectLoopByID(ctx, q, loop.LoopID)
	if err == nil && (!found || current.ControlVersion != expectedVersion) {
		if !found {
			return &VersionConflictError{CurrentVersion: 0}, true
		}
		return loopVersionConflict(current), true
	}
	return nil, false
}
