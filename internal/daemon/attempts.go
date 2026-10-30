package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// AttemptState is the durable state of one PTY delivery attempt.
type AttemptState string

// Delivery-attempt states. The first three are non-terminal; UNCERTAIN is
// terminal for uniqueness while continuing to block automatic delivery.
const (
	AttemptPreparing            AttemptState = "PREPARING"
	AttemptPrepared             AttemptState = "PREPARED"
	AttemptCommitSent           AttemptState = "COMMIT_SENT"
	AttemptCommitted            AttemptState = "COMMITTED"
	AttemptCommitNotConsumed    AttemptState = "COMMIT_NOT_CONSUMED"
	AttemptAbortedHumanPriority AttemptState = "ABORTED_HUMAN_PRIORITY"
	AttemptCancelled            AttemptState = "CANCELLED"
	AttemptFailed               AttemptState = "FAILED"
	AttemptUncertain            AttemptState = "UNCERTAIN"
)

// AttemptReason is an opaque, durable reason token. It is deliberately local
// to the daemon: adding a worker reason does not extend the wire protocol.
type AttemptReason string

// Reasons known to the attempt store itself. Callers may use other valid
// opaque tokens for adapter-specific cancellation and failure reasons.
const (
	ReasonHumanPriority  AttemptReason = "HUMAN_PRIORITY"
	ReasonControlChanged AttemptReason = "CONTROL_CHANGED"
	ReasonDisconnected   AttemptReason = "DISCONNECTED"
	ReasonTimedOut       AttemptReason = "TIMED_OUT"
	ReasonProcessRestart AttemptReason = "PROCESS_RESTART"
)

var (
	// ErrInvalidOptions means the attempt store has no clock.
	ErrInvalidOptions = errors.New("invalid attempt store options")
	// ErrInvalidAttempt means an attempt or transition has invalid fields.
	ErrInvalidAttempt = errors.New("invalid delivery attempt")
	// ErrAttemptNotFound means the delivery id has no durable attempt.
	ErrAttemptNotFound = errors.New("delivery attempt not found")
	// ErrInvalidTransition means the requested state edge is not permitted or
	// the compare-and-swap observed another state.
	ErrInvalidTransition = errors.New("invalid delivery attempt transition")
)

// AttemptSpec is the immutable authority and guard snapshot captured before a
// prepare. The store supplies state and timestamps.
type AttemptSpec struct {
	DeliveryID             protocol.OpaqueID
	MessageID              protocol.ULID
	AgentID                protocol.AgentID
	DeliveryGeneration     int64
	AdapterSessionID       protocol.OpaqueID
	OrchestratorDecisionID protocol.ULID
	ChannelDecisionID      protocol.ChannelDecisionID
	DraftGeneration        int64
	InputGuardVersion      int64
	ControlVersions        protocol.ControlVersions
}

// Attempt is one durable delivery-attempt row.
type Attempt struct {
	AttemptSpec
	WriterLeaseID        protocol.OpaqueID
	State                AttemptState
	CommitSentAt         time.Time
	TerminalReason       AttemptReason
	RecoveryOfDeliveryID protocol.OpaqueID
	UncertainApprovalID  protocol.ApprovalID
	ResolutionApprovalID protocol.ApprovalID
	PreparedAt           time.Time
	FinishedAt           time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// Transition describes one compare-and-swap state edge. WriterLeaseID is
// required only when entering PREPARED. A terminal reason is required for
// cancellation, failure, human-priority abort and uncertainty.
type Transition struct {
	From          AttemptState
	To            AttemptState
	WriterLeaseID protocol.OpaqueID
	Reason        AttemptReason
}

// TransitionError reports the state observed after a failed state CAS.
type TransitionError struct {
	DeliveryID protocol.OpaqueID
	Expected   AttemptState
	Actual     AttemptState
	Requested  AttemptState
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: delivery %s is %s, expected %s before %s",
		ErrInvalidTransition, e.DeliveryID, e.Actual, e.Expected, e.Requested)
}

// Unwrap makes TransitionError match ErrInvalidTransition.
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

// AttemptStore persists attempt state using caller-owned short transactions.
type AttemptStore struct {
	clock clock.Clock
}

// NewAttemptStore returns an attempt store using clk as its only time source.
func NewAttemptStore(clk clock.Clock) (*AttemptStore, error) {
	if clk == nil {
		return nil, ErrInvalidOptions
	}
	return &AttemptStore{clock: clk}, nil
}

// Create inserts a new attempt in PREPARING.
func (s *AttemptStore) Create(ctx context.Context, tx *sql.Tx, spec AttemptSpec) (Attempt, error) {
	return s.create(ctx, tx, spec, "")
}

// CreateRecovery inserts a manually initiated successor to an UNCERTAIN
// attempt. The uncertain row remains unchanged and keeps blocking automation.
func (s *AttemptStore) CreateRecovery(
	ctx context.Context,
	tx *sql.Tx,
	spec AttemptSpec,
	uncertainID protocol.OpaqueID,
) (Attempt, error) {
	if tx == nil || !uncertainID.Valid() {
		return Attempt{}, fmt.Errorf("%w: recovery", ErrInvalidAttempt)
	}
	uncertain, err := getAttempt(ctx, tx, uncertainID)
	if err != nil {
		return Attempt{}, err
	}
	if uncertain.State != AttemptUncertain || uncertain.MessageID != spec.MessageID ||
		uncertain.AgentID != spec.AgentID || uncertain.DeliveryGeneration != spec.DeliveryGeneration ||
		uncertain.AdapterSessionID != spec.AdapterSessionID {
		return Attempt{}, fmt.Errorf("%w: recovery source", ErrInvalidAttempt)
	}
	return s.create(ctx, tx, spec, uncertainID)
}

func (s *AttemptStore) create(
	ctx context.Context,
	tx *sql.Tx,
	spec AttemptSpec,
	recoveryOf protocol.OpaqueID,
) (Attempt, error) {
	if tx == nil {
		return Attempt{}, fmt.Errorf("%w: transaction", ErrInvalidAttempt)
	}
	if err := validateSpec(spec); err != nil {
		return Attempt{}, err
	}
	if spec.DeliveryID == recoveryOf {
		return Attempt{}, fmt.Errorf("%w: recovery delivery id", ErrInvalidAttempt)
	}
	versions, err := json.Marshal(spec.ControlVersions)
	if err != nil {
		return Attempt{}, fmt.Errorf("encode control versions: %w", err)
	}
	now := s.clock.Now().UTC()
	stamp := eventstore.FormatTime(now)
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation,
		 adapter_session_id, orchestrator_decision_id, channel_decision_id,
		 draft_generation, input_guard_version, control_versions_json,
		 state, recovery_of_delivery_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		spec.DeliveryID, spec.MessageID, spec.AgentID, spec.DeliveryGeneration,
		spec.AdapterSessionID, spec.OrchestratorDecisionID, spec.ChannelDecisionID,
		spec.DraftGeneration, spec.InputGuardVersion, string(versions),
		AttemptPreparing, nullOpaqueID(recoveryOf), stamp, stamp)
	if err != nil {
		return Attempt{}, fmt.Errorf("insert delivery attempt %s: %w", spec.DeliveryID, err)
	}
	storedSpec := spec
	storedSpec.ControlVersions = cloneControlVersions(spec.ControlVersions)
	return Attempt{
		AttemptSpec: storedSpec, State: AttemptPreparing, RecoveryOfDeliveryID: recoveryOf,
		CreatedAt: now, UpdatedAt: now,
	}, nil
}

// Get reads one attempt through the caller's transaction.
func (s *AttemptStore) Get(ctx context.Context, tx *sql.Tx, deliveryID protocol.OpaqueID) (Attempt, error) {
	if tx == nil || !deliveryID.Valid() {
		return Attempt{}, fmt.Errorf("%w: get", ErrInvalidAttempt)
	}
	return getAttempt(ctx, tx, deliveryID)
}

// Transition applies one permitted edge with a state compare-and-swap. A
// rejected edge or stale From value leaves the row unchanged.
func (s *AttemptStore) Transition(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
	change Transition,
) error {
	if tx == nil || !deliveryID.Valid() {
		return fmt.Errorf("%w: transition", ErrInvalidAttempt)
	}
	if err := validateTransition(change); err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	stamp := eventstore.FormatTime(now)
	var preparedAt, commitSentAt, finishedAt any
	if change.To == AttemptPrepared {
		preparedAt = stamp
	}
	if change.To == AttemptCommitSent {
		commitSentAt = stamp
	}
	if change.To.terminal() {
		finishedAt = stamp
	}
	result, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET
		state = ?,
		writer_lease_id = COALESCE(?, writer_lease_id),
		terminal_reason = ?,
		prepared_at = COALESCE(?, prepared_at),
		commit_sent_at = COALESCE(?, commit_sent_at),
		finished_at = COALESCE(?, finished_at),
		updated_at = ?
		WHERE delivery_id = ? AND state = ?`,
		change.To, nullOpaqueID(change.WriterLeaseID), nullReason(change.Reason),
		preparedAt, commitSentAt, finishedAt, stamp, deliveryID, change.From)
	if err != nil {
		return fmt.Errorf("transition delivery attempt %s from %s to %s: %w",
			deliveryID, change.From, change.To, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("transition delivery attempt rows affected: %w", err)
	}
	if changed == 1 {
		return nil
	}
	var actual AttemptState
	if err := tx.QueryRowContext(ctx,
		"SELECT state FROM delivery_attempts WHERE delivery_id = ?", deliveryID).Scan(&actual); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAttemptNotFound
		}
		return fmt.Errorf("read delivery attempt state: %w", err)
	}
	return &TransitionError{
		DeliveryID: deliveryID, Expected: change.From, Actual: actual, Requested: change.To,
	}
}

// MarkPrepared stores the writer lease returned by a successful prepare.
func (s *AttemptStore) MarkPrepared(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID, writerLeaseID protocol.OpaqueID,
) error {
	return s.Transition(ctx, tx, deliveryID, Transition{
		From: AttemptPreparing, To: AttemptPrepared, WriterLeaseID: writerLeaseID,
	})
}

// MarkCommitSent durably records the point of no automatic retry. Its caller
// must commit tx before emitting the commit frame.
func (s *AttemptStore) MarkCommitSent(ctx context.Context, tx *sql.Tx, deliveryID protocol.OpaqueID) error {
	return s.Transition(ctx, tx, deliveryID, Transition{From: AttemptPrepared, To: AttemptCommitSent})
}

// MarkUncertain records a COMMIT_SENT attempt whose response cannot be known.
func (s *AttemptStore) MarkUncertain(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
	reason AttemptReason,
) error {
	return s.Transition(ctx, tx, deliveryID, Transition{
		From: AttemptCommitSent, To: AttemptUncertain, Reason: reason,
	})
}

// RecoverCommitSent closes every attempt left active by a process restart.
// PREPARING and PREPARED are safely CANCELLED because no commit frame was
// durably authorized; result-less COMMIT_SENT rows become UNCERTAIN. No TTL
// converts uncertain rows into success or retryable work.
func (s *AttemptStore) RecoverCommitSent(
	ctx context.Context,
	tx *sql.Tx,
	reason AttemptReason,
) (int64, error) {
	if tx == nil || !reason.valid() {
		return 0, fmt.Errorf("%w: recover commit sent", ErrInvalidAttempt)
	}
	stamp := eventstore.FormatTime(s.clock.Now().UTC())
	preCommit, err := tx.ExecContext(ctx, `UPDATE delivery_attempts
		SET state = ?, terminal_reason = ?, finished_at = ?, updated_at = ?
		WHERE state IN (?, ?)`, AttemptCancelled, ReasonProcessRestart, stamp, stamp,
		AttemptPreparing, AttemptPrepared)
	if err != nil {
		return 0, fmt.Errorf("recover pre-commit attempts: %w", err)
	}
	preCommitCount, err := preCommit.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover pre-commit rows affected: %w", err)
	}
	commitSent, err := tx.ExecContext(ctx, `UPDATE delivery_attempts
		SET state = ?, terminal_reason = ?, finished_at = ?, updated_at = ?
		WHERE state = ?`, AttemptUncertain, reason, stamp, stamp, AttemptCommitSent)
	if err != nil {
		return 0, fmt.Errorf("recover commit-sent attempts: %w", err)
	}
	commitSentCount, err := commitSent.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover commit-sent rows affected: %w", err)
	}
	return preCommitCount + commitSentCount, nil
}

// ResolveUncertain records the audited approval that explicitly resolved an
// uncertain commit. The attempt state remains UNCERTAIN forever.
func (s *AttemptStore) ResolveUncertain(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
	approvalID protocol.ApprovalID,
) error {
	if tx == nil || !deliveryID.Valid() || !approvalID.Valid() {
		return fmt.Errorf("%w: uncertain resolution", ErrInvalidAttempt)
	}
	stamp := eventstore.FormatTime(s.clock.Now().UTC())
	result, err := tx.ExecContext(ctx, `UPDATE delivery_attempts
		SET resolution_approval_id = ?, updated_at = ?
		WHERE delivery_id = ? AND state = ? AND uncertain_approval_id = ?
		AND resolution_approval_id IS NULL
		AND EXISTS (SELECT 1 FROM approval_requests AS ar
			WHERE ar.approval_id = ? AND ar.source = 'orchestrator'
			AND ar.object_type = 'message' AND ar.object_id = delivery_attempts.message_id
			AND ar.agent_id = delivery_attempts.agent_id AND ar.state = 'PENDING')`,
		approvalID, stamp, deliveryID, AttemptUncertain, approvalID, approvalID)
	if err != nil {
		return fmt.Errorf("resolve uncertain delivery attempt %s: %w", deliveryID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("resolve uncertain delivery attempt rows affected: %w", err)
	}
	if changed == 1 {
		return nil
	}
	var state AttemptState
	var linked, resolution sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT state, uncertain_approval_id, resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = ?`, deliveryID).Scan(&state, &linked, &resolution); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAttemptNotFound
		}
		return fmt.Errorf("read uncertain delivery resolution: %w", err)
	}
	if state == AttemptUncertain && linked.String == string(approvalID) &&
		resolution.String == string(approvalID) {
		return nil
	}
	return fmt.Errorf("%w: delivery %s is %s with resolution %q",
		ErrInvalidTransition, deliveryID, state, resolution.String)
}

// BindUncertainApproval links a newly created, pending approval to exactly one
// uncertain episode. The approval and this binding are committed together.
func (s *AttemptStore) BindUncertainApproval(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
	approvalID protocol.ApprovalID,
) error {
	if tx == nil || !deliveryID.Valid() || !approvalID.Valid() {
		return fmt.Errorf("%w: uncertain approval binding", ErrInvalidAttempt)
	}
	stamp := eventstore.FormatTime(s.clock.Now().UTC())
	result, err := tx.ExecContext(ctx, `UPDATE delivery_attempts
		SET uncertain_approval_id = ?, updated_at = ?
		WHERE delivery_id = ? AND state = ? AND uncertain_approval_id IS NULL
		AND resolution_approval_id IS NULL
		AND EXISTS (SELECT 1 FROM approval_requests AS ar
			WHERE ar.approval_id = ? AND ar.source = 'orchestrator'
			AND ar.object_type = 'message' AND ar.object_id = delivery_attempts.message_id
			AND ar.agent_id = delivery_attempts.agent_id AND ar.state = 'PENDING')`,
		approvalID, stamp, deliveryID, AttemptUncertain, approvalID)
	if err != nil {
		return fmt.Errorf("bind uncertain delivery approval %s: %w", deliveryID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("bind uncertain delivery approval rows affected: %w", err)
	}
	if changed == 1 {
		return nil
	}
	var state AttemptState
	var linked, resolution sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT state, uncertain_approval_id, resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = ?`, deliveryID).Scan(&state, &linked, &resolution); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAttemptNotFound
		}
		return fmt.Errorf("read uncertain approval binding: %w", err)
	}
	if state == AttemptUncertain && linked.String == string(approvalID) && !resolution.Valid {
		return nil
	}
	return fmt.Errorf("%w: delivery %s is %s with approval %q and resolution %q",
		ErrInvalidTransition, deliveryID, state, linked.String, resolution.String)
}

// UnresolvedForApproval returns the exact uncertain episode linked to a
// pending approval. Message, recipient and generation come from the attempt.
func (s *AttemptStore) UnresolvedForApproval(
	ctx context.Context,
	tx *sql.Tx,
	approvalID protocol.ApprovalID,
) (Attempt, error) {
	if tx == nil || !approvalID.Valid() {
		return Attempt{}, fmt.Errorf("%w: uncertain approval query", ErrInvalidAttempt)
	}
	var deliveryID protocol.OpaqueID
	err := tx.QueryRowContext(ctx, `SELECT delivery_id FROM delivery_attempts
		WHERE uncertain_approval_id = ? AND state = ? AND resolution_approval_id IS NULL`,
		approvalID, AttemptUncertain).Scan(&deliveryID)
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, ErrAttemptNotFound
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("query uncertain approval attempt: %w", err)
	}
	return getAttempt(ctx, tx, deliveryID)
}

// HasOtherUncertain reports whether another unresolved episode still blocks
// this agent after one specifically approved episode is reserved.
func (s *AttemptStore) HasOtherUncertain(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	except protocol.OpaqueID,
) (bool, error) {
	if tx == nil || !agent.Valid() || !except.Valid() {
		return false, fmt.Errorf("%w: other uncertain query", ErrInvalidAttempt)
	}
	var found int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM delivery_attempts WHERE agent_id = ? AND state = ?
		AND resolution_approval_id IS NULL AND delivery_id <> ?
	)`, agent, AttemptUncertain, except).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("query other uncertain attempt: %w", err)
	}
	return found == 1, nil
}

// HasUncertain reports whether agent is blocked from automatic delivery by
// an unresolved uncertain commit. Creating a recovery successor does not
// clear this bit; only ResolveUncertain does.
func (s *AttemptStore) HasUncertain(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
) (bool, error) {
	if tx == nil || !agent.Valid() {
		return false, fmt.Errorf("%w: uncertain query", ErrInvalidAttempt)
	}
	var found int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM delivery_attempts WHERE agent_id = ? AND state = ?
		AND resolution_approval_id IS NULL
	)`, agent, AttemptUncertain).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("query uncertain attempt: %w", err)
	}
	return found == 1, nil
}

func getAttempt(ctx context.Context, tx *sql.Tx, deliveryID protocol.OpaqueID) (Attempt, error) {
	var attempt Attempt
	var versions string
	var writerLease, reason, recoveryOf, uncertainApproval, resolutionApproval sql.NullString
	var commitSentAt, preparedAt, finishedAt sql.NullString
	var createdAt, updatedAt string
	err := tx.QueryRowContext(ctx, `SELECT
		delivery_id, message_id, agent_id, delivery_generation,
		adapter_session_id, orchestrator_decision_id, channel_decision_id,
		draft_generation, input_guard_version, control_versions_json,
		writer_lease_id, state, commit_sent_at, terminal_reason,
		recovery_of_delivery_id, uncertain_approval_id, resolution_approval_id, prepared_at, finished_at,
		created_at, updated_at
		FROM delivery_attempts WHERE delivery_id = ?`, deliveryID).Scan(
		&attempt.DeliveryID, &attempt.MessageID, &attempt.AgentID,
		&attempt.DeliveryGeneration, &attempt.AdapterSessionID,
		&attempt.OrchestratorDecisionID, &attempt.ChannelDecisionID,
		&attempt.DraftGeneration, &attempt.InputGuardVersion, &versions,
		&writerLease, &attempt.State, &commitSentAt, &reason, &recoveryOf,
		&uncertainApproval, &resolutionApproval, &preparedAt, &finishedAt, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, ErrAttemptNotFound
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("read delivery attempt %s: %w", deliveryID, err)
	}
	if err := json.Unmarshal([]byte(versions), &attempt.ControlVersions); err != nil {
		return Attempt{}, fmt.Errorf("decode delivery attempt control versions: %w", err)
	}
	attempt.WriterLeaseID = protocol.OpaqueID(writerLease.String)
	attempt.TerminalReason = AttemptReason(reason.String)
	attempt.RecoveryOfDeliveryID = protocol.OpaqueID(recoveryOf.String)
	attempt.UncertainApprovalID = protocol.ApprovalID(uncertainApproval.String)
	attempt.ResolutionApprovalID = protocol.ApprovalID(resolutionApproval.String)
	for name, raw := range map[string]struct {
		value string
		set   func(time.Time)
	}{
		"created_at":     {createdAt, func(v time.Time) { attempt.CreatedAt = v }},
		"updated_at":     {updatedAt, func(v time.Time) { attempt.UpdatedAt = v }},
		"commit_sent_at": {commitSentAt.String, func(v time.Time) { attempt.CommitSentAt = v }},
		"prepared_at":    {preparedAt.String, func(v time.Time) { attempt.PreparedAt = v }},
		"finished_at":    {finishedAt.String, func(v time.Time) { attempt.FinishedAt = v }},
	} {
		if raw.value == "" {
			continue
		}
		parsed, err := eventstore.ParseTime(raw.value)
		if err != nil {
			return Attempt{}, fmt.Errorf("delivery attempt %s %s: %w", deliveryID, name, err)
		}
		raw.set(parsed)
	}
	if err := validateSpec(attempt.AttemptSpec); err != nil {
		return Attempt{}, err
	}
	if !attempt.State.valid() {
		return Attempt{}, fmt.Errorf("%w: stored state", ErrInvalidAttempt)
	}
	return attempt, nil
}

func validateSpec(spec AttemptSpec) error {
	if !spec.DeliveryID.Valid() || !spec.MessageID.Valid() || !spec.AgentID.Valid() ||
		spec.DeliveryGeneration < 1 || !spec.AdapterSessionID.Valid() ||
		!spec.OrchestratorDecisionID.Valid() || !spec.ChannelDecisionID.Valid() ||
		spec.DraftGeneration < 0 || spec.InputGuardVersion < 0 ||
		!validControlVersions(spec.ControlVersions) {
		return fmt.Errorf("%w: fields", ErrInvalidAttempt)
	}
	return nil
}

func validControlVersions(versions protocol.ControlVersions) bool {
	if len(versions) == 0 || len(versions) > 64 {
		return false
	}
	for key, version := range versions {
		length := utf8.RuneCountInString(key)
		if length < 1 || length > 200 || strings.IndexFunc(key, unicode.IsControl) >= 0 || version < 0 {
			return false
		}
	}
	return true
}

func cloneControlVersions(versions protocol.ControlVersions) protocol.ControlVersions {
	cloned := make(protocol.ControlVersions, len(versions))
	for key, version := range versions {
		cloned[key] = version
	}
	return cloned
}

func validateTransition(change Transition) error {
	if !change.From.valid() || !change.To.valid() || !allowedTransition(change.From, change.To) {
		return fmt.Errorf("%w: %s to %s", ErrInvalidTransition, change.From, change.To)
	}
	if change.To == AttemptPrepared {
		if !change.WriterLeaseID.Valid() || change.Reason != "" {
			return fmt.Errorf("%w: prepared fields", ErrInvalidAttempt)
		}
	} else if change.WriterLeaseID != "" {
		return fmt.Errorf("%w: writer lease transition", ErrInvalidAttempt)
	}
	needsReason := change.To == AttemptAbortedHumanPriority || change.To == AttemptCancelled ||
		change.To == AttemptFailed || change.To == AttemptUncertain
	if needsReason != change.Reason.valid() {
		return fmt.Errorf("%w: terminal reason", ErrInvalidAttempt)
	}
	return nil
}

func allowedTransition(from, to AttemptState) bool {
	switch from {
	case AttemptPreparing:
		return to == AttemptPrepared || to == AttemptAbortedHumanPriority ||
			to == AttemptCancelled || to == AttemptFailed
	case AttemptPrepared:
		return to == AttemptCommitSent || to == AttemptAbortedHumanPriority ||
			to == AttemptCancelled || to == AttemptFailed
	case AttemptCommitSent:
		return to == AttemptCommitted || to == AttemptCommitNotConsumed ||
			to == AttemptAbortedHumanPriority || to == AttemptCancelled ||
			to == AttemptFailed || to == AttemptUncertain
	default:
		return false
	}
}

func (s AttemptState) valid() bool {
	switch s {
	case AttemptPreparing, AttemptPrepared, AttemptCommitSent, AttemptCommitted,
		AttemptCommitNotConsumed, AttemptAbortedHumanPriority, AttemptCancelled,
		AttemptFailed, AttemptUncertain:
		return true
	default:
		return false
	}
}

func (s AttemptState) terminal() bool {
	return s.valid() && s != AttemptPreparing && s != AttemptPrepared && s != AttemptCommitSent
}

func (r AttemptReason) valid() bool {
	return r != "" && protocol.OpaqueID(r).Valid()
}

func nullOpaqueID(id protocol.OpaqueID) any {
	if id == "" {
		return nil
	}
	return id
}

func nullReason(reason AttemptReason) any {
	if reason == "" {
		return nil
	}
	return reason
}
