package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultCallTimeout   = 5 * time.Second
	defaultPolicyVersion = "deterministic-5.3c"
	defaultWorkerID      = "orchestrator-5.3c"
	defaultNoticeLine    = "[GIFFORD] 1 pending message"
	queueStateQueued     = "QUEUED"
	queueStateClaimed    = "CLAIMED"
	queueStateExpired    = "EXPIRED"
)

var (
	// ErrInvalidOptions means the executor cannot enforce its durable or
	// typed-authority invariants.
	ErrInvalidOptions = errors.New("orchestrator: invalid executor options")
	// ErrDeliveryDeferred means a manual order did not pass every hard guard.
	// The approval remains pending and no delivery reservation was created.
	ErrDeliveryDeferred = errors.New("orchestrator: delivery deferred")
)

// LinkProvider exposes the authenticated Proxy link without transferring
// ownership of it to the executor.
type LinkProvider interface {
	Snapshot(protocol.AgentID) (agentlink.Snapshot, bool)
	Client(protocol.AgentID) (*agentlink.Client, bool)
}

// ExecutorOptions configures the only component allowed to emit delivery
// operations to a Proxy.
type ExecutorOptions struct {
	DB            *sql.DB
	Channel       *channel.Engine
	Approvals     *channel.Controller
	Attempts      *daemon.AttemptStore
	Links         LinkProvider
	Clock         clock.Clock
	Entropy       io.Reader
	Processes     ProcessInspector
	PolicyVersion string
	WorkerID      protocol.OpaqueID
	NoticeLine    protocol.NoticeLine
	CallTimeout   time.Duration
}

// Executor evaluates guards and runs the two-phase PTY protocol. It is safe
// for concurrent use; SQLite and the Proxy enforce the remaining per-agent
// serialization.
type Executor struct {
	db        *sql.DB
	channel   *channel.Engine
	approvals *channel.Controller
	attempts  *daemon.AttemptStore
	links     LinkProvider
	clock     clock.Clock
	entropy   io.Reader
	processes ProcessInspector
	policy    string
	workerID  protocol.OpaqueID
	notice    protocol.NoticeLine
	timeout   time.Duration

	idMu        sync.Mutex
	lastMillis  int64
	lastEntropy [10]byte
}

type deliveryRecord struct {
	daemon.QueueDelivery
	fromAgent       protocol.AgentID
	originNode      protocol.NodeID
	destinationNode protocol.NodeID
	threadID        protocol.ThreadID
	parentID        protocol.ULID
	hopCount        int64
	priority        protocol.Priority
	queueState      string
	expiresAt       time.Time
	sessionID       protocol.OpaqueID
	sessionState    protocol.SessionState
	captureMode     protocol.CaptureMode
	deliverable     bool
	process         protocol.ProcessIdentity
}

type executionPlan struct {
	record   deliveryRecord
	decision channel.Decision
	attempt  daemon.Attempt
	message  channel.Message
	ref      protocol.MessageRef
	report   protocol.InputGuardReport
}

type preparedCancellation struct {
	state        daemon.AttemptState
	reason       daemon.AttemptReason
	cancelReason protocol.CancelReason
	queueState   string
}

type persistedEvaluation struct {
	decisionID protocol.ULID
}

// NewExecutor validates options and constructs the delivery executor.
func NewExecutor(opts ExecutorOptions) (*Executor, error) {
	if opts.DB == nil || opts.Channel == nil || opts.Approvals == nil ||
		opts.Links == nil || opts.Clock == nil {
		return nil, ErrInvalidOptions
	}
	if opts.Entropy == nil {
		opts.Entropy = rand.Reader
	}
	if opts.Processes == nil {
		opts.Processes = NewProcessInspector("")
	}
	if opts.PolicyVersion == "" {
		opts.PolicyVersion = defaultPolicyVersion
	}
	if utf8.RuneCountInString(opts.PolicyVersion) > 64 {
		return nil, ErrInvalidOptions
	}
	if opts.WorkerID == "" {
		opts.WorkerID = defaultWorkerID
	}
	if !opts.WorkerID.Valid() {
		return nil, ErrInvalidOptions
	}
	if opts.NoticeLine == "" {
		opts.NoticeLine = defaultNoticeLine
	}
	if !opts.NoticeLine.Valid() {
		return nil, ErrInvalidOptions
	}
	if opts.CallTimeout <= 0 {
		opts.CallTimeout = defaultCallTimeout
	}
	if opts.Attempts == nil {
		attempts, err := daemon.NewAttemptStore(opts.Clock)
		if err != nil {
			return nil, fmt.Errorf("create attempt store: %w", err)
		}
		opts.Attempts = attempts
	}
	return &Executor{
		db: opts.DB, channel: opts.Channel, approvals: opts.Approvals,
		attempts: opts.Attempts, links: opts.Links, clock: opts.Clock,
		entropy: opts.Entropy, processes: opts.Processes, policy: opts.PolicyVersion,
		workerID: opts.WorkerID, notice: opts.NoticeLine, timeout: opts.CallTimeout,
	}, nil
}

// EvaluateAutomatic records a fresh channel decision, deterministic guards
// and an approval request. With no decision model in 5.3c, it never delivers.
func (e *Executor) EvaluateAutomatic(ctx context.Context, candidate daemon.QueueDelivery) error {
	snapshot, _ := e.links.Snapshot(candidate.AgentID)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin automatic evaluation: %w", err)
	}
	defer rollback(tx)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return err
	}
	message := record.channelMessage()
	decided, err := e.channel.AuthorizePTYDelivery(ctx, tx, message, candidate.DeliveryGeneration)
	if err != nil {
		return fmt.Errorf("authorize automatic PTY delivery: %w", err)
	}
	guards := []protocol.GuardResult{}
	if decided.Allowed() {
		guards, err = e.evaluateGuards(ctx, tx, record, snapshot, false, "")
		if err != nil {
			return err
		}
	}
	selected := protocol.OrchestratorActionDefer
	if decided.Allowed() && !record.expiresAt.After(e.clock.Now()) {
		selected = protocol.OrchestratorActionExpire
		if err := expireQueue(ctx, tx, record, e.clock.Now()); err != nil {
			return err
		}
	}
	_, err = e.persistEvaluation(ctx, tx, record, snapshot, decided, guards, selected,
		protocol.ExecutionStatusExecuted)
	if err != nil {
		return err
	}
	if selected == protocol.OrchestratorActionDefer {
		note := "automatic delivery deferred: no decision model"
		if detail := firstFailedGuard(guards); detail != "" {
			note = "automatic delivery deferred: " + detail
		}
		if _, _, err := e.approvals.EnsureOrchestratorApproval(
			ctx, tx, record.MessageID, record.AgentID, truncate(note, 500),
		); err != nil {
			return fmt.Errorf("ensure automatic approval: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit automatic evaluation: %w", err)
	}
	return nil
}

// ResolveApproval applies one authenticated orchestrator approval. Reject is
// a durable recipient cancellation. Deliver-now still has to pass every hard
// guard; it only overrides the unavailable p(IDLE) threshold.
func (e *Executor) ResolveApproval(
	ctx context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	if params.Reason == "" {
		params.Reason = "human selected " + string(params.Option)
	}
	if params.Option == "reject" {
		return e.rejectApproval(ctx, params, actor)
	}
	return e.deliverApproved(ctx, params, actor)
}

func (e *Executor) deliverApproved(
	ctx context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("begin manual delivery: %w", err)
	}
	defer rollback(tx)
	request, err := e.approvals.ApprovalTx(ctx, tx, params.ApprovalID)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("load manual approval: %w", err)
	}
	replay, err := approvalReplay(request, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	if replay {
		return request, nil
	}
	candidate, uncertain, err := e.approvalCandidate(ctx, tx, request)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	snapshot, _ := e.links.Snapshot(candidate.AgentID)
	record, err := e.loadDelivery(ctx, tx, candidate, snapshot.SessionID)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	message := record.channelMessage()
	decided, err := e.channel.AuthorizePTYDelivery(ctx, tx, message, candidate.DeliveryGeneration)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("authorize manual PTY delivery: %w", err)
	}
	guards := []protocol.GuardResult{}
	if decided.Allowed() {
		uncertainID := protocol.OpaqueID("")
		if uncertain != nil {
			uncertainID = uncertain.DeliveryID
		}
		guards, err = e.evaluateGuards(ctx, tx, record, snapshot, true, uncertainID)
		if err != nil {
			return protocol.ApprovalRequest{}, err
		}
	}
	failure := firstFailedGuard(guards)
	if !decided.Allowed() {
		failure = "channel decision is " + string(decided.Outcome())
	}
	if failure != "" {
		_, err := e.persistEvaluation(ctx, tx, record, snapshot, decided, guards,
			protocol.OrchestratorActionDefer, protocol.ExecutionStatusExecuted)
		if err != nil {
			return protocol.ApprovalRequest{}, err
		}
		if err := tx.Commit(); err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("commit deferred approval: %w", err)
		}
		return request, fmt.Errorf("%w: %s", ErrDeliveryDeferred, failure)
	}
	persisted, err := e.persistEvaluation(ctx, tx, record, snapshot, decided, guards,
		protocol.OrchestratorActionInjectPTYPrompt, protocol.ExecutionStatusPending)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	deliveryID, err := e.mintOpaqueID("dlv", e.clock.Now())
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	claimUntil := eventstore.FormatTime(e.clock.Now().Add(2 * e.timeout))
	result, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET
		state = 'CLAIMED', attempts = attempts + 1, claimed_by = ?, claim_until = ?, updated_at = ?
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ? AND state = 'QUEUED'`,
		e.workerID, claimUntil, eventstore.FormatTime(e.clock.Now()),
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("claim approved delivery: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("claim approved delivery rows affected: %w", err)
	}
	if changed != 1 {
		return protocol.ApprovalRequest{}, fmt.Errorf("claim approved delivery: %w", daemon.ErrInvalidAttempt)
	}
	spec := daemon.AttemptSpec{
		DeliveryID: deliveryID, MessageID: record.MessageID, AgentID: record.AgentID,
		DeliveryGeneration: record.DeliveryGeneration, AdapterSessionID: record.sessionID,
		OrchestratorDecisionID: persisted.decisionID, ChannelDecisionID: decided.ID(),
		DraftGeneration:   snapshot.LastReport.DraftGeneration,
		InputGuardVersion: snapshot.LastReport.InputGuardVersion,
		ControlVersions:   decided.Versions(),
	}
	var attempt daemon.Attempt
	if uncertain != nil {
		attempt, err = e.attempts.CreateRecovery(ctx, tx, spec, uncertain.DeliveryID)
	} else {
		attempt, err = e.attempts.Create(ctx, tx, spec)
	}
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("create approved delivery attempt: %w", err)
	}
	if uncertain != nil {
		if err := e.attempts.ResolveUncertain(ctx, tx, uncertain.DeliveryID, request.ApprovalID); err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("resolve prior uncertain attempt: %w", err)
		}
	}
	resolved, err := e.approvals.ResolveOrchestratorApproval(ctx, tx, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve delivery approval: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("commit approved delivery plan: %w", err)
	}
	plan := executionPlan{
		record: record, decision: decided, attempt: attempt, message: message,
		ref: protocol.MessageRef{
			MessageID: record.MessageID, FromAgent: record.fromAgent,
			Priority: record.priority, ThreadID: record.threadID,
		},
		report: snapshot.LastReport,
	}
	if err := e.execute(ctx, plan); err != nil {
		return resolved, err
	}
	return resolved, nil
}

func (e *Executor) rejectApproval(
	ctx context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("begin rejected approval: %w", err)
	}
	defer rollback(tx)
	request, err := e.approvals.ApprovalTx(ctx, tx, params.ApprovalID)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("load rejected approval: %w", err)
	}
	replay, err := approvalReplay(request, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	if replay {
		return request, nil
	}
	_, uncertain, err := e.approvalCandidate(ctx, tx, request)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	messageID := protocol.ULID(request.ObjectID)
	_, err = tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'CANCELLED',
		claimed_by = NULL, claim_until = NULL, next_attempt_at = NULL, updated_at = ?
		WHERE message_id = ? AND agent_id = ? AND state <> 'DELIVERED'`,
		eventstore.FormatTime(e.clock.Now()), messageID, request.AgentID)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("cancel rejected recipient: %w", err)
	}
	if uncertain != nil {
		if err := e.attempts.ResolveUncertain(ctx, tx, uncertain.DeliveryID, request.ApprovalID); err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("resolve rejected uncertain attempt: %w", err)
		}
	}
	resolved, err := e.approvals.ResolveOrchestratorApproval(ctx, tx, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve rejected approval: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("commit rejected approval: %w", err)
	}
	return resolved, nil
}

func (e *Executor) approvalCandidate(
	ctx context.Context,
	tx *sql.Tx,
	request protocol.ApprovalRequest,
) (daemon.QueueDelivery, *daemon.Attempt, error) {
	messageID := protocol.ULID(request.ObjectID)
	if request.Source != protocol.ApprovalSourceOrchestrator ||
		request.ObjectType != protocol.ApprovalObjectTypeMessage ||
		request.State != protocol.ApprovalStatePending || !messageID.Valid() || !request.AgentID.Valid() {
		return daemon.QueueDelivery{}, nil, fmt.Errorf("approval candidate: %w", channel.ErrInvalidQuery)
	}
	var uncertain *daemon.Attempt
	linked, err := e.attempts.UnresolvedForApproval(ctx, tx, request.ApprovalID)
	if err == nil {
		if linked.MessageID != messageID || linked.AgentID != request.AgentID {
			return daemon.QueueDelivery{}, nil, fmt.Errorf("approval target mismatch: %w", channel.ErrInvalidQuery)
		}
		uncertain = &linked
	} else if !errors.Is(err, daemon.ErrAttemptNotFound) {
		return daemon.QueueDelivery{}, nil, fmt.Errorf("load uncertain approval target: %w", err)
	}
	var candidate daemon.QueueDelivery
	candidate.MessageID = messageID
	candidate.AgentID = request.AgentID
	if uncertain != nil {
		candidate.DeliveryGeneration = uncertain.DeliveryGeneration
		err = tx.QueryRowContext(ctx, `SELECT delivery_generation FROM delivery_queue
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?
			AND state NOT IN ('DELIVERED', 'EXPIRED', 'CANCELLED')`,
			messageID, request.AgentID, uncertain.DeliveryGeneration).Scan(&candidate.DeliveryGeneration)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT delivery_generation FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND state NOT IN ('DELIVERED', 'EXPIRED', 'CANCELLED')
		ORDER BY delivery_generation DESC LIMIT 1`, messageID, request.AgentID).
			Scan(&candidate.DeliveryGeneration)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return daemon.QueueDelivery{}, nil, channel.ErrNotFound
	}
	if err != nil {
		return daemon.QueueDelivery{}, nil, fmt.Errorf("select approval delivery: %w", err)
	}
	return candidate, uncertain, nil
}

func approvalReplay(
	request protocol.ApprovalRequest,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (bool, error) {
	if request.State == protocol.ApprovalStatePending {
		return false, nil
	}
	if actor.Type == protocol.ActorTypeHuman && actor.ID != "" &&
		request.Source == protocol.ApprovalSourceOrchestrator && request.Resolution != nil &&
		(request.State == protocol.ApprovalStateApproved || request.State == protocol.ApprovalStateRejected) &&
		request.Resolution.Option == params.Option && request.Resolution.Reason == params.Reason &&
		request.ResolvedBy == actor.ID {
		return true, nil
	}
	return false, fmt.Errorf("approval resolve replay mismatch: %w", channel.ErrInvalidQuery)
}

func (e *Executor) execute(ctx context.Context, plan executionPlan) error {
	if !plan.decision.Allowed() {
		return fmt.Errorf("execute without channel allow: %w", channel.ErrDenied)
	}
	client, ok := e.links.Client(plan.record.AgentID)
	if !ok {
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptFailed, daemon.ReasonDisconnected,
			protocol.ExecutionStatusFailed)
	}
	prepare := protocol.PrepareDelivery{
		DeliveryID: plan.attempt.DeliveryID, AgentID: plan.record.AgentID,
		MessageRef: plan.ref, NoticeLine: e.notice,
		OrchestratorDecisionID: plan.attempt.OrchestratorDecisionID,
		IdempotencyKey:         plan.attempt.DeliveryID, ExpectedStateVersion: 1,
		ExpectedInputGuardVersion: plan.report.InputGuardVersion,
		ExpectedDraftGeneration:   plan.report.DraftGeneration,
		ControlVersions:           plan.decision.Versions(),
	}
	prepareStarted := e.clock.Now()
	prepared, err := e.callDelivery(ctx, client, protocol.AgentOpPrepareDelivery, prepare, plan)
	if err != nil {
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptFailed, callReason(err),
			protocol.ExecutionStatusFailed)
	}
	if prepared.Result != protocol.DeliveryResultKindPrepared {
		return e.finishPrepareResult(ctx, plan, prepared)
	}
	lease, err := e.mintOpaqueID("lease", e.clock.Now())
	if err != nil {
		return err
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin mark prepared: %w", err)
	}
	if err := e.attempts.MarkPrepared(ctx, tx, plan.attempt.DeliveryID, lease); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark delivery prepared: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prepared attempt: %w", err)
	}
	plan.attempt.State = daemon.AttemptPrepared
	plan.attempt.WriterLeaseID = lease
	tx, err = e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delivery revalidation: %w", err)
	}
	cancellation, err := e.revalidatePreparedGuards(ctx, tx, plan, prepareStarted)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if cancellation != nil {
		_ = tx.Rollback()
		return e.cancelPrepared(ctx, plan, cancellation.state, cancellation.reason,
			cancellation.cancelReason, cancellation.queueState)
	}
	err = e.channel.Revalidate(ctx, tx, plan.decision, plan.message, plan.record.DeliveryGeneration)
	if err != nil {
		var changed *channel.RevalidationError
		if errors.As(err, &changed) {
			if commitErr := tx.Commit(); commitErr != nil {
				return fmt.Errorf("commit control revalidation failure: %w", commitErr)
			}
			return e.cancelPrepared(ctx, plan, daemon.AttemptCancelled,
				daemon.ReasonControlChanged, protocol.CancelReasonControlChanged, queueStateQueued)
		}
		_ = tx.Rollback()
		return fmt.Errorf("revalidate PTY delivery: %w", err)
	}
	if err := e.attempts.MarkCommitSent(ctx, tx, plan.attempt.DeliveryID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark delivery commit sent: %w", err)
	}
	queueResult, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'DELIVERING', updated_at = ?
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ? AND state = 'CLAIMED'`,
		eventstore.FormatTime(e.clock.Now()), plan.record.MessageID, plan.record.AgentID,
		plan.record.DeliveryGeneration)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark queue delivering: %w", err)
	}
	changed, err := queueResult.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark queue delivering rows affected: %w", err)
	}
	if changed != 1 {
		_ = tx.Rollback()
		return fmt.Errorf("mark queue delivering: %w", daemon.ErrInvalidAttempt)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit COMMIT_SENT: %w", err)
	}
	committed, err := e.callDelivery(ctx, client, protocol.AgentOpCommitDelivery,
		protocol.CommitDelivery{DeliveryID: plan.attempt.DeliveryID, WriterLeaseID: lease}, plan)
	if err != nil {
		return e.markUncertain(ctx, plan, callReason(err))
	}
	return e.finishCommitResult(ctx, plan, committed)
}

func (e *Executor) callDelivery(
	ctx context.Context,
	client *agentlink.Client,
	op protocol.AgentOp,
	body any,
	plan executionPlan,
) (protocol.DeliveryResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	frame, err := client.Call(callCtx, op, body)
	if err != nil {
		return protocol.DeliveryResult{}, fmt.Errorf("call agent delivery operation %s: %w", op, err)
	}
	if frame.Op != protocol.AgentOpReportDeliveryResult || frame.ReplyTo == nil {
		return protocol.DeliveryResult{}, agentlink.ErrUnexpectedFrame
	}
	var result protocol.DeliveryResult
	if err := json.Unmarshal(frame.Body, &result); err != nil {
		return protocol.DeliveryResult{}, fmt.Errorf("decode delivery result: %w", err)
	}
	if err := result.Validate(); err != nil {
		return protocol.DeliveryResult{}, fmt.Errorf("validate delivery result: %w", err)
	}
	if result.DeliveryID != plan.attempt.DeliveryID || result.AgentID != plan.record.AgentID ||
		!maps.Equal(result.ControlVersions, plan.attempt.ControlVersions) {
		return protocol.DeliveryResult{}, agentlink.ErrUnexpectedFrame
	}
	return result, nil
}

func (e *Executor) finishPrepareResult(
	ctx context.Context,
	plan executionPlan,
	result protocol.DeliveryResult,
) error {
	switch result.Result {
	case protocol.DeliveryResultKindAbortedHumanPriority:
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptAbortedHumanPriority,
			daemon.ReasonHumanPriority, protocol.ExecutionStatusAborted)
	case protocol.DeliveryResultKindCancelled:
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptCancelled,
			daemon.AttemptReason(result.CancelReason), protocol.ExecutionStatusAborted)
	case protocol.DeliveryResultKindFailed:
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptFailed,
			"PREPARE_FAILED", protocol.ExecutionStatusFailed)
	case protocol.DeliveryResultKindPrepared:
		return nil
	case protocol.DeliveryResultKindCommitted, protocol.DeliveryResultKindCommitNotConsumed:
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptFailed,
			"UNEXPECTED_PREPARE_RESULT", protocol.ExecutionStatusFailed)
	default:
		return e.finishBeforeCommit(ctx, plan, daemon.AttemptFailed,
			"UNEXPECTED_PREPARE_RESULT", protocol.ExecutionStatusFailed)
	}
}

func (e *Executor) finishBeforeCommit(
	ctx context.Context,
	plan executionPlan,
	state daemon.AttemptState,
	reason daemon.AttemptReason,
	status protocol.ExecutionStatus,
) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pre-commit result: %w", err)
	}
	defer rollback(tx)
	if err := e.attempts.Transition(ctx, tx, plan.attempt.DeliveryID, daemon.Transition{
		From: daemon.AttemptPreparing, To: state, Reason: reason,
	}); err != nil {
		return fmt.Errorf("finish pre-commit delivery attempt: %w", err)
	}
	if err := requeue(ctx, tx, plan.record, e.clock.Now()); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, plan.attempt.OrchestratorDecisionID, status); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pre-commit result: %w", err)
	}
	return nil
}

func (e *Executor) revalidatePreparedGuards(
	ctx context.Context,
	tx *sql.Tx,
	plan executionPlan,
	preparedAt time.Time,
) (*preparedCancellation, error) {
	humanPriority := func() *preparedCancellation {
		return &preparedCancellation{
			state: daemon.AttemptAbortedHumanPriority, reason: daemon.ReasonHumanPriority,
			cancelReason: protocol.CancelReasonHumanPriority, queueState: queueStateQueued,
		}
	}
	stateChanged := func(queueState string) *preparedCancellation {
		return &preparedCancellation{
			state: daemon.AttemptCancelled, reason: daemon.AttemptReason(protocol.CancelReasonStateChanged),
			cancelReason: protocol.CancelReasonStateChanged, queueState: queueState,
		}
	}

	snapshot, ok := e.links.Snapshot(plan.record.AgentID)
	if !ok || snapshot.State != agentlink.StateUp || snapshot.SessionID != plan.record.sessionID ||
		!snapshot.HasReport || snapshot.LastReport.AgentID != plan.record.AgentID ||
		snapshot.LastReport.SessionID != plan.record.sessionID {
		return humanPriority(), nil
	}
	report := snapshot.LastReport
	if report.InputGuardVersion != plan.report.InputGuardVersion ||
		report.DraftGeneration != plan.report.DraftGeneration || report.DraftState != protocol.DraftStateClosed {
		return humanPriority(), nil
	}
	if report.ActivityState == protocol.ActivityStateActive {
		return humanPriority(), nil
	}
	if !report.LastHumanKeyAt.IsZero() && report.LastHumanKeyAt.After(preparedAt) {
		return humanPriority(), nil
	}

	var queueState, expiresAt, sessionState, captureMode, processJSON string
	var deliverable bool
	err := tx.QueryRowContext(ctx, `SELECT dq.state, dq.expires_at,
		s.state, s.capture_mode, s.deliverable, s.process_json
		FROM delivery_queue AS dq
		JOIN sessions AS s ON s.session_id = ? AND s.agent_id = dq.agent_id
		WHERE dq.message_id = ? AND dq.agent_id = ? AND dq.delivery_generation = ?`,
		plan.record.sessionID, plan.record.MessageID, plan.record.AgentID,
		plan.record.DeliveryGeneration).Scan(
		&queueState, &expiresAt, &sessionState, &captureMode, &deliverable, &processJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return stateChanged(queueStateQueued), nil
	}
	if err != nil {
		return nil, fmt.Errorf("reload prepared delivery guards: %w", err)
	}
	if queueState != queueStateClaimed || sessionState != string(protocol.SessionStateConnected) ||
		captureMode != string(protocol.CaptureModePTYProxy) || !deliverable {
		return stateChanged(queueStateQueued), nil
	}
	process, valid := decodeProcessIdentity(processJSON)
	if !valid || process != plan.record.process {
		return stateChanged(queueStateQueued), nil
	}
	if !processGuardPasses(e.processes, process) {
		return stateChanged(queueStateQueued), nil
	}
	expires, valid := parseDeliveryExpiry(expiresAt)
	if !valid {
		return stateChanged(queueStateQueued), nil
	}
	if !expires.After(e.clock.Now()) {
		return stateChanged(queueStateExpired), nil
	}
	hasUncertain, err := e.attempts.HasUncertain(ctx, tx, plan.record.AgentID)
	if err != nil {
		return nil, fmt.Errorf("recheck unresolved uncertain attempts: %w", err)
	}
	if hasUncertain {
		return stateChanged(queueStateQueued), nil
	}
	return nil, nil
}

func (e *Executor) cancelPrepared(
	ctx context.Context,
	plan executionPlan,
	state daemon.AttemptState,
	reason daemon.AttemptReason,
	cancelReason protocol.CancelReason,
	queueState string,
) error {
	if client, ok := e.links.Client(plan.record.AgentID); ok {
		_, _ = e.callDelivery(ctx, client, protocol.AgentOpCancelDelivery,
			protocol.CancelDelivery{DeliveryID: plan.attempt.DeliveryID, Reason: cancelReason}, plan)
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cancel prepared: %w", err)
	}
	defer rollback(tx)
	err = e.attempts.Transition(ctx, tx, plan.attempt.DeliveryID, daemon.Transition{
		From: daemon.AttemptPrepared, To: state, Reason: reason,
	})
	if err != nil {
		var stale *daemon.TransitionError
		if !errors.As(err, &stale) || !attemptTerminal(stale.Actual) {
			return fmt.Errorf("cancel prepared delivery attempt: %w", err)
		}
		return nil
	}
	if err := setQueueState(ctx, tx, plan.record, queueState, e.clock.Now()); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, plan.attempt.OrchestratorDecisionID,
		protocol.ExecutionStatusAborted); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cancel prepared: %w", err)
	}
	return nil
}

func (e *Executor) finishCommitResult(
	ctx context.Context,
	plan executionPlan,
	result protocol.DeliveryResult,
) error {
	if result.Result == protocol.DeliveryResultKindFailed || result.Result == protocol.DeliveryResultKindPrepared ||
		!result.Result.Valid() {
		return e.markUncertain(ctx, plan, "COMMIT_RESULT_UNKNOWN")
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin commit result: %w", err)
	}
	defer rollback(tx)
	transition := daemon.Transition{From: daemon.AttemptCommitSent}
	queueState := "QUEUED"
	status := protocol.ExecutionStatusAborted
	switch result.Result {
	case protocol.DeliveryResultKindCommitted:
		transition.To = daemon.AttemptCommitted
		queueState = "DELIVERED"
		status = protocol.ExecutionStatusExecuted
	case protocol.DeliveryResultKindCommitNotConsumed:
		transition.To = daemon.AttemptCommitNotConsumed
	case protocol.DeliveryResultKindAbortedHumanPriority:
		transition.To = daemon.AttemptAbortedHumanPriority
		transition.Reason = daemon.ReasonHumanPriority
	case protocol.DeliveryResultKindCancelled:
		transition.To = daemon.AttemptCancelled
		transition.Reason = daemon.AttemptReason(result.CancelReason)
	case protocol.DeliveryResultKindFailed, protocol.DeliveryResultKindPrepared:
		return fmt.Errorf("unreachable uncertain commit result")
	default:
		return fmt.Errorf("unsupported commit result %s", result.Result)
	}
	if err := e.attempts.Transition(ctx, tx, plan.attempt.DeliveryID, transition); err != nil {
		return fmt.Errorf("finish committed delivery attempt: %w", err)
	}
	if err := setQueueState(ctx, tx, plan.record, queueState, e.clock.Now()); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, plan.attempt.OrchestratorDecisionID, status); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delivery result: %w", err)
	}
	return nil
}

func (e *Executor) markUncertain(
	ctx context.Context,
	plan executionPlan,
	reason daemon.AttemptReason,
) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin uncertain commit: %w", err)
	}
	defer rollback(tx)
	if err := e.attempts.MarkUncertain(ctx, tx, plan.attempt.DeliveryID, reason); err != nil {
		return fmt.Errorf("mark delivery outcome uncertain: %w", err)
	}
	if err := requeue(ctx, tx, plan.record, e.clock.Now()); err != nil {
		return err
	}
	if err := e.createUncertainApproval(ctx, tx, plan.attempt.DeliveryID,
		plan.record.MessageID, plan.record.AgentID, plan.record.DeliveryGeneration,
		"commit outcome uncertain"); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, plan.attempt.OrchestratorDecisionID,
		protocol.ExecutionStatusFailed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit uncertain result: %w", err)
	}
	return nil
}

// ControlChanged immediately cancels all PREPARED attempts for agent.
func (e *Executor) ControlChanged(ctx context.Context, agent protocol.AgentID) error {
	return e.cancelPreparedForAgent(ctx, agent, true)
}

// PauseAutomation first tells the Proxy to pause, then closes every durable
// PREPARED attempt. Pause never changes an UNCERTAIN row.
func (e *Executor) PauseAutomation(ctx context.Context, agent protocol.AgentID) error {
	if !agent.Valid() {
		return ErrInvalidOptions
	}
	var pauseErr error
	if client, ok := e.links.Client(agent); ok {
		pauseErr = client.Pause(ctx)
	}
	cancelErr := e.cancelPreparedForAgent(ctx, agent, false)
	return errors.Join(pauseErr, cancelErr)
}

func (e *Executor) cancelPreparedForAgent(
	ctx context.Context,
	agent protocol.AgentID,
	sendCancel bool,
) error {
	if !agent.Valid() {
		return ErrInvalidOptions
	}
	rows, err := e.db.QueryContext(ctx, `SELECT delivery_id, message_id, delivery_generation,
		orchestrator_decision_id FROM delivery_attempts
		WHERE agent_id = ? AND state = 'PREPARED' ORDER BY created_at, delivery_id`, agent)
	if err != nil {
		return fmt.Errorf("select prepared attempts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type preparedRow struct {
		deliveryID protocol.OpaqueID
		messageID  protocol.ULID
		generation int64
		decisionID protocol.ULID
	}
	var found []preparedRow
	for rows.Next() {
		var row preparedRow
		if err := rows.Scan(&row.deliveryID, &row.messageID, &row.generation, &row.decisionID); err != nil {
			return fmt.Errorf("scan prepared attempt: %w", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read prepared attempts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close prepared attempts: %w", err)
	}
	var joined error
	for _, row := range found {
		if sendCancel {
			if client, ok := e.links.Client(agent); ok {
				callCtx, cancel := context.WithTimeout(ctx, e.timeout)
				_, callErr := client.Call(callCtx, protocol.AgentOpCancelDelivery,
					protocol.CancelDelivery{DeliveryID: row.deliveryID, Reason: protocol.CancelReasonControlChanged})
				cancel()
				if callErr != nil {
					joined = errors.Join(joined, callErr)
				}
			}
		}
		tx, err := e.db.BeginTx(ctx, nil)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		err = e.attempts.Transition(ctx, tx, row.deliveryID, daemon.Transition{
			From: daemon.AttemptPrepared, To: daemon.AttemptCancelled,
			Reason: daemon.ReasonControlChanged,
		})
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'QUEUED',
				claimed_by = NULL, claim_until = NULL, updated_at = ?
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
				eventstore.FormatTime(e.clock.Now()), row.messageID, agent, row.generation)
		}
		if err == nil {
			err = updateExecution(ctx, tx, row.decisionID, protocol.ExecutionStatusAborted)
		}
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

// Recover converts restart-stranded attempts and opens an approval for every
// COMMIT_SENT row that became UNCERTAIN in the same transaction.
func (e *Executor) Recover(ctx context.Context) (int64, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin attempt recovery: %w", err)
	}
	defer rollback(tx)
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id, message_id, agent_id,
		delivery_generation, state, orchestrator_decision_id FROM delivery_attempts
		WHERE state IN ('PREPARING', 'PREPARED', 'COMMIT_SENT')
		ORDER BY created_at, delivery_id`)
	if err != nil {
		return 0, fmt.Errorf("select restart attempts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type recoveryRow struct {
		deliveryID protocol.OpaqueID
		messageID  protocol.ULID
		agentID    protocol.AgentID
		generation int64
		state      daemon.AttemptState
		decisionID protocol.ULID
	}
	var found []recoveryRow
	for rows.Next() {
		var row recoveryRow
		if err := rows.Scan(&row.deliveryID, &row.messageID, &row.agentID,
			&row.generation, &row.state, &row.decisionID); err != nil {
			return 0, fmt.Errorf("scan restart attempt: %w", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read restart attempts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close restart attempts: %w", err)
	}
	count, err := e.attempts.RecoverCommitSent(ctx, tx, daemon.ReasonProcessRestart)
	if err != nil {
		return 0, fmt.Errorf("recover commit-sent attempts: %w", err)
	}
	for _, row := range found {
		_, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'QUEUED',
			claimed_by = NULL, claim_until = NULL, updated_at = ?
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
			eventstore.FormatTime(e.clock.Now()), row.messageID, row.agentID, row.generation)
		if err != nil {
			return 0, fmt.Errorf("requeue restart attempt: %w", err)
		}
		status := protocol.ExecutionStatusAborted
		if row.state == daemon.AttemptCommitSent {
			status = protocol.ExecutionStatusFailed
			if err := e.createUncertainApproval(ctx, tx, row.deliveryID,
				row.messageID, row.agentID, row.generation,
				"commit outcome uncertain after restart"); err != nil {
				return 0, err
			}
		}
		if err := updateExecution(ctx, tx, row.decisionID, status); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit attempt recovery: %w", err)
	}
	return count, nil
}

func (e *Executor) createUncertainApproval(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
	messageID protocol.ULID,
	agent protocol.AgentID,
	generation int64,
	detail string,
) error {
	note := fmt.Sprintf("%s; delivery %s generation %d may already have executed",
		detail, deliveryID, generation)
	request, err := e.approvals.CreateOrchestratorApproval(
		ctx, tx, messageID, agent, truncate(note, 500))
	if err != nil {
		return fmt.Errorf("create uncertain delivery approval: %w", err)
	}
	if err := e.attempts.BindUncertainApproval(ctx, tx, deliveryID, request.ApprovalID); err != nil {
		return fmt.Errorf("link uncertain delivery approval: %w", err)
	}
	return nil
}

func (e *Executor) evaluateGuards(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	snapshot agentlink.Snapshot,
	manual bool,
	recoveryOf protocol.OpaqueID,
) ([]protocol.GuardResult, error) {
	guards := make([]protocol.GuardResult, 0, 10)
	guards = append(guards,
		guard(protocol.GuardNameSessionConnected,
			record.sessionState == protocol.SessionStateConnected, "session is not CONNECTED"),
		guard(protocol.GuardNameDeliverable,
			record.deliverable && record.captureMode == protocol.CaptureModePTYProxy,
			"session is not a deliverable PTY Proxy"),
	)
	linkFresh := snapshot.State == agentlink.StateUp && snapshot.SessionID == record.sessionID &&
		snapshot.HasReport && snapshot.LastReport.AgentID == record.AgentID &&
		snapshot.LastReport.SessionID == record.sessionID
	guards = append(guards, guard(protocol.GuardNameAdapterConnected, linkFresh,
		"agent link has no fresh high-water report"))
	if linkFresh {
		guards = append(guards,
			guard(protocol.GuardNameHumanDraftClosed,
				snapshot.LastReport.DraftState == protocol.DraftStateClosed, "human draft is not CLOSED"),
			guard(protocol.GuardNameHumanActivityNotActive,
				snapshot.LastReport.ActivityState != protocol.ActivityStateActive, "human activity is ACTIVE"),
		)
		processes, err := e.processes.Descendants(record.process)
		if err != nil {
			guards = append(guards, guard(protocol.GuardNameNoNewChildOrTool, false,
				truncate(err.Error(), 200)))
		} else {
			// 5.3c has no trusted "harness is fully idle" signal. Therefore no
			// descendant is learned as normal. The admitted root set contains only
			// the harness identity; 5.3d may add the registered Gifford endpoint
			// without granting anything to its children.
			guards = append(guards, guard(protocol.GuardNameNoNewChildOrTool,
				processesWithinRoots(record.process, processes),
				"harness descendant present; idle process baseline is not confirmed"))
		}
	} else {
		guards = append(guards,
			guard(protocol.GuardNameHumanDraftClosed, false, "draft state is unavailable"),
			guard(protocol.GuardNameHumanActivityNotActive, false, "activity state is unavailable"),
			guard(protocol.GuardNameNoNewChildOrTool, false, "process state is unavailable"),
		)
	}
	guards = append(guards,
		guard(protocol.GuardNameNotExpired, record.expiresAt.After(e.clock.Now()), "delivery expired"),
	)
	var hasUncertain bool
	var err error
	if recoveryOf.Valid() {
		hasUncertain, err = e.attempts.HasOtherUncertain(ctx, tx, record.AgentID, recoveryOf)
	} else {
		hasUncertain, err = e.attempts.HasUncertain(ctx, tx, record.AgentID)
	}
	if err != nil {
		return nil, fmt.Errorf("check unresolved uncertain attempts: %w", err)
	}
	guards = append(guards, guard(protocol.GuardNameNotDuplicate,
		!hasUncertain, "agent has another unresolved UNCERTAIN commit"))
	probability := guard(protocol.GuardNamePIdleThreshold, false, "no orchestration model is available")
	if manual {
		probability.Outcome = protocol.GuardOutcomeOverridden
		probability.Detail = "manual deliver_now order"
	}
	guards = append(guards, probability)
	return guards, nil
}

func processesWithinRoots(process protocol.ProcessIdentity, descendants []ProcessToken) bool {
	allowedRoots := map[ProcessToken]struct{}{
		{PID: process.PID, StartTime: process.ProcessStartTime}: {},
	}
	observed := make([]ProcessToken, 0, len(descendants)+1)
	observed = append(observed, ProcessToken{PID: process.PID, StartTime: process.ProcessStartTime})
	observed = append(observed, descendants...)
	for _, identity := range observed {
		if _, allowed := allowedRoots[identity]; !allowed {
			return false
		}
	}
	return true
}

func decodeProcessIdentity(raw string) (protocol.ProcessIdentity, bool) {
	var process protocol.ProcessIdentity
	if json.Unmarshal([]byte(raw), &process) != nil || process.Validate() != nil {
		return protocol.ProcessIdentity{}, false
	}
	return process, true
}

func processGuardPasses(inspector ProcessInspector, process protocol.ProcessIdentity) bool {
	descendants, err := inspector.Descendants(process)
	return err == nil && processesWithinRoots(process, descendants)
}

func parseDeliveryExpiry(raw string) (time.Time, bool) {
	expires, err := eventstore.ParseTime(raw)
	return expires, err == nil
}

func (e *Executor) persistEvaluation(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	snapshot agentlink.Snapshot,
	decided channel.Decision,
	guards []protocol.GuardResult,
	selected protocol.OrchestratorAction,
	status protocol.ExecutionStatus,
) (persistedEvaluation, error) {
	now := e.clock.Now().UTC()
	stateID, err := e.mintULID(now)
	if err != nil {
		return persistedEvaluation{}, err
	}
	decisionID, err := e.mintULID(now)
	if err != nil {
		return persistedEvaluation{}, err
	}
	fresh := snapshot.State == agentlink.StateUp && snapshot.SessionID == record.sessionID && snapshot.HasReport
	draft := protocol.DraftStateUnknown
	activity := protocol.ActivityStateUnknown
	draftGeneration := int64(0)
	guardVersion := int64(0)
	if fresh {
		draft = snapshot.LastReport.DraftState
		activity = snapshot.LastReport.ActivityState
		draftGeneration = snapshot.LastReport.DraftGeneration
		guardVersion = snapshot.LastReport.InputGuardVersion
	}
	health := protocol.HealthStateDisconnected
	if snapshot.State == agentlink.StateUp {
		health = protocol.HealthStateHealthy
	}
	confidence := `{"human_draft_state":1,"agent_phase":0,"attention_state":0,"delivery_safety":0}`
	_, err = tx.ExecContext(ctx, `INSERT INTO state_snapshots (
		state_id, agent_id, observed_at, session_state, human_draft_state,
		human_activity_state, draft_generation, input_guard_version, agent_phase,
		attention_state, health_state, delivery_safety, confidence_json,
		evidence_json, analyst_version, stale
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?)`,
		stateID, record.AgentID, eventstore.FormatTime(now), record.sessionState,
		draft, activity, draftGeneration, guardVersion, protocol.AgentPhaseUnknown,
		protocol.AttentionStateNone, health, protocol.DeliverySafetyUncertain,
		confidence, "none-5.3c", !fresh)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("insert state snapshot: %w", err)
	}
	allowed := []protocol.OrchestratorAction{protocol.OrchestratorActionDefer}
	if selected != protocol.OrchestratorActionDefer {
		allowed = append([]protocol.OrchestratorAction{selected}, allowed...)
	}
	allowedJSON, err := json.Marshal(allowed)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("encode allowed actions: %w", err)
	}
	if guards == nil {
		guards = []protocol.GuardResult{}
	}
	guardsJSON, err := json.Marshal(guards)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("encode hard guards: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO orchestrator_decisions (
		decision_id, agent_id, message_id, artifact_id, state_id,
		allowed_actions_json, selected_action, probabilities_json, hard_guards_json,
		policy_version, decision_model, glimmer_call_id, channel_decision_id,
		created_at, execution_status
	) VALUES (?, ?, ?, NULL, ?, ?, ?, NULL, ?, ?, NULL, NULL, ?, ?, ?)`,
		decisionID, record.AgentID, record.MessageID, stateID, string(allowedJSON),
		selected, string(guardsJSON), e.policy, decided.ID(), eventstore.FormatTime(now), status)
	if err != nil {
		return persistedEvaluation{}, fmt.Errorf("insert orchestrator decision: %w", err)
	}
	return persistedEvaluation{decisionID: decisionID}, nil
}

func (e *Executor) loadDelivery(
	ctx context.Context,
	tx *sql.Tx,
	candidate daemon.QueueDelivery,
	sessionID protocol.OpaqueID,
) (deliveryRecord, error) {
	if tx == nil || !candidate.MessageID.Valid() || !candidate.AgentID.Valid() ||
		candidate.DeliveryGeneration < 1 || !sessionID.Valid() {
		return deliveryRecord{}, fmt.Errorf("load delivery: %w", ErrInvalidOptions)
	}
	var record deliveryRecord
	record.QueueDelivery = candidate
	var threadID, parentID, processJSON sql.NullString
	var expiresAt string
	err := tx.QueryRowContext(ctx, `SELECT dq.from_agent, dq.state, dq.expires_at,
		m.origin_node, m.destination_node, m.thread_id, m.parent_message_id,
		m.hop_count, m.priority, s.session_id, s.state, s.capture_mode,
		s.deliverable, s.process_json
		FROM delivery_queue AS dq
		JOIN messages AS m ON m.message_id = dq.message_id
		JOIN sessions AS s ON s.session_id = ? AND s.agent_id = dq.agent_id
		WHERE dq.message_id = ? AND dq.agent_id = ? AND dq.delivery_generation = ?`,
		sessionID, candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(
		&record.fromAgent, &record.queueState, &expiresAt,
		&record.originNode, &record.destinationNode, &threadID, &parentID,
		&record.hopCount, &record.priority, &record.sessionID, &record.sessionState,
		&record.captureMode, &record.deliverable, &processJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return deliveryRecord{}, channel.ErrNotFound
	}
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("select delivery candidate: %w", err)
	}
	record.threadID = protocol.ThreadID(threadID.String)
	record.parentID = protocol.ULID(parentID.String)
	record.expiresAt, err = eventstore.ParseTime(expiresAt)
	if err != nil {
		return deliveryRecord{}, fmt.Errorf("parse delivery expiry: %w", err)
	}
	if !processJSON.Valid {
		return deliveryRecord{}, fmt.Errorf("decode session process: %w", ErrInvalidOptions)
	}
	if err := json.Unmarshal([]byte(processJSON.String), &record.process); err != nil {
		return deliveryRecord{}, fmt.Errorf("decode session process: %w", err)
	}
	if err := record.process.Validate(); err != nil {
		return deliveryRecord{}, fmt.Errorf("validate session process: %w", err)
	}
	if !record.fromAgent.Valid() || !record.originNode.Valid() ||
		!record.destinationNode.Valid() || !record.threadID.Valid() ||
		!record.priority.Valid() || record.queueState != "QUEUED" {
		return deliveryRecord{}, fmt.Errorf("invalid delivery candidate: %w", ErrInvalidOptions)
	}
	return record, nil
}

func (r deliveryRecord) channelMessage() channel.Message {
	return channel.Message{
		ID: r.MessageID, From: r.fromAgent, FromNode: r.originNode,
		To:     []channel.Recipient{{Agent: r.AgentID, Deliverable: r.deliverable, Policy: channel.PolicyAllowed}},
		ToNode: r.destinationNode, ParentID: r.parentID, ThreadID: r.threadID, HopCount: r.hopCount,
	}
}

func (e *Executor) mintOpaqueID(prefix string, now time.Time) (protocol.OpaqueID, error) {
	id, err := e.mintULID(now)
	if err != nil {
		return "", err
	}
	return protocol.OpaqueID(prefix + "-" + string(id)), nil
}

func (e *Executor) mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("mint ULID: time outside range")
	}
	e.idMu.Lock()
	defer e.idMu.Unlock()
	var entropy [10]byte
	if _, err := io.ReadFull(e.entropy, entropy[:]); err != nil {
		return "", fmt.Errorf("read ULID entropy: %w", err)
	}
	if millis == e.lastMillis && bytes.Compare(entropy[:], e.lastEntropy[:]) <= 0 {
		entropy = e.lastEntropy
		for index := len(entropy) - 1; index >= 0; index-- {
			entropy[index]++
			if entropy[index] != 0 {
				break
			}
		}
	}
	e.lastMillis = millis
	e.lastEntropy = entropy
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	copy(raw[6:], entropy[:])
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
	const digits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var out strings.Builder
	for out.Len()+len(encoded) < 26 {
		out.WriteByte('0')
	}
	for index := range len(encoded) {
		char := encoded[index]
		value := int(char - '0')
		if char >= 'a' {
			value = int(char-'a') + 10
		}
		out.WriteByte(digits[value])
	}
	return protocol.ULID(out.String()), nil
}

func guard(name protocol.GuardName, pass bool, detail string) protocol.GuardResult {
	outcome := protocol.GuardOutcomePass
	if !pass {
		outcome = protocol.GuardOutcomeFail
	}
	if pass {
		detail = ""
	}
	return protocol.GuardResult{Guard: name, Outcome: outcome, Detail: truncate(detail, 200)}
}

func firstFailedGuard(guards []protocol.GuardResult) string {
	for _, result := range guards {
		if result.Outcome == protocol.GuardOutcomeFail {
			if result.Detail != "" {
				return string(result.Guard) + ": " + result.Detail
			}
			return string(result.Guard)
		}
	}
	return ""
}

func truncate(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit])
}

func callReason(err error) daemon.AttemptReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return daemon.ReasonTimedOut
	}
	return daemon.ReasonDisconnected
}

func attemptTerminal(state daemon.AttemptState) bool {
	switch state {
	case daemon.AttemptCommitted, daemon.AttemptCommitNotConsumed,
		daemon.AttemptAbortedHumanPriority, daemon.AttemptCancelled,
		daemon.AttemptFailed, daemon.AttemptUncertain:
		return true
	case daemon.AttemptPreparing, daemon.AttemptPrepared, daemon.AttemptCommitSent:
		return false
	default:
		return false
	}
}

func expireQueue(ctx context.Context, tx *sql.Tx, record deliveryRecord, now time.Time) error {
	return setQueueState(ctx, tx, record, "EXPIRED", now)
}

func requeue(ctx context.Context, tx *sql.Tx, record deliveryRecord, now time.Time) error {
	return setQueueState(ctx, tx, record, "QUEUED", now)
}

func setQueueState(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	state string,
	now time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = ?, claimed_by = NULL,
		claim_until = NULL, next_attempt_at = NULL, updated_at = ?
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		state, eventstore.FormatTime(now), record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return fmt.Errorf("set delivery queue %s: %w", state, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set delivery queue %s rows affected: %w", state, err)
	}
	if changed != 1 {
		return fmt.Errorf("set delivery queue %s: %w", state, daemon.ErrInvalidAttempt)
	}
	return nil
}

func updateExecution(
	ctx context.Context,
	tx *sql.Tx,
	decisionID protocol.ULID,
	status protocol.ExecutionStatus,
) error {
	_, err := tx.ExecContext(ctx, `UPDATE orchestrator_decisions SET execution_status = ?
		WHERE decision_id = ?`, status, decisionID)
	if err != nil {
		return fmt.Errorf("update orchestrator execution: %w", err)
	}
	return nil
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
