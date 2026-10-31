package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultWorkerBatch = 64
	// defaultWorkerConcurrency bounds the agents evaluated at once. A
	// TRIAGE or CHOOSE call of one agent must not hold the others back.
	defaultWorkerConcurrency = 4
	// lowBatchWindow is how long a message the sender sent as LOW waits
	// before it is eligible. No other priority waits.
	lowBatchWindow = 30 * time.Second
	// maxQueueAge is the age from which a queued delivery is selected one
	// level up: HIGH and NORMAL join the top group, LOW joins NORMAL.
	maxQueueAge = 120 * time.Second
)

// QueueDelivery identifies one durable delivery_queue generation. The
// executor reloads all mutable metadata inside its own transaction.
type QueueDelivery struct {
	MessageID          protocol.ULID
	AgentID            protocol.AgentID
	DeliveryGeneration int64
}

// DeliveryProcessor evaluates automatic work: it delivers one queued
// delivery, defers it to the human, expires it or closes it as a
// duplicate. The worker holds the agent's in-flight mark while it runs,
// model calls included, so nothing else may wait on that mark.
type DeliveryProcessor interface {
	EvaluateAutomatic(context.Context, QueueDelivery) error
}

// ApprovalProcessor applies a manual orchestrator approval.
type ApprovalProcessor interface {
	ResolveApproval(context.Context, protocol.ResolveApprovalParams, channel.Actor) (protocol.ApprovalRequest, error)
}

// PreparedController reacts immediately to control changes: a restrictive
// one cancels prepared work and may pause the agent's Proxy, and an agent
// control that allows automatic delivery again resumes it.
type PreparedController interface {
	ControlChanged(context.Context, protocol.AgentID) error
	PauseAutomation(context.Context, protocol.AgentID) error
	ResumeAutomation(context.Context, protocol.AgentID) error
}

// ProxySnapshots supplies race-safe agent-link state without transferring
// ownership of a client to the worker.
type ProxySnapshots interface {
	Snapshot(protocol.AgentID) (agentlink.Snapshot, bool)
}

// WorkerOptions configures a delivery queue scan.
type WorkerOptions struct {
	DB         *sql.DB
	Processor  DeliveryProcessor
	Approvals  ApprovalProcessor
	Prepared   PreparedController
	Proxies    ProxySnapshots
	BatchLimit int
	// Clock dates the batch window and the queue age; nil is the system
	// clock.
	Clock clock.Clock
	// Concurrency bounds the agents evaluated at once; zero or less is
	// defaultWorkerConcurrency. One agent is never evaluated twice at once.
	Concurrency int
}

// Worker scans queued PTY recipients and serializes work per agent.
type Worker struct {
	db          *sql.DB
	processor   DeliveryProcessor
	approvals   ApprovalProcessor
	prepared    PreparedController
	proxies     ProxySnapshots
	batchLimit  int
	clock       clock.Clock
	concurrency int

	mu       sync.Mutex
	inFlight map[protocol.AgentID]bool
}

// NewWorker validates options. The daemon wiring starts recurring scans in
// 5.3d; this increment exposes deterministic RunOnce behavior.
func NewWorker(opts WorkerOptions) (*Worker, error) {
	if opts.DB == nil || opts.Processor == nil || opts.Approvals == nil ||
		opts.Prepared == nil || opts.Proxies == nil {
		return nil, ErrInvalidOptions
	}
	if opts.BatchLimit <= 0 {
		opts.BatchLimit = defaultWorkerBatch
	}
	if opts.Clock == nil {
		opts.Clock = clock.System{}
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultWorkerConcurrency
	}
	return &Worker{
		db: opts.DB, processor: opts.Processor, approvals: opts.Approvals,
		prepared: opts.Prepared, proxies: opts.Proxies, batchLimit: opts.BatchLimit,
		clock: opts.Clock, concurrency: opts.Concurrency,
		inFlight: make(map[protocol.AgentID]bool),
	}, nil
}

// RunOnce evaluates at most one queued row per live agent, and at most
// Concurrency agents at once. Rows with an already-pending orchestrator
// approval stay quiet until the human resolves it.
//
// Rows are taken by selection level, then age, and the level never
// rewrites the stored priority: a CONFLICT_OR_URGENT triage selects a row
// as HIGH, and a row queued maxQueueAge ago or more moves one level up,
// HIGH and NORMAL into the top group with URGENT and LOW to NORMAL, so a
// stream of fresh URGENT rows cannot starve it. The triage never lowers a
// row. Only a row its sender sent as LOW, and that the triage did not
// raise, waits lowBatchWindow. Independent agent errors are joined after
// the bounded scan.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock.Now()
	rows, err := w.db.QueryContext(ctx, `WITH eligible AS (
		SELECT dq.message_id, dq.agent_id, dq.delivery_generation, dq.created_at,
			MIN(dq.priority,
				CASE WHEN mt.label = 'CONFLICT_OR_URGENT' THEN 1 ELSE 3 END,
				CASE WHEN dq.created_at <= ? THEN (CASE WHEN dq.priority = 3 THEN 2 ELSE 0 END)
					ELSE 3 END) AS selection
		FROM delivery_queue AS dq
		LEFT JOIN message_triage AS mt ON mt.message_id = dq.message_id
		WHERE dq.state = 'QUEUED'
			AND (dq.priority <> 3 OR mt.label = 'CONFLICT_OR_URGENT' OR dq.created_at <= ?)
			AND EXISTS (SELECT 1 FROM sessions AS s WHERE s.agent_id = dq.agent_id
				AND s.state = 'CONNECTED' AND s.capture_mode = 'pty_proxy'
				AND s.deliverable = 1 AND s.ended_at IS NULL)
			AND NOT EXISTS (SELECT 1 FROM approval_requests AS ar
				WHERE ar.source = 'orchestrator' AND ar.object_type = 'message'
				AND ar.object_id = dq.message_id AND ar.agent_id = dq.agent_id
				AND ar.state = 'PENDING')
			AND NOT EXISTS (SELECT 1 FROM delivery_attempts AS da
				WHERE da.agent_id = dq.agent_id AND da.state = 'UNCERTAIN'
				AND da.resolution_approval_id IS NULL)
	), ranked AS (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_id
			ORDER BY selection, created_at, message_id, delivery_generation) AS agent_rank
		FROM eligible
	)
		SELECT message_id, agent_id, delivery_generation FROM ranked WHERE agent_rank = 1
		ORDER BY selection, created_at, message_id, delivery_generation
		LIMIT ?`, eventstore.FormatTime(now.Add(-maxQueueAge)),
		eventstore.FormatTime(now.Add(-lowBatchWindow)), w.batchLimit)
	if err != nil {
		return 0, fmt.Errorf("scan orchestrator queue: %w", err)
	}
	defer func() { _ = rows.Close() }()
	candidates := make([]QueueDelivery, 0, w.batchLimit)
	seen := make(map[protocol.AgentID]bool)
	for rows.Next() {
		var candidate QueueDelivery
		if err := rows.Scan(&candidate.MessageID, &candidate.AgentID, &candidate.DeliveryGeneration); err != nil {
			return 0, fmt.Errorf("scan orchestrator delivery: %w", err)
		}
		if seen[candidate.AgentID] {
			continue
		}
		seen[candidate.AgentID] = true
		candidateSnapshot, ok := w.proxies.Snapshot(candidate.AgentID)
		if !ok || candidateSnapshot.State != agentlink.StateUp {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read orchestrator queue: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close orchestrator queue: %w", err)
	}
	return w.evaluate(ctx, candidates)
}

// evaluate runs the candidates' evaluations, at most w.concurrency at
// once, each under its agent's in-flight mark, and joins their errors in
// candidate order. An agent already in flight is skipped. Nothing else
// waits on the mark: approvals, control changes and pauses never take it.
func (w *Worker) evaluate(ctx context.Context, candidates []QueueDelivery) (int, error) {
	errs := make([]error, len(candidates))
	ran := make([]bool, len(candidates))
	slots := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup
	for i, candidate := range candidates {
		if !w.acquire(candidate.AgentID) {
			continue
		}
		ran[i] = true
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			defer w.release(candidate.AgentID)
			errs[i] = w.processor.EvaluateAutomatic(ctx, candidate)
		}()
	}
	wg.Wait()
	processed := 0
	var joined error
	for i, candidate := range candidates {
		switch {
		case !ran[i]:
		case errs[i] != nil:
			joined = errors.Join(joined, fmt.Errorf("evaluate %s for %s: %w",
				candidate.MessageID, candidate.AgentID, errs[i]))
		default:
			processed++
		}
	}
	return processed, joined
}

// ResolveApproval applies an authenticated human choice.
func (w *Worker) ResolveApproval(
	ctx context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	request, err := w.approvals.ResolveApproval(ctx, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval: %w", err)
	}
	return request, nil
}

// ControlChanged cancels prepared work before a newly restrictive control can
// race a commit.
func (w *Worker) ControlChanged(ctx context.Context, agent protocol.AgentID) error {
	if err := w.prepared.ControlChanged(ctx, agent); err != nil {
		return fmt.Errorf("cancel prepared delivery after control change: %w", err)
	}
	return nil
}

// PauseAutomation sends the Proxy pause and closes every durable PREPARED
// attempt for the agent.
func (w *Worker) PauseAutomation(ctx context.Context, agent protocol.AgentID) error {
	if err := w.prepared.PauseAutomation(ctx, agent); err != nil {
		return fmt.Errorf("pause prepared delivery: %w", err)
	}
	return nil
}

// ResumeAutomation lifts the Proxy pause once the agent's confirmed control
// allows automatic delivery again.
func (w *Worker) ResumeAutomation(ctx context.Context, agent protocol.AgentID) error {
	if err := w.prepared.ResumeAutomation(ctx, agent); err != nil {
		return fmt.Errorf("resume automation: %w", err)
	}
	return nil
}

func (w *Worker) acquire(agent protocol.AgentID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inFlight[agent] {
		return false
	}
	w.inFlight[agent] = true
	return true
}

func (w *Worker) release(agent protocol.AgentID) {
	w.mu.Lock()
	delete(w.inFlight, agent)
	w.mu.Unlock()
}
