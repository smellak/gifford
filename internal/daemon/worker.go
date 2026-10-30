package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

const defaultWorkerBatch = 64

// QueueDelivery identifies one durable delivery_queue generation. The
// executor reloads all mutable metadata inside its own transaction.
type QueueDelivery struct {
	MessageID          protocol.ULID
	AgentID            protocol.AgentID
	DeliveryGeneration int64
}

// DeliveryProcessor evaluates automatic work. In 5.3c every automatic
// evaluation defers and opens an approval; only a resolved manual order may
// reach the executor's prepare/commit path.
type DeliveryProcessor interface {
	EvaluateAutomatic(context.Context, QueueDelivery) error
}

// ApprovalProcessor applies a manual orchestrator approval.
type ApprovalProcessor interface {
	ResolveApproval(context.Context, protocol.ResolveApprovalParams, channel.Actor) (protocol.ApprovalRequest, error)
}

// PreparedController reacts immediately to restrictive control changes.
type PreparedController interface {
	ControlChanged(context.Context, protocol.AgentID) error
	PauseAutomation(context.Context, protocol.AgentID) error
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
}

// Worker scans queued PTY recipients and serializes work per agent.
type Worker struct {
	db         *sql.DB
	processor  DeliveryProcessor
	approvals  ApprovalProcessor
	prepared   PreparedController
	proxies    ProxySnapshots
	batchLimit int

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
	return &Worker{
		db: opts.DB, processor: opts.Processor, approvals: opts.Approvals,
		prepared: opts.Prepared, proxies: opts.Proxies, batchLimit: opts.BatchLimit,
		inFlight: make(map[protocol.AgentID]bool),
	}, nil
}

// RunOnce evaluates at most one queued row per live agent. Rows with an
// already-pending orchestrator approval stay quiet until the human resolves
// it. Independent agent errors are joined after the bounded scan.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	rows, err := w.db.QueryContext(ctx, `WITH eligible AS (
		SELECT dq.message_id, dq.agent_id, dq.delivery_generation, dq.priority, dq.created_at
		FROM delivery_queue AS dq WHERE dq.state = 'QUEUED'
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
			ORDER BY priority, created_at, message_id, delivery_generation) AS agent_rank
		FROM eligible
	)
		SELECT message_id, agent_id, delivery_generation FROM ranked WHERE agent_rank = 1
		ORDER BY priority, created_at, message_id, delivery_generation
		LIMIT ?`, w.batchLimit)
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
	processed := 0
	var joined error
	for _, candidate := range candidates {
		if !w.acquire(candidate.AgentID) {
			continue
		}
		err := w.processor.EvaluateAutomatic(ctx, candidate)
		w.release(candidate.AgentID)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("evaluate %s for %s: %w",
				candidate.MessageID, candidate.AgentID, err))
			continue
		}
		processed++
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
