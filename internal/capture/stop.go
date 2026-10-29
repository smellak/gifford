package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// StopDeps is stop's injectable environment. Zero Timeout and Poll
// take the §10 bounds; tests pass short ones.
type StopDeps struct {
	PID     int
	Clock   clock.Clock
	Mono    Mono
	DataDir func() string
	Timeout time.Duration
	Poll    time.Duration
}

// Stop bounds for the mailbox lock and the ingest poll.
const (
	stopLockBound   = time.Second
	stopPollTimeout = 5 * time.Second
	stopPollEvery   = 50 * time.Millisecond
)

// errStopRefused reports a stop mailbox that cannot be recorded: two
// crash remnants already occupy the name.
var errStopRefused = errors.New("stop request could not be recorded")

// Stop records a durable stop request and waits for it to be
// honoured: it validates first, finds the agent's live session
// through the read-only open without creating anything, publishes
// the request under stop.lock holding no other lock, then polls
// ingest.lock until the stop commits or the bound expires. It
// returns a human line for the CLI.
func Stop(ctx context.Context, deps StopDeps, opts StopOptions) (string, error) {
	reason := opts.Reason
	if reason == "" {
		reason = "manual"
	}
	req := &stopRequest{
		Schema: "gifford.capture.stop.v1", Reason: reason,
		Detail: opts.Detail, RequestedAt: formatWall(deps.Clock.Now()),
	}
	if err := req.validate(); err != nil {
		return "", fmt.Errorf("%w: invalid stop: %w", ErrUsage, err)
	}
	data := opts.Data
	if data == "" {
		data = deps.DataDir()
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrNotCaptured, opts.Agent)
	}
	defer func() { _ = root.Close() }()
	agent, session, err := stopHint(ctx, data, opts.Agent)
	if err != nil {
		return "", err
	}
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		return "", err
	}
	defer func() { _ = store.Close() }()
	dir := agent + "/" + session
	if err := ensureLock(root, dir+"/stop.lock"); err != nil {
		return "", err
	}
	pending, err := publishStopRequest(root, dir, agent, session, req)
	if err != nil {
		return "", err
	}
	if pending != nil {
		return fmt.Sprintf("a stop request is already pending (reason %s)", pending.Reason), nil
	}
	return deps.pollStop(ctx, store, root, agent, session)
}

// stopHint finds the agent's live session read without locks through
// the read-only open. Anything missing or unreadable means there is
// no live capture, and nothing is created.
func stopHint(ctx context.Context, data, agent string) (string, string, error) {
	missing := func() (string, string, error) {
		return "", "", fmt.Errorf("%w: %s", ErrNotCaptured, agent)
	}
	if _, err := os.Stat(filepath.Join(data, "gifford.db")); err != nil {
		return missing()
	}
	db, err := OpenStoreReadOnly(filepath.Join(data, "gifford.db"))
	if err != nil {
		if errors.Is(err, ErrUnsafePath) {
			return "", "", err
		}
		return missing()
	}
	defer func() { _ = db.Close() }()
	rows, err := readOnlyRows(ctx, db, agent)
	if err != nil {
		return missing()
	}
	for _, row := range rows {
		terminal, _, incomplete, err := readOnlyLife(ctx, db, row.CaptureSessionID)
		if err != nil {
			return missing()
		}
		if !terminal && !incomplete {
			return row.AgentID, row.CaptureSessionID, nil
		}
	}
	return missing()
}

// publishStopRequest writes the request under stop.lock holding no
// other lock: an existing request is kept (its reason reported), two
// remnants refuse the write, and a lost rename race re-reads the
// winner. It returns the pending request when nothing was published.
func publishStopRequest(root *os.Root, dir, agent, session string, req *stopRequest) (*stopRequest, error) {
	var pending *stopRequest
	err := withLock(root, dir+"/stop.lock", true, stopLockBound, func() error {
		existing, err := readStopRequest(root, agent, session)
		if err != nil {
			return err
		}
		if existing != nil {
			pending = existing
			return nil
		}
		n, err := countRemnants(root, dir, "stop.request")
		if err != nil {
			return err
		}
		if n >= 2 {
			return errStopRefused
		}
		data, err := jsonBytes(req)
		if err != nil {
			return err
		}
		if err := publishFile(root, dir, "stop.request", []byte(data), true); err != nil {
			if !errors.Is(err, errNameExists) {
				return err
			}
			winner, err := readStopRequest(root, agent, session)
			if err != nil {
				return err
			}
			pending = winner
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pending, nil
}

// pollStop tries ingest.lock every poll until the stop commits or
// the timeout expires. A free lock writes the stop marker and
// decides under it; a held one is honoured by its holder.
func (deps StopDeps) pollStop(ctx context.Context, store *Store, root *os.Root, agent, session string) (string, error) {
	timeout, poll := stopPollTimeout, stopPollEvery
	if deps.Timeout > 0 {
		timeout = deps.Timeout
	}
	if deps.Poll > 0 {
		poll = deps.Poll
	}
	deadline := deps.Clock.Now().Add(timeout)
	dir := agent + "/" + session
	for {
		held, err := tryLock(root, dir+"/ingest.lock", true)
		if err == nil {
			return stopUnderLock(ctx, deps, store, root, agent, session, held)
		}
		if !errors.Is(err, ErrBusy) {
			return "", err
		}
		if !deps.Clock.Now().Before(deadline) {
			return "", fmt.Errorf("%w: stop requested for %s; the request is durable and is honoured by the running ingest or the next supervisor pass", ErrStopPending, session)
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("stop poll: %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// stopUnderLock honours the durable request holding the session
// lock: a terminal session only gets its rows repaired, an
// incomplete one finishes row-only, and a live one appends
// capture.stopped from stop.request with the STOPPED row.
func stopUnderLock(ctx context.Context, deps StopDeps, store *Store, root *os.Root, agent, session string, held *os.File) (string, error) {
	released := false
	release := func() {
		if !released {
			released = true
			_ = releaseLock(held)
		}
	}
	defer release()
	if err := writeLockLine(held, fmt.Sprintf("stop %d", deps.PID)); err != nil {
		return "", err
	}
	row, found, err := store.SessionRow(ctx, session)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("%w: %s", ErrNotCaptured, agent)
	}
	// One bounded streaming pass: the fold carries the lifecycle and
	// the next free sequence, so the history is never materialized.
	fold := newRepairFold(row)
	if err := streamSession(ctx, store, session, fold.add); err != nil {
		return "", err
	}
	switch fold.life() {
	case LifeTerminal:
		if err := store.repairFolded(ctx, row, fold); err != nil {
			return "", err
		}
		return fmt.Sprintf("capture %s already %s", session, row.State), nil
	case LifeIncomplete:
		if err := finishIncompleteHeld(ctx, store, row, deps.Clock); err != nil {
			return "", err
		}
		return fmt.Sprintf("capture %s incomplete, finished as ERROR", session), nil
	default:
		req, err := readStopRequest(root, agent, session)
		if err != nil {
			return "", err
		}
		if req == nil {
			return "", fmt.Errorf("%w: stop request missing", ErrUnsafePath)
		}
		if err := stopLiveSession(ctx, store, root, row, fold.maxSeq+1, req, deps); err != nil {
			return "", err
		}
		return fmt.Sprintf("stop recorded for %s", session), nil
	}
}

// finishIncompleteHeld finishes a row whose attach never committed,
// row-only: the caller holds the session lock, so no liveness probe
// runs. No event stream exists to append to.
func finishIncompleteHeld(ctx context.Context, store *Store, row SessionRow, clk clock.Clock) error {
	row.State = stateError
	row.StopReason = reasonError
	row.StoppedAt = formatWall(clk.Now())
	return store.UpdateSession(ctx, row)
}

// stopLiveSession appends capture.stopped{reason, detail} from the
// durable request after the last chunk and sets the STOPPED row,
// all under the caller's ingest.lock. nextSeq is the first free
// sequence, folded from the stream without retaining it. The ledger
// settles terminally best-effort, exactly as the ingest exit does.
func stopLiveSession(ctx context.Context, store *Store, root *os.Root, row SessionRow, nextSeq int64, req *stopRequest, deps StopDeps) error {
	var meta SessionMeta
	if err := readControlAt(root, row.AgentID+"/"+row.CaptureSessionID+"/session.json", 16384, &meta); err != nil {
		return err
	}
	node, err := readNodeID(root)
	if err != nil {
		return err
	}
	data, err := jsonBytes(protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		Reason:           protocol.CaptureStopReason(req.Reason),
		Detail:           req.Detail,
	})
	if err != nil {
		return err
	}
	ev, err := sessionEnvelope(deps.Clock, node, row, protocol.EventTypeCaptureStopped, nextSeq, deps.Mono.MonotonicNS(), data)
	if err != nil {
		return err
	}
	if err := store.AppendEvents(ctx, []protocol.EventEnvelope{ev}); err != nil {
		return err
	}
	row.State = StoppedRowState(protocol.CaptureStopReason(req.Reason))
	row.StoppedAt = formatWall(ev.WallTime)
	row.StopReason = req.Reason
	if err := store.UpdateSession(ctx, row); err != nil {
		return err
	}
	// Best-effort, as the ingest exit: a stale ledger over-counts
	// usage, which fails closed.
	_ = settle(root, row.AgentID, row.CaptureSessionID, meta.Limits.resolve(), true)
	return nil
}

// sessionEnvelope builds one envelope over a session row: the §8.4
// identity fields, the wall time from the clock and the given
// monotonic reading.
func sessionEnvelope(clk clock.Clock, node string, row SessionRow, evType protocol.EventType, seq int64, monoNS int64, payload protocol.Payload) (protocol.EventEnvelope, error) {
	id, err := newULID(clk)
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("event ulid: %w", err)
	}
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(id),
		SchemaVersion: 1,
		NodeID:        protocol.NodeID(node),
		SessionID:     protocol.OpaqueID(row.CaptureSessionID),
		AgentID:       protocol.AgentID(row.AgentID),
		Harness:       protocol.Harness(row.Harness),
		AdapterType:   protocol.AdapterTypeNone,
		SourceQuality: protocol.SourceQualityBootstrapPipePane,
		EventType:     evType,
		Stream:        protocol.StreamPTY,
		WallTime:      clk.Now(),
		MonotonicNS:   monoNS,
		Sequence:      seq,
		Payload:       payload,
		Sensitivity:   protocol.SensitivityNormal,
	}, nil
}
