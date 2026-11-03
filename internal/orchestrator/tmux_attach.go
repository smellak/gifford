package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/decide"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

var errTmuxNoWrite = errors.New("tmux attach delivery handled without a write")

const tmuxRevalidateFailed daemon.AttemptReason = "TMUX_REVALIDATE_FAILED"

// TmuxInstanceRegistry is the narrow P1 surface used by delivery. Registration
// and lifecycle reconciliation remain outside the executor.
type TmuxInstanceRegistry interface {
	Lookup(context.Context, protocol.AgentID) (tmuxinstance.Instance, error)
	Verify(context.Context, tmuxinstance.Instance, tmuxinstance.PaneObservation) error
}

// TmuxDeliveryTransport is the bounded, locked physical writer.
type TmuxDeliveryTransport interface {
	Deliver(context.Context, tmuxattach.Request, tmuxattach.Guard) (tmuxattach.Result, error)
}

// TmuxSessionInvalidator closes the Core session when P1 invalidates its
// registered instance.
type TmuxSessionInvalidator interface {
	Invalidate(context.Context, protocol.AgentID, protocol.OpaqueID) error
}

// TmuxInputGuardWriter publishes the fresh detector result while the pane
// writer lock is held.
type TmuxInputGuardWriter interface {
	WriteInputGuard(context.Context, protocol.InputGuardReport) (bool, error)
}

// TmuxProcessGuard is the P3 resting-baseline guard.
type TmuxProcessGuard interface {
	Rebaseline(string, []procguard.Entry)
	Check(string, []procguard.Entry) procguard.Verdict
}

// TmuxAttachOptions wires the dependencies whose effects must all happen
// while the physical transport owns the shared pane lock.
type TmuxAttachOptions struct {
	Registry            TmuxInstanceRegistry
	Transport           TmuxDeliveryTransport
	Sessions            TmuxSessionInvalidator
	InputGuards         TmuxInputGuardWriter
	ProcessGuard        TmuxProcessGuard
	SampleTree          func(int) ([]procguard.Entry, error)
	DetectorTable       detect.Table
	HumanActivityWindow time.Duration
}

type tmuxAttachRuntime struct {
	registry      TmuxInstanceRegistry
	transport     TmuxDeliveryTransport
	sessions      TmuxSessionInvalidator
	inputGuards   TmuxInputGuardWriter
	processGuard  TmuxProcessGuard
	sampleTree    func(int) ([]procguard.Entry, error)
	detectorTable detect.Table
	humanWindow   time.Duration

	baselineMu    sync.Mutex
	baselineReady map[string]bool
	deferMu       sync.Mutex
	deferred      map[protocol.AgentID]tmuxDeferred
}

func newTmuxAttachRuntime(opts TmuxAttachOptions) (*tmuxAttachRuntime, error) {
	if opts.Registry == nil || opts.Transport == nil || opts.Sessions == nil ||
		opts.InputGuards == nil || opts.ProcessGuard == nil ||
		opts.HumanActivityWindow <= 0 {
		return nil, ErrInvalidOptions
	}
	if opts.SampleTree == nil {
		opts.SampleTree = defaultSampleTree
	}
	if opts.DetectorTable == nil {
		opts.DetectorTable = detect.DefaultTable()
	}
	return &tmuxAttachRuntime{
		registry: opts.Registry, transport: opts.Transport, sessions: opts.Sessions,
		inputGuards: opts.InputGuards, processGuard: opts.ProcessGuard,
		sampleTree: opts.SampleTree, detectorTable: opts.DetectorTable,
		humanWindow: opts.HumanActivityWindow, baselineReady: make(map[string]bool),
	}, nil
}

func (r *tmuxAttachRuntime) baselineIsReady(key string) bool {
	r.baselineMu.Lock()
	defer r.baselineMu.Unlock()
	return r.baselineReady[key]
}

func (r *tmuxAttachRuntime) markBaselineReady(key string) {
	r.baselineMu.Lock()
	r.baselineReady[key] = true
	r.baselineMu.Unlock()
}

// EvaluateTmuxAttach implements daemon.TmuxAttachDeliveryProcessor. It reads
// durable provenance before acquiring the pane lock, then reloads all mutable
// authority inside the locked guard before the first physical effect.
func (e *Executor) EvaluateTmuxAttach(ctx context.Context, candidate daemon.QueueDelivery) error {
	if e.tmux == nil || ctx == nil {
		return ErrInvalidOptions
	}
	instance, err := e.tmux.registry.Lookup(ctx, candidate.AgentID)
	if err != nil {
		return fmt.Errorf("lookup tmux delivery instance: %w", err)
	}
	if instance.AgentID != candidate.AgentID || !instance.SessionID.Valid() {
		return fmt.Errorf("tmux delivery instance has no bound session: %w", ErrInvalidOptions)
	}

	// Message provenance is immutable, but loading it here selects the exact
	// allowlisted line before Transport validates its Request. Revalidate
	// reloads the same record under the pane lock before granting authority.
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin tmux delivery selection: %w", err)
	}
	record, err := e.loadDelivery(ctx, tx, candidate, instance.SessionID)
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		return fmt.Errorf("load tmux delivery selection: %w", err)
	}
	if record.captureMode != protocol.CaptureModeTmuxAttach ||
		record.sessionState != protocol.SessionStateConnected || !record.deliverable ||
		record.process != instance.Process() {
		return fmt.Errorf("tmux delivery session is unavailable: %w", ErrInvalidOptions)
	}
	notice := e.noticeFor(record)
	target := tmuxattach.Target{
		Server: tmuxattach.Server{
			OwnerUser: instance.OwnerUser, OwnerUID: instance.OwnerUID, Socket: instance.Socket,
			Bank: instance.Bank,
		},
		Pane: instance.PaneID,
	}
	guard := &tmuxDeliveryGuard{
		executor: e, runtime: e.tmux, candidate: candidate, instance: instance,
		target: target, notice: notice,
	}
	_, err = e.tmux.transport.Deliver(ctx, tmuxattach.Request{Target: target, Notice: notice}, guard)
	if errors.Is(err, errTmuxNoWrite) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deliver tmux notice: %w", err)
	}
	return nil
}

type tmuxDeliveryGuard struct {
	executor  *Executor
	runtime   *tmuxAttachRuntime
	candidate daemon.QueueDelivery
	instance  tmuxinstance.Instance
	target    tmuxattach.Target
	notice    protocol.NoticeLine

	observation tmuxattach.PaneObservation
	report      protocol.InputGuardReport
	plan        *executionPlan
	finished    bool
	deferState  tmuxDeferState
}

// tmuxDetectorHarness maps canonical protocol harnesses to the detector's
// closed profile names. Harnesses without a measured tmux detector profile
// are rejected instead of being cast into a possibly matching map key.
func tmuxDetectorHarness(harness protocol.Harness) (detect.Harness, bool) {
	switch harness {
	case protocol.HarnessCodex:
		return detect.HarnessCodex, true
	case protocol.HarnessClaudeCode:
		return detect.HarnessClaude, true
	case protocol.HarnessMuseCode:
		return detect.HarnessMuse, true
	case protocol.HarnessOpencode:
		return detect.HarnessOpenCode, true
	default:
		return "", false
	}
}

func (g *tmuxDeliveryGuard) Revalidate(
	ctx context.Context,
	target tmuxattach.Target,
	observation tmuxattach.PaneObservation,
	clients []byte,
	screen []byte,
) error {
	if target != g.target || observation.PaneID != target.Pane {
		return fmt.Errorf("tmux target changed: %w", errTmuxNoWrite)
	}
	registryObservation := tmuxinstance.PaneObservation{
		PaneID: observation.PaneID, PanePID: observation.PanePID,
		Width: observation.Cols, Height: observation.Rows,
	}
	if err := g.runtime.registry.Verify(ctx, g.instance, registryObservation); err != nil {
		if errors.Is(err, tmuxinstance.ErrInvalidated) {
			closeErr := g.runtime.sessions.Invalidate(ctx, g.instance.AgentID, g.instance.SessionID)
			return errors.Join(fmt.Errorf("registered tmux instance invalid: %w", errTmuxNoWrite), closeErr)
		}
		return fmt.Errorf("verify registered tmux instance: %w", err)
	}

	entries, err := g.runtime.sampleTree(int(g.instance.PanePID))
	if err != nil {
		return fmt.Errorf("sample tmux process tree: %w", err)
	}
	processes := detectorProcesses(entries)
	state := detect.State{Detail: "unsupported-harness"}
	if harness, ok := tmuxDetectorHarness(g.instance.Harness); ok {
		state = detect.Classify(
			harness, g.runtime.detectorTable, screen,
			observation.Cols, observation.Rows, int(g.instance.PanePID), processes,
		)
	}
	state.IsCLI = state.IsCLI && registeredInForeground(g.instance, entries)
	humanActive, humanCertain := detect.HumanActivity(
		string(clients), string(target.Pane), g.executor.clock.Now(), g.runtime.humanWindow,
	)

	panelKey := string(g.instance.ID)
	workEntries := entries
	workCensusReady := true
	if g.instance.Harness == protocol.HarnessCodex {
		var harnessStart uint64
		if g.instance.HarnessStart > 0 {
			harnessStart = uint64(g.instance.HarnessStart)
		}
		workEntries, workCensusReady = procguard.CodexWorkCensus(entries,
			int(g.instance.HarnessPID), harnessStart,
			g.instance.HarnessArgv0, g.instance.HarnessArgv1)
	}
	baselineReady := g.runtime.baselineIsReady(panelKey)
	processVerdict := g.runtime.processGuard.Check(panelKey, workEntries)
	if !workCensusReady {
		processVerdict = procguard.Busy
	}
	warmedNow := false
	confirmedRest := state.Certain && !state.Busy && !state.BoxBusy && state.IsCLI &&
		humanCertain && !humanActive && workCensusReady
	if !baselineReady && confirmedRest {
		g.runtime.processGuard.Rebaseline(panelKey, workEntries)
		g.runtime.markBaselineReady(panelKey)
		warmedNow = true
	}
	if skip, err := g.skipUnchangedDefer(ctx, state, humanActive, humanCertain,
		processVerdict, baselineReady && !warmedNow); err != nil {
		return err
	} else if skip {
		g.finished = true
		return errTmuxNoWrite
	}

	report, err := g.executor.nextTmuxInputGuard(ctx, g.instance, state, humanActive, humanCertain)
	if err != nil {
		return err
	}
	written, err := g.runtime.inputGuards.WriteInputGuard(ctx, report)
	if err != nil {
		return fmt.Errorf("write tmux input guard: %w", err)
	}
	if !written {
		return fmt.Errorf("write tmux input guard lost monotonic race: %w", errTmuxNoWrite)
	}
	g.observation = observation
	g.report = report
	return g.authorize(ctx, state, humanActive, humanCertain, processVerdict,
		baselineReady, warmedNow)
}

func detectorProcesses(entries []procguard.Entry) []detect.Process {
	processes := make([]detect.Process, 0, len(entries))
	for _, entry := range entries {
		var state byte
		if len(entry.State) == 1 {
			state = entry.State[0]
		}
		var argv []string
		if entry.Program != "" {
			argv = []string{entry.Program}
		}
		processes = append(processes, detect.Process{
			PID: entry.PID, PPID: entry.PPID, Pgrp: entry.Pgrp, State: state,
			TPGID: entry.TPGID, StartTime: entry.StartTime, Argv: argv,
		})
	}
	return processes
}

func (e *Executor) nextTmuxInputGuard(
	ctx context.Context,
	instance tmuxinstance.Instance,
	state detect.State,
	humanActive bool,
	humanCertain bool,
) (protocol.InputGuardReport, error) {
	draftState := protocol.DraftStateUnknown
	if state.Certain {
		if state.BoxBusy {
			draftState = protocol.DraftStateOpen
		} else {
			draftState = protocol.DraftStateClosed
		}
	}
	activityState := protocol.ActivityStateUnknown
	if humanCertain {
		activityState = protocol.ActivityStateIdle
		if humanActive {
			activityState = protocol.ActivityStateActive
		}
	}

	var generation, version int64
	var previous string
	err := e.db.QueryRowContext(ctx, `SELECT draft_generation, draft_state, version
		FROM input_guards WHERE session_id = ? AND agent_id = ?`,
		instance.SessionID, instance.AgentID).Scan(&generation, &previous, &version)
	if errors.Is(err, sql.ErrNoRows) {
		generation, version, previous = 0, 0, ""
	} else if err != nil {
		return protocol.InputGuardReport{}, fmt.Errorf("read tmux input guard version: %w", err)
	}
	if previous != "" && previous != string(draftState) {
		generation++
	}
	confidence := 0.0
	if state.Certain && humanCertain {
		confidence = 1
	}
	report := protocol.InputGuardReport{
		AgentID: instance.AgentID, SessionID: instance.SessionID,
		DraftState: draftState, ActivityState: activityState,
		DraftGeneration: generation, InputGuardVersion: version + 1,
		DetectionSource: protocol.InputGuardProviderTmuxAttach, Confidence: confidence,
	}
	if err := report.Validate(); err != nil {
		return protocol.InputGuardReport{}, fmt.Errorf("build tmux input guard: %w", err)
	}
	return report, nil
}

func (g *tmuxDeliveryGuard) authorize(
	ctx context.Context,
	state detect.State,
	humanActive bool,
	humanCertain bool,
	processVerdict procguard.Verdict,
	baselineReady bool,
	warmedNow bool,
) error {
	tx, err := g.executor.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux delivery authorization: %w", err)
	}
	defer rollback(tx)
	record, err := g.executor.loadDelivery(ctx, tx, g.candidate, g.instance.SessionID)
	if err != nil {
		return err
	}
	if record.sessionID != g.instance.SessionID ||
		record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable ||
		record.process != g.instance.Process() ||
		g.executor.noticeFor(record) != g.notice {
		return fmt.Errorf("tmux delivery identity changed: %w", errTmuxNoWrite)
	}
	if !record.expiresAt.After(g.executor.clock.Now()) {
		if err := expireQueue(ctx, tx, record, g.executor.clock.Now()); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit expired tmux delivery: %w", err)
		}
		g.finished = true
		return errTmuxNoWrite
	}
	committed, err := g.executor.attempts.HasCommittedEvidenceForIdentity(
		ctx, tx, record.MessageID, record.AgentID, record.DeliveryGeneration,
	)
	if err != nil {
		return fmt.Errorf("check committed tmux evidence: %w", err)
	}
	uncertain, err := g.executor.attempts.HasUncertain(ctx, tx, record.AgentID)
	if err != nil {
		return fmt.Errorf("check uncertain tmux evidence: %w", err)
	}
	if committed || uncertain {
		return errTmuxNoWrite
	}

	message := record.channelMessage()
	channelDecision, err := g.executor.channel.AuthorizePTYDelivery(
		ctx, tx, message, record.DeliveryGeneration, "",
	)
	if err != nil {
		return fmt.Errorf("authorize tmux delivery controls: %w", err)
	}
	control := tmuxControl(channelDecision)
	detectorCertain := state.Certain && humanCertain
	processFree := processVerdict == procguard.Free
	if !baselineReady {
		// A missing baseline is warmed only at confirmed rest. The warming
		// pass itself holds; a later pass may be Free. Busy pre-baseline
		// screens also hold instead of fabricating an uncertain effect.
		processFree = false
	} else if processVerdict == procguard.Uncertain {
		detectorCertain = false
	}
	if warmedNow {
		processFree = false
	}
	decision := decide.Decide(control, decide.Guard{
		DetectorCertain: detectorCertain,
		Busy:            state.Busy,
		BoxBusy:         state.BoxBusy,
		HumanActive:     humanActive,
		RegistryValid:   true,
		IsCLI:           state.IsCLI,
	}, processFree)
	guards := tmuxHardGuards(record, state, humanActive, humanCertain,
		processVerdict, baselineReady && !warmedNow, g.executor.clock.Now())

	selected := protocol.OrchestratorActionDefer
	status := protocol.ExecutionStatusExecuted
	switch decision.Outcome {
	case decide.Notify:
		selected, status = protocol.OrchestratorActionInjectPTYPrompt, protocol.ExecutionStatusPending
	case decide.Drop:
		status = protocol.ExecutionStatusAborted
	case decide.Hold, decide.Uncertain:
		// Defer is already the fail-closed default for every no-write outcome.
	}
	persisted, err := g.executor.persistEvaluation(
		ctx, tx, record, channelDecision, guards, selected, status,
	)
	if err != nil {
		return err
	}

	switch decision.Outcome {
	case decide.Hold, decide.Uncertain:
		// Before the first physical send there cannot be an uncertain
		// effect. Persist the decision and retry on a later worker pass; an
		// UNCERTAIN attempt and reconciliation approval are reserved for the
		// no-retry edge after the first send starts.
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit held tmux delivery: %w", err)
		}
		g.runtime.rememberDefer(g.candidate.AgentID, g.deferState, g.executor.clock.Now())
		g.finished = true
		return errTmuxNoWrite
	case decide.Drop:
		if err := transitionQueueCAS(ctx, tx, record,
			queueStateCancelled, g.executor.clock.Now()); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit dropped tmux delivery: %w", err)
		}
		g.runtime.forgetDefer(g.candidate.AgentID)
		g.finished = true
		return errTmuxNoWrite
	case decide.Notify:
		if err := g.executor.claimDelivery(ctx, tx, record); err != nil {
			return err
		}
		attempt, err := g.createAttempt(ctx, tx, record, channelDecision, persisted)
		if err != nil {
			return err
		}
		if err := g.executor.attempts.MarkPrepared(
			ctx, tx, attempt.DeliveryID, attempt.WriterLeaseID,
		); err != nil {
			return fmt.Errorf("mark tmux attempt prepared: %w", err)
		}
		attempt.State = daemon.AttemptPrepared
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit prepared tmux attempt: %w", err)
		}
		g.runtime.forgetDefer(g.candidate.AgentID)
		g.plan = &executionPlan{
			record: record, decision: channelDecision, attempt: attempt,
			message: message, report: g.report,
		}
		return g.commitSent(ctx)
	default:
		return fmt.Errorf("unsupported tmux decision: %w", ErrInvalidOptions)
	}
}

func tmuxControl(decision channel.Decision) decide.Control {
	switch decision.Outcome() {
	case channel.Allow:
		return decide.ControlAllow
	case channel.Hold, channel.Approval:
		return decide.ControlHold
	case channel.Deny:
		return decide.ControlDeny
	default:
		// An invalid value is intentional: P4 maps every unknown control to
		// its explicit fail-closed Hold row.
		return decide.Control(-1)
	}
}

func tmuxHardGuards(
	record deliveryRecord,
	state detect.State,
	humanActive bool,
	humanCertain bool,
	processVerdict procguard.Verdict,
	baselineReady bool,
	now time.Time,
) []protocol.GuardResult {
	processFree := baselineReady && processVerdict == procguard.Free
	processDetail := "process guard is busy"
	if !baselineReady {
		processDetail = "resting process baseline is not ready"
	} else if processVerdict == procguard.Uncertain {
		processDetail = "process guard is uncertain"
	}
	return []protocol.GuardResult{
		guard(protocol.GuardNameSessionConnected,
			record.sessionState == protocol.SessionStateConnected,
			"tmux session is not CONNECTED"),
		guard(protocol.GuardNameDeliverable,
			record.deliverable && record.captureMode == protocol.CaptureModeTmuxAttach,
			"session is not a deliverable tmux attachment"),
		// These stable protocol names describe the attached-mode checks,
		// not a missing PTY link or a native plugin. They remain mandatory:
		// the physical writer must have a foreground CLI and a known idle
		// screen, and the resting process baseline must have no new work.
		guard(protocol.GuardNameAdapterConnected, state.IsCLI,
			"tmux registered harness CLI is not verified in the foreground"),
		guard(protocol.GuardNamePTYOutputQuiet, state.Certain && !state.Busy,
			"tmux screen detector is busy or uncertain"),
		guard(protocol.GuardNameHumanDraftClosed, state.Certain && !state.BoxBusy,
			"input box is occupied or uncertain"),
		guard(protocol.GuardNameHumanActivityNotActive, humanCertain && !humanActive,
			"client activity is recent or uncertain"),
		guard(protocol.GuardNameNoNewChildOrTool, processFree, processDetail),
		guard(protocol.GuardNameNotExpired, record.expiresAt.After(now),
			"delivery expired"),
		guard(protocol.GuardNameNotDuplicate, true, ""),
	}
}

func (g *tmuxDeliveryGuard) createAttempt(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	decision channel.Decision,
	persisted persistedEvaluation,
) (daemon.Attempt, error) {
	deliveryID, err := g.executor.mintOpaqueID("dlv", g.executor.clock.Now())
	if err != nil {
		return daemon.Attempt{}, err
	}
	leaseID, err := g.executor.mintOpaqueID("lease", g.executor.clock.Now())
	if err != nil {
		return daemon.Attempt{}, err
	}
	attempt, err := g.executor.attempts.Create(ctx, tx, daemon.AttemptSpec{
		DeliveryID: deliveryID, MessageID: record.MessageID, AgentID: record.AgentID,
		DeliveryGeneration: record.DeliveryGeneration, AdapterSessionID: record.sessionID,
		OrchestratorDecisionID: persisted.decisionID, ChannelDecisionID: decision.ID(),
		DraftGeneration:   g.report.DraftGeneration,
		InputGuardVersion: g.report.InputGuardVersion,
		ControlVersions:   decision.Versions(),
	})
	if err != nil {
		return daemon.Attempt{}, fmt.Errorf("create tmux delivery attempt: %w", err)
	}
	attempt.WriterLeaseID = leaseID
	return attempt, nil
}

func (g *tmuxDeliveryGuard) commitSent(ctx context.Context) error {
	if g.plan == nil || g.plan.attempt.State != daemon.AttemptPrepared {
		return fmt.Errorf("tmux delivery has no prepared attempt: %w", ErrInvalidOptions)
	}
	tx, err := g.executor.db.BeginTx(ctx, nil)
	if err != nil {
		return g.cancelPrepared(ctx, err)
	}
	defer rollback(tx)
	err = g.executor.channel.Revalidate(
		ctx, tx, g.plan.decision, g.plan.message, g.plan.record.DeliveryGeneration,
	)
	if err != nil {
		return g.handleCommitRevalidationFailure(ctx, tx, err)
	}
	if err := g.executor.attempts.MarkCommitSent(
		ctx, tx, g.plan.attempt.DeliveryID,
	); err != nil {
		_ = tx.Rollback()
		return g.cancelPrepared(ctx, err)
	}
	if err := transitionAttemptQueue(ctx, tx, g.plan.attempt.DeliveryID,
		g.plan.record, queueStateClaimed, queueStateDelivering,
		g.executor.clock.Now(), true); err != nil {
		_ = tx.Rollback()
		return g.cancelPrepared(ctx, err)
	}
	if err := tx.Commit(); err != nil {
		return g.cancelPrepared(ctx, err)
	}
	g.plan.attempt.State = daemon.AttemptCommitSent
	return nil
}

func (g *tmuxDeliveryGuard) handleCommitRevalidationFailure(
	ctx context.Context,
	tx *sql.Tx,
	cause error,
) error {
	var changed *channel.RevalidationError
	if !errors.As(cause, &changed) {
		_ = tx.Rollback()
		return g.cancelPrepared(ctx, cause)
	}
	if err := g.executor.attempts.Transition(ctx, tx,
		g.plan.attempt.DeliveryID, daemon.Transition{
			From: daemon.AttemptPrepared, To: daemon.AttemptCancelled,
			Reason: daemon.ReasonControlChanged,
		}); err != nil {
		return fmt.Errorf("cancel tmux attempt after control change: %w", err)
	}
	if err := transitionAttemptQueue(ctx, tx, g.plan.attempt.DeliveryID,
		g.plan.record, queueStateClaimed, queueStateCancelled,
		g.executor.clock.Now(), false); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, g.plan.attempt.OrchestratorDecisionID,
		protocol.ExecutionStatusAborted); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit changed tmux controls: %w", err)
	}
	g.plan.attempt.State = daemon.AttemptCancelled
	g.finished = true
	return errTmuxNoWrite
}

func (g *tmuxDeliveryGuard) cancelPrepared(
	ctx context.Context,
	cause error,
) error {
	if g.plan == nil || g.plan.attempt.State != daemon.AttemptPrepared {
		return cause
	}
	cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.executor.timeout)
	defer cancel()
	tx, err := g.executor.db.BeginTx(cleanCtx, nil)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("begin tmux attempt cancellation: %w", err))
	}
	defer rollback(tx)
	if err := g.executor.attempts.Transition(cleanCtx, tx,
		g.plan.attempt.DeliveryID, daemon.Transition{
			From: daemon.AttemptPrepared, To: daemon.AttemptFailed, Reason: tmuxRevalidateFailed,
		}); err != nil {
		return errors.Join(cause, err)
	}
	if err := transitionAttemptQueue(cleanCtx, tx,
		g.plan.attempt.DeliveryID, g.plan.record, queueStateClaimed,
		queueStateQueued, g.executor.clock.Now(), false); err != nil {
		return errors.Join(cause, err)
	}
	if err := updateExecution(cleanCtx, tx,
		g.plan.attempt.OrchestratorDecisionID, protocol.ExecutionStatusFailed); err != nil {
		return errors.Join(cause, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cause, fmt.Errorf("commit tmux attempt cancellation: %w", err))
	}
	g.plan.attempt.State = daemon.AttemptFailed
	g.finished = true
	return cause
}

// VerifyNotice runs between the typed notice and Enter. Besides the exact
// notice on screen, it re-checks that the registered CLI is still the same
// process and owns the pane's foreground: a shell that hosts the CLI gets
// the terminal back when the CLI exits or stops. The check runs last, right
// before Enter, but tmux cannot make it atomic with the keystroke: a CLI
// that exits or stops in the milliseconds between them still lets Enter
// reach the shell.
func (g *tmuxDeliveryGuard) VerifyNotice(
	ctx context.Context,
	target tmuxattach.Target,
	screen []byte,
	notice protocol.NoticeLine,
) error {
	if target != g.target || notice != g.notice || g.observation.Cols <= 0 || g.observation.Rows <= 0 {
		return ErrInvalidOptions
	}
	harness, ok := tmuxDetectorHarness(g.instance.Harness)
	if !ok {
		return errors.New("tmux harness has no detector profile")
	}
	certain, exact := detect.VerifyNotice(
		harness, g.runtime.detectorTable, screen,
		g.observation.Cols, g.observation.Rows, string(notice),
	)
	if !certain || !exact {
		return errors.New("tmux notice is not the exact sole input")
	}
	// An INVALID row already stops every later write, so the session is left
	// for the next Revalidate to close; this attempt ends UNCERTAIN either way.
	registryObservation := tmuxinstance.PaneObservation{
		PaneID: g.observation.PaneID, PanePID: g.observation.PanePID,
		Width: g.observation.Cols, Height: g.observation.Rows,
	}
	if err := g.runtime.registry.Verify(ctx, g.instance, registryObservation); err != nil {
		return fmt.Errorf("verify registered tmux instance before Enter: %w", err)
	}
	entries, err := g.runtime.sampleTree(int(g.instance.PanePID))
	if err != nil {
		return fmt.Errorf("sample pre-Enter process tree: %w", err)
	}
	state := detect.Classify(
		harness, g.runtime.detectorTable, screen,
		g.observation.Cols, g.observation.Rows, int(g.instance.PanePID),
		detectorProcesses(entries),
	)
	if !state.IsCLI || !registeredInForeground(g.instance, entries) {
		return errors.New("registered harness CLI is not in the foreground before Enter")
	}
	return nil
}

// registeredInForeground reports whether the registered CLI process itself,
// by PID and start time, owns the foreground process group of the pane's tty
// and is running or sleeping. Another CLI in the same tree never stands in
// for it: a registered CLI stopped or sent to the background holds the write
// even if a second one runs in front.
func registeredInForeground(instance tmuxinstance.Instance, entries []procguard.Entry) bool {
	for _, entry := range entries {
		if int64(entry.PID) != instance.HarnessPID {
			continue
		}
		running := entry.State == "R" || entry.State == "S" || entry.State == "D"
		return instance.HarnessStart >= 0 && entry.StartTime == uint64(instance.HarnessStart) &&
			entry.Pgrp > 0 && entry.Pgrp == entry.TPGID && running
	}
	return false
}

func (g *tmuxDeliveryGuard) VerifySubmitted(
	_ context.Context,
	target tmuxattach.Target,
	screen []byte,
) error {
	if target != g.target || g.observation.Cols <= 0 || g.observation.Rows <= 0 {
		return ErrInvalidOptions
	}
	entries, err := g.runtime.sampleTree(int(g.instance.PanePID))
	if err != nil {
		return fmt.Errorf("sample post-submit process tree: %w", err)
	}
	harness, ok := tmuxDetectorHarness(g.instance.Harness)
	if !ok {
		return errors.New("tmux harness has no detector profile")
	}
	state := detect.Classify(
		harness, g.runtime.detectorTable, screen,
		g.observation.Cols, g.observation.Rows, int(g.instance.PanePID),
		detectorProcesses(entries),
	)
	if state.Busy || state.Certain && !state.BoxBusy {
		return nil
	}
	return errors.New("tmux post-submit state is uncertain")
}

func (g *tmuxDeliveryGuard) Record(ctx context.Context, result tmuxattach.Result) error {
	if g.finished || g.plan == nil {
		return nil
	}
	if g.plan.attempt.State == daemon.AttemptPrepared {
		return g.cancelPrepared(ctx,
			errors.New("tmux attempt ended before commit authorization"))
	}
	if g.plan.attempt.State != daemon.AttemptCommitSent {
		return nil
	}

	tx, err := g.executor.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux delivery result: %w", err)
	}
	defer rollback(tx)
	status := protocol.ExecutionStatusFailed
	queueState := queueStateQueued
	switch result.Outcome {
	case tmuxattach.OutcomeCommitted:
		err = g.executor.attempts.Transition(ctx, tx, g.plan.attempt.DeliveryID,
			daemon.Transition{From: daemon.AttemptCommitSent, To: daemon.AttemptCommitted})
		status, queueState = protocol.ExecutionStatusExecuted, queueStateDelivered
	case tmuxattach.OutcomeUncertain, tmuxattach.OutcomeNotWritten:
		reason := daemon.AttemptReason("TMUX_" + string(result.Stage) + "_UNCERTAIN")
		err = g.executor.attempts.MarkUncertain(ctx, tx, g.plan.attempt.DeliveryID, reason)
		if err == nil {
			err = g.executor.createUncertainApproval(ctx, tx, g.plan.attempt.DeliveryID,
				g.plan.record.MessageID, g.plan.record.AgentID,
				g.plan.record.DeliveryGeneration, "tmux write outcome uncertain")
		}
	default:
		err = fmt.Errorf("unknown tmux transport outcome %q", result.Outcome)
	}
	if err != nil {
		return err
	}
	if err := transitionAttemptQueue(ctx, tx, g.plan.attempt.DeliveryID,
		g.plan.record, queueStateDelivering, queueState,
		g.executor.clock.Now(), false); err != nil {
		return err
	}
	if err := updateExecution(ctx, tx, g.plan.attempt.OrchestratorDecisionID, status); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tmux delivery result: %w", err)
	}
	if result.Outcome != tmuxattach.OutcomeCommitted {
		g.executor.wakeNotifications()
		g.plan.attempt.State = daemon.AttemptUncertain
	} else {
		g.plan.attempt.State = daemon.AttemptCommitted
	}
	g.finished = true
	return nil
}
