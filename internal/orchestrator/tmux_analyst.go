package orchestrator

// This file holds the analyst-preferred route for tmux-attached deliveries.
// A usable Glimmer judgment remains authoritative. When Glimmer is absent,
// fails, opens its circuit, or supplies no valid analysis inside the bounded
// window, the same pass returns to the deterministic detector route in
// tmux_attach.go; that route still owns controls and every physical guard.
//
// The route is read-only until its own transaction commits an injection:
// Demand asks for analysis, one locked capture feeds the submission, the
// analyzed state gates the model choice, and the delivery-opening capture is
// hash-compared against the analyzed screen before the first keystroke.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// tmuxAnalystRouted reports whether tmux deliveries take the analyst route.
// It needs only the state producer and its expected profile. Model
// availability is decided inside the route so a configured-but-unavailable
// Glimmer can yield to the detector with an explicit audit reason.
func (e *Executor) tmuxAnalystRouted() bool {
	if e.tmux == nil || e.states == nil {
		return false
	}
	return e.expectedStateProfile() != nil
}

// evaluateTmuxAnalyst returns a closed fallback reason only when Glimmer gave
// no usable judgment. An empty reason means this route handled or deferred the
// delivery; the caller must not reinterpret a valid non-IDLE or CHOOSE DEFER.
func (e *Executor) evaluateTmuxAnalyst(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	notice protocol.NoticeLine,
) (tmuxDetectorFallbackReason, error) {
	if ctx == nil || !candidate.AgentID.Valid() || !instance.SessionID.Valid() {
		return "", ErrInvalidOptions
	}
	now := e.clock.Now().UTC()
	locked, err := e.tmux.transport.CaptureLocked(ctx, target)
	if err != nil {
		e.forgetAutomaticStateWait(candidate)
		return "", errTmuxAnalystWait
	}
	if e.model == nil {
		return tmuxFallbackModelUnavailable, nil
	}
	if locked.Observation.Cols < 1 || len(locked.Screen) == 0 {
		e.forgetAutomaticStateWait(candidate)
		return "", errTmuxAnalystWait
	}
	entries, err := e.tmux.sampleTree(int(locked.Observation.PanePID))
	if err != nil {
		e.forgetAutomaticStateWait(candidate)
		return "", errTmuxAnalystWait
	}
	if !e.tmuxAnalysisDemanded(candidate.AgentID) {
		return tmuxFallbackAnalysisUnavailable, nil
	}
	record, screenHash, detail, report, detectorState, active, err := e.tmuxAnalystGate(ctx, candidate,
		instance, target, locked, entries, notice)
	if err != nil {
		return "", err
	}
	if record == nil {
		return "", nil
	}
	if screenHash == "" {
		e.forgetAutomaticStateWait(candidate)
		return "", e.persistAnalystDefer(ctx, candidate, *record,
			protocol.ULID(""), detail, e.clock.Now().UTC())
	}
	observationID, citedGuard, citedDraft, detail, err := e.tmuxAnalystSubmit(ctx, candidate,
		record.sessionID, instance, locked, screenHash, report, detectorState, active, now)
	if err != nil {
		return "", err
	}
	if observationID == "" {
		e.forgetAutomaticStateWait(candidate)
		return "", e.persistAnalystDefer(ctx, candidate, *record,
			protocol.ULID(""), detail, e.clock.Now().UTC())
	}
	// Only a submittable, free-screen observation opens the bounded wait.
	// Later observations while the screen stays free share that first mark;
	// otherwise a moving screen could postpone detector fallback forever.
	withinWindow := e.withinAutomaticStateWindow(candidate, now)
	// The attempt cites the versions the analysis validated against, which
	// on a memo hit are the published ones, not the next report's.
	report.InputGuardVersion = citedGuard
	report.DraftGeneration = citedDraft
	analyzed, found, err := e.readAnalyzedState(ctx, candidate.AgentID,
		record.sessionID, observationID)
	if err != nil {
		return "", err
	}
	readNow := e.clock.Now().UTC()
	if !found || !readNow.Before(analyzed.ExpiresAt) {
		if !withinWindow {
			return tmuxFallbackAnalysisTimeout, nil
		}
		// The submission this pass read is dead: the analysis never
		// landed, or it expired before the pass could choose on it.
		// Resubmit under the published versions unless the producer
		// still holds the report, in which case the analysis is
		// genuinely in flight and the pass only waits. A live analysis
		// is never superseded; a discarded or expired one always
		// recovers, even when the screen did not move.
		key := tmuxAnalystMemoKey(candidate.AgentID, record.sessionID,
			candidate.MessageID, candidate.DeliveryGeneration)
		e.tmux.analystGenMu.Lock()
		entry, entrySeen := e.tmux.analystLast[key]
		e.tmux.analystGenMu.Unlock()
		if !entrySeen || entry.observationID != observationID {
			return "", nil
		}
		if !found && e.states.HasQueuedObservation(
			candidate.AgentID, record.sessionID, observationID,
		) {
			return "", nil
		}
		if _, _, _, _, err := e.tmuxAnalystResubmit(ctx, key, entry,
			candidate.AgentID, record.sessionID, instance, locked,
			screenHash, detectorState, readNow); err != nil {
			return "", err
		}
		return "", nil
	}
	if !tmuxAnalysisUsable(analyzed) {
		return e.tmuxAnalysisFallbackReason(candidate.AgentID, record.sessionID, observationID), nil
	}
	e.forgetAutomaticStateWait(candidate)
	if analyzed.Snapshot.HumanDraftState != protocol.DraftStateClosed ||
		!analyzed.AutomaticEligible {
		// The DEFER links a real channel decision: decision.get reads the
		// row through it, so an unauthorized DEFER would be unreadable.
		decided, err := e.authorizeTmuxAnalystDelivery(ctx, candidate, *record)
		if err != nil {
			if errors.Is(err, errTmuxAnalystGone) {
				return "", nil
			}
			return "", err
		}
		return "", e.persistTmuxAnalystDecision(ctx, candidate, *record, analyzed,
			modelChoice{}, false, &pendingChoice{
				candidate: candidate, decided: decided,
				stateID: analyzed.Snapshot.StateID,
				allowed: []protocol.OrchestratorAction{protocol.OrchestratorActionDefer},
			})
	}
	return e.finishTmuxAnalystChoice(ctx, candidate, instance, target,
		notice, *record, analyzed, screenHash, report, now)
}

// tmuxAnalysisDemanded collapses a producer control-path failure into model
// unavailability. The fallback reason, not an arbitrary producer error, is the
// only value allowed to reach the durable decision.
func (e *Executor) tmuxAnalysisDemanded(agent protocol.AgentID) bool {
	return e.states.Demand(agent) == nil
}

// tmuxAnalysisUsable accepts a current, durable PTY judgment even when its
// phase is UNKNOWN and therefore vetoes delivery. Only missing or invalid
// model provenance may yield to the detector.
func tmuxAnalysisUsable(state StateRead) bool {
	if state.AnalysisRoute != string(analyst.RoutePTY) || !state.modelFieldsValid ||
		!state.GlimmerCallID.Valid() {
		return false
	}
	switch state.EligibilityReason {
	case StateEligibilityStale, StateEligibilityExpired, StateEligibilityNoModelEvidence,
		StateEligibilityProfileChanged, StateEligibilityCallInvalid:
		return false
	default:
		return true
	}
}

type tmuxAnalysisFailureReader interface {
	tmuxAnalysisFailure(
		agent protocol.AgentID,
		session protocol.OpaqueID,
		observation protocol.OpaqueID,
	) (tmuxDetectorFallbackReason, bool)
}

func (e *Executor) tmuxAnalysisFallbackReason(
	agent protocol.AgentID,
	session protocol.OpaqueID,
	observation protocol.OpaqueID,
) tmuxDetectorFallbackReason {
	if reader, ok := e.states.(tmuxAnalysisFailureReader); ok {
		if reason, found := reader.tmuxAnalysisFailure(agent, session, observation); found && reason.valid() {
			return reason
		}
	}
	return tmuxFallbackAnalysisUnavailable
}

// tmuxAnalystSubmit submits the locked screen unless the same screen is
// already waiting for analysis under the same guard versions. A hit reuses
// the submitted observation with its published guard and draft versions,
// never the next unpublished report: the attempt must cite what the
// analysis validated against. A hit on a dead submission resubmits instead:
// when the producer holds no report for the agent anymore, or the
// observation expired, the same screen goes out again under the published
// versions, because neither Demand nor a retry recreates the discarded
// report. The guard is published before submitting so the analysis
// validates against it. It returns the observation identity with the guard
// and draft versions the attempt must cite, or an empty identity with the
// defer detail when the pass must defer without submitting: a lost guard
// race or recent client activity. The per-delivery last submission pins the
// observation identity and generation: an unchanged pane never resubmits
// while its submission is live, so in-flight analysis is never superseded
// by its own retry, while a moved pane always supersedes the stale
// submission with a newer generation.
func (e *Executor) tmuxAnalystSubmit(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	session protocol.OpaqueID,
	instance tmuxinstance.Instance,
	locked tmuxattach.LockedCapture,
	screenHash string,
	report protocol.InputGuardReport,
	detectorState detect.State,
	active bool,
	now time.Time,
) (observationID protocol.OpaqueID, guardVersion, draftGeneration int64, detail string, err error) {
	agent := candidate.AgentID
	key := tmuxAnalystMemoKey(agent, session, candidate.MessageID,
		candidate.DeliveryGeneration)
	e.tmux.analystGenMu.Lock()
	last, seen := e.tmux.analystLast[key]
	e.tmux.analystGenMu.Unlock()
	// The built report always carries one version past the durable row, so
	// a hit is the same screen with exactly our own last write in between:
	// nothing else published since the submission. Any other version step
	// means another writer moved the guard and the analysis must revalidate.
	// A hit always reuses: whether the submission is still alive is decided
	// after the read, never here, so a live analysis is never superseded by
	// its own retry.
	if seen && last.screenHash == screenHash &&
		report.InputGuardVersion == last.guardVersion+1 && last.observationID.Valid() {
		return last.observationID, last.guardVersion, last.draftGeneration, "", nil
	}
	written, err := e.tmux.inputGuards.WriteInputGuard(ctx, report)
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("write tmux analyst input guard: %w", err)
	}
	if !written {
		return "", 0, 0, "input guard lost its write race", nil
	}
	if active {
		return "", 0, 0, "client activity is recent or uncertain", nil
	}
	var previous int64
	if seen {
		previous = last.generation
	}
	return e.tmuxAnalystSubmitFresh(ctx, key, agent, session, instance, locked,
		screenHash, report.InputGuardVersion, report.DraftGeneration, detectorState,
		previous, now)
}

// tmuxAnalystSubmitFresh mints one observation under the given published
// guard versions, submits it for analysis and records the memo entry.
func (e *Executor) tmuxAnalystSubmitFresh(
	ctx context.Context,
	key string,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	instance tmuxinstance.Instance,
	locked tmuxattach.LockedCapture,
	screenHash string,
	guardVersion, draftGeneration int64,
	detectorState detect.State,
	previous int64,
	now time.Time,
) (observationID protocol.OpaqueID, citedGuard, citedDraft int64, detail string, err error) {
	generation, err := e.tmuxAnalystGeneration(ctx, agent, session, previous)
	if err != nil {
		return "", 0, 0, "", err
	}
	observationID, err = e.mintOpaqueID("obs", now)
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("mint tmux observation identity: %w", err)
	}
	observed := e.tmuxAnalystReport(ctx, agent, session, instance, locked,
		observationID, generation, guardVersion, now)
	if err := observed.Validate(); err != nil {
		return "", 0, 0, "", fmt.Errorf("validate tmux analysis submission: %w", err)
	}
	processJSON, err := json.Marshal(instance.Process())
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("encode tmux analysis process identity: %w", err)
	}
	submitter, ok := e.states.(tmuxCorroboratedStateSubmitter)
	if !ok {
		return "", 0, 0, "", fmt.Errorf("submit tmux analysis: %w", ErrStateProducerOptions)
	}
	corroboration := tmuxDetectorCorroboration{
		agentID: agent, sessionID: session, instanceID: instance.ID,
		paneID: locked.Observation.PaneID, panePID: locked.Observation.PanePID,
		observationID: observationID, observedAt: observed.ObservedAt,
		harness: observed.Harness, harnessVersion: observed.HarnessVersion,
		observationGeneration: observed.ObservationGeneration,
		outputGeneration:      observed.OutputGeneration, eventGeneration: observed.EventGeneration,
		inputGuardVersion: guardVersion, draftGeneration: draftGeneration,
		projectedScreenSHA256: protocol.SHA256Hex(screenHash),
		processSHA256:         sha256.Sum256(processJSON), policyVersion: stateAnalysisPolicy,
		certain: detectorState.Certain, busy: detectorState.Busy,
		boxBusy: detectorState.BoxBusy, isCLI: detectorState.IsCLI, detail: detectorState.Detail,
	}
	if err := submitter.submitTmuxCorroborated(observed, corroboration); err != nil {
		return "", 0, 0, "", fmt.Errorf("submit tmux analysis: %w", err)
	}
	e.tmux.analystGenMu.Lock()
	// The memo keys one delivery, so losing it only repeats that
	// delivery's submission. Bound the map instead of letting resolved
	// deliveries accumulate without end.
	if len(e.tmux.analystLast) >= tmuxAnalystMemoLimit {
		clear(e.tmux.analystLast)
	}
	e.tmux.analystLast[key] = analystSubmission{
		observationID: observationID, generation: generation, screenHash: screenHash,
		guardVersion: guardVersion, draftGeneration: draftGeneration,
	}
	e.tmux.analystGenMu.Unlock()
	return observationID, guardVersion, draftGeneration, "", nil
}

// tmuxAnalystResubmit resubmits the same unchanged screen after its
// observation died: the producer discarded the report, or it expired, so no
// later pass can decide on it. The published versions are reused without a
// new write; only the observation identity and generation move forward.
func (e *Executor) tmuxAnalystResubmit(
	ctx context.Context,
	key string,
	last analystSubmission,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	instance tmuxinstance.Instance,
	locked tmuxattach.LockedCapture,
	screenHash string,
	detectorState detect.State,
	now time.Time,
) (observationID protocol.OpaqueID, guardVersion, draftGeneration int64, detail string, err error) {
	return e.tmuxAnalystSubmitFresh(ctx, key, agent, session, instance, locked,
		screenHash, last.guardVersion, last.draftGeneration, detectorState,
		last.generation, now)
}

// tmuxAnalystReport builds the observation report for one locked screen.
// tmux reports no cursor position: (0,0) is a non-position and travels
// explicitly unknown, so no projection can mistake it for a located cursor.
// The detector pre-gate only submits a closed draft, so no projection
// relies on it. Report validation still requires a cursor inside the
// capture when the screen is available.
func (e *Executor) tmuxAnalystReport(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	instance tmuxinstance.Instance,
	locked tmuxattach.LockedCapture,
	observationID protocol.OpaqueID,
	generation, guardVersion int64,
	now time.Time,
) protocol.StateObservationReport {
	harnessVersion := instance.HarnessVersion
	if harnessVersion == "" {
		// Registration may not state the harness version; the session
		// manifest declares it. The report requires a version, so the
		// manifest is the honest fallback, never an invented value.
		harnessVersion = e.tmuxAnalystManifestVersion(ctx, agent, session)
	}
	unknown := false
	return protocol.StateObservationReport{
		AgentID: agent, SessionID: session,
		ObservationID: observationID, ObservedAt: now,
		Harness: instance.Harness, HarnessVersion: harnessVersion,
		ObservationGeneration: generation,
		InputGuardVersion:     guardVersion,
		ScreenState:           protocol.StateObservationScreenAvailable,
		ScreenRows:            tmuxScreenRows(locked.Screen),
		NativeEvents:          []protocol.StateObservationEvent{},
		Cols:                  int64(locked.Observation.Cols),
		Cursor:                &protocol.StateObservationCursor{Row: 0, Col: 0, Known: &unknown},
	}
}

// tmuxAnalystGate loads the delivery, checks the detector draft on the
// locked capture and builds the detector verdict as the session's input
// guard, mirroring the deterministic publication. The guard is published by
// the submission, not here. It returns a nil record
// when the delivery is gone or unavailable, and an empty hash with its
// detail when the screen cannot be submitted: an open draft, an unknown
// harness or an unprojectable screen. Both are silent drops without
// analysis; the caller persists them as DEFER evaluations without an
// analyzed state. The returned report is the published guard; active is true
// when recent or uncertain client activity holds the submission even though
// the draft is closed.
func (e *Executor) tmuxAnalystGate(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	locked tmuxattach.LockedCapture,
	entries []procguard.Entry,
	notice protocol.NoticeLine,
) (record *deliveryRecord, screenHash, detail string,
	report protocol.InputGuardReport, detectorState detect.State, active bool, err error,
) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, "", "", report, detectorState, false, fmt.Errorf("begin tmux analyst selection: %w", err)
	}
	loaded, err := e.loadDelivery(ctx, tx, candidate, instance.SessionID)
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			return nil, "", "", report, detectorState, false, nil
		}
		return nil, "", "", report, detectorState, false, fmt.Errorf("load tmux analyst selection: %w", err)
	}
	if loaded.sessionID != instance.SessionID ||
		loaded.sessionState != protocol.SessionStateConnected ||
		loaded.captureMode != protocol.CaptureModeTmuxAttach || !loaded.deliverable ||
		loaded.process != instance.Process() || e.noticeFor(loaded) != notice {
		return nil, "", "", report, detectorState, false, nil
	}
	// The registered pane is verified before anything is submitted: a
	// replaced pane must never reach the analysis, even when the new
	// screen still classifies as idle.
	registryObservation := tmuxinstance.PaneObservation{
		PaneID: locked.Observation.PaneID, PanePID: locked.Observation.PanePID,
		Width: locked.Observation.Cols, Height: locked.Observation.Rows,
	}
	if err := e.tmux.registry.Verify(ctx, instance, registryObservation); err != nil {
		if errors.Is(err, tmuxinstance.ErrInvalidated) {
			if closeErr := e.tmux.sessions.Invalidate(ctx, instance.AgentID,
				instance.SessionID); closeErr != nil {
				return nil, "", "", report, detectorState, false, errors.Join(
					fmt.Errorf("registered tmux instance invalid: %w", errTmuxNoWrite),
					closeErr)
			}
			return &loaded, "", "registered pane was replaced", report, detectorState, false, nil
		}
		return nil, "", "", report, detectorState, false, fmt.Errorf("verify registered tmux instance: %w", err)
	}
	harness, ok := tmuxDetectorHarness(instance.Harness)
	detectorState = detect.State{Detail: "unsupported-harness"}
	if ok {
		detectorState = detect.Classify(harness, e.tmux.detectorTable, locked.Screen,
			locked.Observation.Cols, locked.Observation.Rows,
			int(instance.PanePID), detectorProcesses(entries))
	}
	detectorState.IsCLI = detectorState.IsCLI && registeredInForeground(instance, entries)
	if rule := e.tmux.screenRule(instance.ID, detectorState, locked.Screen,
		e.clock.Now().UTC()); !rule.pass() {
		return &loaded, "", rule.detail(), report, detectorState, false, nil
	}
	clients, err := e.tmux.transport.ListClientActivity(ctx, target)
	if err != nil {
		return nil, "", "", report, detectorState, false, fmt.Errorf("list tmux client activity: %w", err)
	}
	humanActive, humanCertain := detect.HumanActivity(string(clients),
		string(target.Pane), e.clock.Now().UTC(), e.tmux.humanWindow)
	// Warm the resting process baseline while the screen is submittable, so
	// the first analyzed pass already finds it ready instead of spending a
	// choice on the warming pass. This mirrors the deterministic warming,
	// only earlier: the analyst route never chooses on an unwarned baseline.
	panelKey := string(instance.ID)
	workEntries := entries
	workCensusReady := true
	if instance.Harness == protocol.HarnessCodex {
		var harnessStart uint64
		if instance.HarnessStart > 0 {
			harnessStart = uint64(instance.HarnessStart)
		}
		workEntries, workCensusReady = procguard.CodexWorkCensus(entries,
			int(instance.HarnessPID), harnessStart,
			instance.HarnessArgv0, instance.HarnessArgv1)
	}
	if !e.tmux.baselineIsReady(panelKey) && detectorState.IsCLI && humanCertain &&
		!humanActive && workCensusReady {
		e.tmux.processGuard.Rebaseline(panelKey, workEntries)
		e.tmux.markBaselineReady(panelKey)
	}
	report, err = e.nextTmuxInputGuard(ctx, instance, detectorState, humanActive, humanCertain)
	if err != nil {
		return nil, "", "", report, detectorState, false, err
	}
	rows := tmuxScreenRows(locked.Screen)
	hash, err := tmuxProjectedScreenHash(e.tmux.projection, rows, locked.Observation.Cols)
	if err != nil {
		return nil, "", "", report, detectorState, false, fmt.Errorf("project tmux locked screen: %w", err)
	}
	return &loaded, hash, "", report, detectorState, humanActive || !humanCertain, nil
}

// persistAnalystDefer records a DEFER evaluation without an analyzed state:
// the screen was never submitted, so the draft guard carries the gate detail
// and p_idle stays failed. The state ID is empty on purpose: no analysis
// drove this deferral.
func (e *Executor) persistAnalystDefer(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	record deliveryRecord,
	stateID protocol.ULID,
	detail string,
	now time.Time,
) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux analyst deferral: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return nil
		}
		return err
	}
	record = loaded
	if record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable {
		_ = tx.Rollback()
		return nil
	}
	decided, err := e.channel.AuthorizePTYDelivery(ctx, tx, record.channelMessage(),
		record.DeliveryGeneration, "")
	if err != nil {
		return fmt.Errorf("authorize tmux analyst deferral: %w", err)
	}
	guards := tmuxAnalystGuards(record, now,
		guard(protocol.GuardNameHumanDraftClosed, false, detail),
		guard(protocol.GuardNamePIdleThreshold, false, "no analyzed state drove this deferral"))
	if _, err := e.insertDecision(ctx, tx, decisionRow{
		record: record, stateID: stateID,
		allowed:  []protocol.OrchestratorAction{protocol.OrchestratorActionDefer},
		selected: protocol.OrchestratorActionDefer,
		guards:   guards, channel: decided.ID(), status: protocol.ExecutionStatusExecuted,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tmux analyst deferral: %w", err)
	}
	return nil
}

// tmuxAnalystGuards is the decision evidence of the analyst route: durable
// record facts plus the analyzed draft and p_idle verdicts. The detector and
// process checks do not appear here; the write-time revalidation in
// analystRevalidate enforces them on the delivery-opening capture instead.
func tmuxAnalystGuards(
	record deliveryRecord,
	now time.Time,
	draft, idle protocol.GuardResult,
) []protocol.GuardResult {
	return []protocol.GuardResult{
		guard(protocol.GuardNameSessionConnected,
			record.sessionState == protocol.SessionStateConnected,
			"tmux session is not CONNECTED"),
		guard(protocol.GuardNameDeliverable,
			record.deliverable && record.captureMode == protocol.CaptureModeTmuxAttach,
			"session is not a deliverable tmux attachment"),
		draft, idle,
		guard(protocol.GuardNameNotExpired,
			record.expiresAt.After(now), "delivery expired"),
		guard(protocol.GuardNameNotDuplicate, true, ""),
	}
}

// tmuxProjectedScreenHash projects visible rows under the deploy policies and
// hashes the projected lines. The draft is always closed and the cursor
// always unknown here: the analyst route only hashes a screen it analyzes,
// and tmux never reports a cursor position.
func tmuxProjectedScreenHash(
	policies screenproj.Policies,
	rows []string,
	cols int,
) (string, error) {
	projection, err := screenproj.Project(screenproj.Input{
		Rows: rows, Cols: cols, Draft: protocol.DraftStateClosed,
	}, policies)
	if err != nil {
		return "", fmt.Errorf("project tmux screen: %w", err)
	}
	sum := sha256.Sum256([]byte(strings.Join(projection.Lines, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

// tmuxAnalystGeneration assigns the next monotonic observation generation
// of the session, seeded from the durable snapshots and the previous
// in-process submission. The guard versions always come from the freshly
// published input-guard report instead.
func (e *Executor) tmuxAnalystGeneration(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	previous int64,
) (int64, error) {
	var durable int64
	if err := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(observation_generation), 0)
		FROM state_snapshots WHERE agent_id = ? AND session_id = ?`,
		agent, session).Scan(&durable); err != nil {
		return 0, fmt.Errorf("read tmux analyst generation: %w", err)
	}
	generation := durable + 1
	if generation <= previous {
		generation = previous + 1
	}
	return generation, nil
}

// readAnalyzedState reads the latest analyzed state once and reports whether
// it answers the submitted observation on this session. A miss is an
// ordinary wait: the caller ends the pass without persisting anything. It
// never invents analysis.
func (e *Executor) readAnalyzedState(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	observationID protocol.OpaqueID,
) (StateRead, bool, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return StateRead{}, false, fmt.Errorf("begin tmux analyst read: %w", err)
	}
	defer rollback(tx)
	state, err := e.states.ReadLatestForAutomatic(ctx, tx, agent, session,
		e.clock.Now().UTC(), e.expectedStateProfile())
	if err != nil {
		if errors.Is(err, ErrStateObservationNotFound) {
			_ = tx.Rollback()
			return StateRead{}, false, nil
		}
		return StateRead{}, false, fmt.Errorf("read tmux analyzed state: %w", err)
	}
	if err := tx.Rollback(); err != nil {
		return StateRead{}, false, fmt.Errorf("rollback tmux analyst read: %w", err)
	}
	if state.ObservationID != observationID || state.SessionID != session {
		return StateRead{}, false, nil
	}
	return state, true, nil
}

// finishTmuxAnalystChoice asks the model between injecting and deferring on
// the analyzed state, then commits the choice. The screen hash pins the
// analyzed capture; the write-time comparison enforces it.
func (e *Executor) finishTmuxAnalystChoice(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	notice protocol.NoticeLine,
	record deliveryRecord,
	analyzed StateRead,
	screenHash string,
	published protocol.InputGuardReport,
	now time.Time,
) (tmuxDetectorFallbackReason, error) {
	decided, err := e.authorizeTmuxAnalystDelivery(ctx, candidate, record)
	if err != nil {
		if errors.Is(err, errTmuxAnalystGone) {
			return "", nil
		}
		return "", err
	}
	if !decided.Allowed() {
		return "", e.persistTmuxAnalystDecision(ctx, candidate, record, analyzed,
			modelChoice{action: protocol.OrchestratorActionDefer}, false,
			&pendingChoice{candidate: candidate, decided: decided})
	}
	manifest, queuedAt := e.tmuxAnalystContext(ctx, record)
	allowed := []protocol.OrchestratorAction{
		protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
	}
	request, requestOK := tmuxChooseRequest(e, record, analyzed, manifest,
		queuedAt, decided, allowed, now)
	pending := &pendingChoice{
		candidate: candidate, decided: decided, allowed: allowed,
		guards: tmuxAnalystGuards(record, now,
			guard(protocol.GuardNameHumanDraftClosed, true, ""),
			pIdleGuard(analyzed)),
		stateID: analyzed.Snapshot.StateID, request: request, requestOK: requestOK,
	}
	choice, outcome := e.choose(ctx, pending)
	if outcome != choiceCallValid {
		switch outcome {
		case choiceCallCircuitOpen:
			return tmuxFallbackCircuitOpen, nil
		case choiceCallFailed:
			return tmuxFallbackChooseFailed, nil
		case choiceCallUnsent:
			return tmuxFallbackModelUnavailable, nil
		default:
			return tmuxFallbackChooseFailed, nil
		}
	}
	if choice.action != protocol.OrchestratorActionInjectPTYPrompt {
		return "", e.persistTmuxAnalystDecision(ctx, candidate, record, analyzed,
			choice, true, pending)
	}
	verified, err := e.verifyTmuxAnalystScreen(ctx, candidate, instance, target,
		notice, record, analyzed, screenHash, decided, pending, choice)
	if err != nil {
		return "", err
	}
	if !verified {
		return "", nil
	}
	return "", e.injectTmuxAnalyst(ctx, candidate, instance, target, notice, record,
		analyzed, screenHash, published, pending, choice)
}

// errTmuxAnalystGone marks a delivery that vanished, expired or became
// unavailable between the analyst passes. The caller drops it silently: the
// queue row is gone or expired, so there is nothing to decide.
var errTmuxAnalystGone = errors.New("tmux analyst delivery is gone")

// authorizeTmuxAnalystDelivery reloads the delivery and authorizes the
// channel controls before the model is asked. The authorization commits on
// its own transaction so the model call runs with nothing held, mirroring
// the automatic tx A; commitSent revalidates the controls before any write.
func (e *Executor) authorizeTmuxAnalystDelivery(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	record deliveryRecord,
) (channel.Decision, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return channel.Decision{}, fmt.Errorf("begin tmux analyst authorization: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return channel.Decision{}, errTmuxAnalystGone
		}
		return channel.Decision{}, err
	}
	record = loaded
	if record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable {
		_ = tx.Rollback()
		return channel.Decision{}, errTmuxAnalystGone
	}
	if !record.expiresAt.After(e.clock.Now().UTC()) {
		if err := expireQueue(ctx, tx, record, e.clock.Now().UTC()); err != nil {
			return channel.Decision{}, err
		}
		if err := tx.Commit(); err != nil {
			return channel.Decision{}, fmt.Errorf("commit expired tmux analyst delivery: %w", err)
		}
		e.tmuxForgetAnalystSubmission(record.AgentID, record.sessionID,
			candidate.MessageID, candidate.DeliveryGeneration)
		return channel.Decision{}, errTmuxAnalystGone
	}
	decided, err := e.channel.AuthorizePTYDelivery(ctx, tx, record.channelMessage(),
		record.DeliveryGeneration, "")
	if err != nil {
		return channel.Decision{}, fmt.Errorf("authorize tmux analyst delivery: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return channel.Decision{}, fmt.Errorf("commit tmux analyst authorization: %w", err)
	}
	return decided, nil
}

// tmuxAnalystManifestVersion reads the session manifest's declared harness
// version. It feeds the analysis submission when registration states none.
func (e *Executor) tmuxAnalystManifestVersion(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
) string {
	var manifestJSON string
	err := e.db.QueryRowContext(ctx, `SELECT manifest_json FROM sessions
		WHERE session_id = ? AND agent_id = ?`, session, agent).Scan(&manifestJSON)
	if err != nil {
		return ""
	}
	var caps protocol.CapabilityManifest
	if json.Unmarshal([]byte(manifestJSON), &caps) != nil || caps.Validate() != nil {
		return ""
	}
	return caps.HarnessVersion
}

// tmuxAnalystContext reads the session manifest and queue time of the
// choice request. Missing rows fail the request closed: the model is never
// asked on partial provenance.
func (e *Executor) tmuxAnalystContext(
	ctx context.Context,
	record deliveryRecord,
) (manifest string, queuedAt time.Time) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", time.Time{}
	}
	defer rollback(tx)
	var queuedAtRaw string
	if err := tx.QueryRowContext(ctx, `SELECT s.manifest_json, q.created_at FROM sessions AS s
		JOIN delivery_queue AS q ON q.agent_id = s.agent_id
		WHERE s.session_id = ? AND s.agent_id = ?
		AND q.message_id = ? AND q.agent_id = ? AND q.delivery_generation = ?`,
		record.sessionID, record.AgentID,
		record.MessageID, record.AgentID, record.DeliveryGeneration).Scan(
		&manifest, &queuedAtRaw); err != nil {
		_ = tx.Rollback()
		return "", time.Time{}
	}
	_ = tx.Rollback()
	queuedAt, err = eventstore.ParseTime(queuedAtRaw)
	if err != nil {
		return "", time.Time{}
	}
	return manifest, queuedAt
}

// tmuxChooseRequest builds the CHOOSE_ORCHESTRATION_ACTION request of an
// analyzed tmux capture. Like the automatic request it carries state, never
// the screen; unlike it, the message was never triaged, so the triage label
// stays empty instead of inventing a verdict.
func tmuxChooseRequest(
	e *Executor,
	record deliveryRecord,
	state StateRead,
	manifest string,
	queuedAt time.Time,
	decided channel.Decision,
	allowed []protocol.OrchestratorAction,
	now time.Time,
) (protocol.InferRequest, bool) {
	var caps protocol.CapabilityManifest
	if json.Unmarshal([]byte(manifest), &caps) != nil || caps.Validate() != nil {
		return protocol.InferRequest{}, false
	}
	order, err := e.shuffle(allowed)
	if err != nil {
		return protocol.InferRequest{}, false
	}
	effective := decided.Effective()
	autoContinue := effective.FromAutoContinue
	payload, err := json.Marshal(protocol.OrchestrationRequest{
		TaskType: protocol.ModelContractChooseOrchestrationAction,
		Agent: protocol.OrchestrationAgent{
			ID: record.AgentID, Harness: caps.Harness, Adapter: caps.AdapterType,
			Capabilities: caps.Capabilities,
		},
		State: protocol.OrchestrationState{
			HumanDraft: state.Snapshot.HumanDraftState, AgentPhase: state.Snapshot.AgentPhase,
			PIdle: state.IdleProbability, Attention: state.Snapshot.AttentionState,
		},
		Message: &protocol.PendingObject{
			// Tmux deliveries are never triaged, and the contract
			// requires a label: routine by default, never urgent.
			Priority: record.priority, Triage: protocol.TriageLabelNormal,
			AgeMS: max(0, now.Sub(queuedAt).Milliseconds()),
		},
		Channel: protocol.OrchestrationChannel{
			DecisionID: decided.ID(), Outcome: decided.Outcome(),
			Global: effective.GlobalMode, Link: effective.LinkMode, AutoContinue: &autoContinue,
		},
		AllowedActions: order,
	})
	if err != nil {
		return protocol.InferRequest{}, false
	}
	infer := protocol.InferRequest{
		Contract: protocol.ModelContractChooseOrchestrationAction, AgentID: record.AgentID,
		Payload: protocol.Payload(payload),
	}
	if infer.Validate() != nil {
		return protocol.InferRequest{}, false
	}
	return infer, true
}

// persistTmuxAnalystDecision records the choice outcome with the analyzed
// state linked: a model or channel DEFER, a failed call, or a stale link.
// The record is reloaded and the duplicate evidence rechecked first, so a
// delivery another pass resolved is dropped instead of decided twice.
func (e *Executor) persistTmuxAnalystDecision(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	record deliveryRecord,
	analyzed StateRead,
	choice modelChoice,
	validChoice bool,
	pending *pendingChoice,
) error {
	if !validChoice {
		choice = modelChoice{action: protocol.OrchestratorActionDefer}
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux analyst decision: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return nil
		}
		return err
	}
	record = loaded
	if record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable {
		_ = tx.Rollback()
		return nil
	}
	if !record.expiresAt.After(e.clock.Now().UTC()) {
		if err := expireQueue(ctx, tx, record, e.clock.Now().UTC()); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit expired tmux analyst decision: %w", err)
		}
		e.tmuxForgetAnalystSubmission(record.AgentID, record.sessionID,
			candidate.MessageID, candidate.DeliveryGeneration)
		return nil
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return fmt.Errorf("check committed tmux analyst evidence: %w", err)
	}
	uncertain, err := e.attempts.HasUncertain(ctx, tx, record.AgentID)
	if err != nil {
		return fmt.Errorf("check uncertain tmux analyst evidence: %w", err)
	}
	if committed || uncertain {
		_ = tx.Rollback()
		return nil
	}
	guards := tmuxAnalystGuards(record, e.clock.Now().UTC(),
		guard(protocol.GuardNameHumanDraftClosed,
			analyzed.Snapshot.HumanDraftState == protocol.DraftStateClosed,
			"analyzed draft is not closed"),
		pIdleGuard(analyzed))
	if _, err := e.insertDecision(ctx, tx, choiceRow(pending, choice, record,
		guards, protocol.ExecutionStatusExecuted)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tmux analyst decision: %w", err)
	}
	return nil
}

// persistTmuxAnalystMismatch records a DEFER after a valid CHOOSE when the
// delivery-opening screen no longer matches the analyzed screen. The row
// keeps the full causality: the analyzed state, the model choice that drove
// the injection (its call ID links the CHOOSE call), and a failing
// screen_unchanged guard as the durable mismatch reason. The allowed set
// stays the real CHOOSE set, never a rewritten DEFER-only set, so the row
// passes OrchestratorDecision.Validate with the causal probabilities. The
// model is never blamed: selected stays DEFER while the choice provenance
// stays Inject.
func (e *Executor) persistTmuxAnalystMismatch(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	record deliveryRecord,
	analyzed StateRead,
	decided channel.Decision,
	pending *pendingChoice,
	choice modelChoice,
) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux analyst mismatch: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return nil
		}
		return err
	}
	record = loaded
	if record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable {
		_ = tx.Rollback()
		return nil
	}
	if !record.expiresAt.After(e.clock.Now().UTC()) {
		if err := expireQueue(ctx, tx, record, e.clock.Now().UTC()); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit expired tmux analyst mismatch: %w", err)
		}
		e.tmuxForgetAnalystSubmission(record.AgentID, record.sessionID,
			candidate.MessageID, candidate.DeliveryGeneration)
		return nil
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return fmt.Errorf("check committed tmux analyst evidence: %w", err)
	}
	uncertain, err := e.attempts.HasUncertain(ctx, tx, record.AgentID)
	if err != nil {
		return fmt.Errorf("check uncertain tmux analyst evidence: %w", err)
	}
	if committed || uncertain {
		_ = tx.Rollback()
		return nil
	}
	now := e.clock.Now().UTC()
	guards := append(tmuxAnalystGuards(record, now,
		guard(protocol.GuardNameHumanDraftClosed,
			analyzed.Snapshot.HumanDraftState == protocol.DraftStateClosed,
			"analyzed draft is not closed"),
		pIdleGuard(analyzed)),
		guard(protocol.GuardNameScreenUnchanged, false,
			"TMUX_ANALYST_SCREEN_CHANGED: delivery-opening screen moved after CHOOSE"))
	// The allowed set is the real CHOOSE set from the pending choice, not a
	// rewritten DEFER-only set: probabilities must stay inside it for the
	// row to persist, and rewriting it would erase what the model saw.
	// insertDecision enforces the row rules before writing.
	if _, err := e.insertDecision(ctx, tx, decisionRow{
		record: record, stateID: analyzed.Snapshot.StateID,
		allowed:       pending.allowed,
		selected:      protocol.OrchestratorActionDefer,
		probabilities: choice.probabilities, callID: choice.callID, model: choice.model,
		guards: guards, channel: decided.ID(), status: protocol.ExecutionStatusExecuted,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tmux analyst mismatch: %w", err)
	}
	return nil
}

// injectTmuxAnalyst commits the injection: it revalidates everything the
// choice saw, persists the Inject evaluation with the analyzed state linked,
// reserves the single attempt and hands the pinned guard to the transport.
// The write-time comparison then enforces the analyzed screen.
func (e *Executor) injectTmuxAnalyst(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	notice protocol.NoticeLine,
	record deliveryRecord,
	analyzed StateRead,
	screenHash string,
	published protocol.InputGuardReport,
	pending *pendingChoice,
	choice modelChoice,
) error {
	// The commit decides on a fresh clock, never on the pre-submit one the
	// choice saw: an expiry crossed while the model chose must stop the
	// injection here instead of writing on dead evidence.
	now := e.clock.Now().UTC()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux analyst injection: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return nil
		}
		return err
	}
	record = loaded
	if record.sessionID != instance.SessionID ||
		record.sessionState != protocol.SessionStateConnected ||
		record.captureMode != protocol.CaptureModeTmuxAttach || !record.deliverable ||
		record.process != instance.Process() || e.noticeFor(record) != notice {
		_ = tx.Rollback()
		return e.persistTmuxAnalystDecision(ctx, candidate, record, analyzed,
			modelChoice{}, false, pending)
	}
	if !record.expiresAt.After(now) {
		if err := expireQueue(ctx, tx, record, now); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit expired tmux analyst injection: %w", err)
		}
		e.tmuxForgetAnalystSubmission(record.AgentID, record.sessionID,
			candidate.MessageID, candidate.DeliveryGeneration)
		return nil
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return fmt.Errorf("check committed tmux analyst evidence: %w", err)
	}
	uncertain, err := e.attempts.HasUncertain(ctx, tx, record.AgentID)
	if err != nil {
		return fmt.Errorf("check uncertain tmux analyst evidence: %w", err)
	}
	if committed || uncertain {
		_ = tx.Rollback()
		return nil
	}
	fresh, err := e.states.ReadLatestForAutomatic(ctx, tx, record.AgentID,
		record.sessionID, now, e.expectedStateProfile())
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, ErrStateObservationNotFound) {
			return e.persistTmuxAnalystDecision(ctx, candidate, record, analyzed,
				modelChoice{}, false, pending)
		}
		return fmt.Errorf("revalidate tmux analyzed state: %w", err)
	}
	if fresh.ObservationID != analyzed.ObservationID ||
		fresh.Snapshot.HumanDraftState != protocol.DraftStateClosed ||
		!fresh.AutomaticEligible {
		_ = tx.Rollback()
		return e.persistTmuxAnalystDecision(ctx, candidate, record, fresh,
			modelChoice{}, false, pending)
	}
	message := record.channelMessage()
	guards := tmuxAnalystGuards(record, now,
		guard(protocol.GuardNameHumanDraftClosed, true, ""),
		pIdleGuard(fresh))
	persisted, err := e.insertDecision(ctx, tx, choiceRow(pending, choice,
		record, guards, protocol.ExecutionStatusPending))
	if err != nil {
		return err
	}
	guard := &tmuxDeliveryGuard{
		executor: e, runtime: e.tmux, candidate: candidate, instance: instance,
		target: target, notice: notice, analyzedScreenHash: screenHash,
		// The attempt carries the published input-guard versions the
		// analysis validated against.
		report: published,
	}
	if err := e.claimDelivery(ctx, tx, record); err != nil {
		return err
	}
	operation, err := tmuxPaneOperation(ctx, tx, record)
	if err != nil {
		return err
	}
	attempt, err := guard.createAttempt(ctx, tx, record, pending.decided, persisted)
	if err != nil {
		return err
	}
	if err := e.attempts.MarkPrepared(ctx, tx, attempt.DeliveryID, attempt.WriterLeaseID); err != nil {
		return fmt.Errorf("mark tmux analyst attempt prepared: %w", err)
	}
	attempt.State = daemon.AttemptPrepared
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prepared tmux analyst attempt: %w", err)
	}
	guard.plan = &executionPlan{
		record: record, decision: pending.decided, attempt: attempt,
		message: message, report: guard.report,
	}
	if err := guard.commitSent(ctx); err != nil {
		return err
	}
	ctx = tmuxattach.WithPaneOperation(ctx, operation)
	result, err := e.tmux.transport.Deliver(ctx, tmuxattach.Request{Target: target, Notice: notice}, guard)
	e.tmux.logAnalystNotWritten(ctx, result, err)
	if errors.Is(err, errTmuxNoWrite) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deliver tmux analyst notice: %w", err)
	}
	return nil
}

// verifyTmuxAnalystScreen re-reads the pane under a fresh lock after CHOOSE
// and before the injection commits. It compares the projected screen with
// the analyzed hash, re-runs the write-safety checks and revalidates the
// analyzed state with a fresh clock, all without an attempt and without
// typing: a moved or busy pane, or an analysis that expired, revoked or
// lost its guard race while the model chose, defers here with no attempt
// and no write, while the write-time comparison stays as the final backstop
// for a race inside the commit itself.
func (e *Executor) verifyTmuxAnalystScreen(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	notice protocol.NoticeLine,
	record deliveryRecord,
	analyzed StateRead,
	screenHash string,
	decided channel.Decision,
	pending *pendingChoice,
	choice modelChoice,
) (bool, error) {
	locked, err := e.tmux.transport.CaptureLocked(ctx, target)
	if err != nil {
		return false, errTmuxAnalystWait
	}
	deferDecision := func() (bool, error) {
		err := e.persistTmuxAnalystDecision(ctx, candidate, record, analyzed,
			modelChoice{}, false, &pendingChoice{
				candidate: candidate, decided: decided,
				allowed: []protocol.OrchestratorAction{protocol.OrchestratorActionDefer},
			})
		if err != nil {
			return false, err
		}
		return false, nil
	}
	current, err := tmuxProjectedScreenHash(e.tmux.projection,
		tmuxScreenRows(locked.Screen), locked.Observation.Cols)
	if err != nil || current != screenHash {
		// The screen moved between CHOOSE and the commit: keep the
		// analyzed state and the valid choice linked, and mark the
		// mismatch as the durable reason instead of blaming the model.
		if err := e.persistTmuxAnalystMismatch(ctx, candidate, record,
			analyzed, decided, pending, choice); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := e.checkTmuxWriteSafety(ctx, instance, target, locked.Observation,
		nil, locked.Screen); err != nil {
		if errors.Is(err, tmuxinstance.ErrInvalidated) {
			return false, nil
		}
		return deferDecision()
	}
	return e.revalidateTmuxAnalystPostChoose(ctx, candidate, instance,
		notice, record, analyzed, decided)
}

// revalidateTmuxAnalystPostChoose reloads the delivery and rereads the
// analyzed state with a fresh clock after CHOOSE. The choice saw the world
// at choose time; anything that moved since — an expired or revoked
// analysis, a new input guard, a changed session — must defer instead of
// injecting on stale evidence. A stale basis persists a DEFER on the
// current truth, never on the dead one; the persisting path reloads again
// and drops, expires or persists as appropriate.
func (e *Executor) revalidateTmuxAnalystPostChoose(
	ctx context.Context,
	candidate daemon.QueueDelivery,
	instance tmuxinstance.Instance,
	notice protocol.NoticeLine,
	record deliveryRecord,
	analyzed StateRead,
	decided channel.Decision,
) (bool, error) {
	freshNow := e.clock.Now().UTC()
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("begin tmux analyst post-choose read: %w", err)
	}
	defer rollback(tx)
	loaded, err := e.loadDelivery(ctx, tx, candidate, record.sessionID)
	if err != nil {
		if errors.Is(err, channel.ErrNotFound) {
			_ = tx.Rollback()
			return false, nil
		}
		return false, err
	}
	fresh, rerr := e.states.ReadLatestForAutomatic(ctx, tx, loaded.AgentID,
		loaded.sessionID, freshNow, e.expectedStateProfile())
	if rerr != nil && !errors.Is(rerr, ErrStateObservationNotFound) {
		_ = tx.Rollback()
		return false, fmt.Errorf("reread tmux analyzed state: %w", rerr)
	}
	_ = tx.Rollback()
	basis := analyzed
	if rerr == nil {
		basis = fresh
	}
	if rerr == nil && fresh.ObservationID == analyzed.ObservationID &&
		fresh.Snapshot.HumanDraftState == protocol.DraftStateClosed &&
		fresh.AutomaticEligible &&
		loaded.sessionID == instance.SessionID &&
		loaded.sessionState == protocol.SessionStateConnected &&
		loaded.captureMode == protocol.CaptureModeTmuxAttach && loaded.deliverable &&
		loaded.process == instance.Process() && e.noticeFor(loaded) == notice &&
		loaded.expiresAt.After(freshNow) {
		return true, nil
	}
	if err := e.persistTmuxAnalystDecision(ctx, candidate, loaded, basis,
		modelChoice{}, false, &pendingChoice{
			candidate: candidate, decided: decided,
			stateID: basis.Snapshot.StateID,
			allowed: []protocol.OrchestratorAction{protocol.OrchestratorActionDefer},
		}); err != nil {
		return false, err
	}
	return false, nil
}

// checkTmuxWriteSafety runs the detector, foreground, registry and
// process-baseline checks on a locked capture, plus the client-activity
// check when the delivery-opening rows are known. A nil clients skips the
// activity check: the pre-commit verification has no delivery-opening rows
// yet, while the write-time revalidation enforces them. It never decides,
// persists or plans.
func (e *Executor) checkTmuxWriteSafety(
	ctx context.Context,
	instance tmuxinstance.Instance,
	target tmuxattach.Target,
	observation tmuxattach.PaneObservation,
	clients []byte,
	screen []byte,
) error {
	registryObservation := tmuxinstance.PaneObservation{
		PaneID: observation.PaneID, PanePID: observation.PanePID,
		Width: observation.Cols, Height: observation.Rows,
	}
	if err := e.tmux.registry.Verify(ctx, instance, registryObservation); err != nil {
		if errors.Is(err, tmuxinstance.ErrInvalidated) {
			closeErr := e.tmux.sessions.Invalidate(ctx, instance.AgentID, instance.SessionID)
			return errors.Join(fmt.Errorf("registered tmux instance invalid: %w", errTmuxNoWrite), closeErr)
		}
		return fmt.Errorf("verify registered tmux instance: %w", err)
	}
	entries, err := e.tmux.sampleTree(int(instance.PanePID))
	if err != nil {
		return fmt.Errorf("sample tmux process tree: %w", err)
	}
	harness, ok := tmuxDetectorHarness(instance.Harness)
	if !ok {
		return fmt.Errorf("tmux harness has no detector profile: %w", errTmuxNoWrite)
	}
	state := detect.Classify(harness, e.tmux.detectorTable, screen,
		observation.Cols, observation.Rows, int(instance.PanePID),
		detectorProcesses(entries))
	state.IsCLI = state.IsCLI && registeredInForeground(instance, entries)
	if !state.IsCLI {
		return fmt.Errorf("registered harness CLI is not in the foreground: %w", errTmuxNoWrite)
	}
	humanOK := true
	if clients != nil {
		humanActive, humanCertain := detect.HumanActivity(
			string(clients), string(target.Pane), e.clock.Now(), e.tmux.humanWindow)
		if !humanCertain || humanActive {
			return fmt.Errorf("client activity is recent or uncertain: %w", errTmuxNoWrite)
		}
		humanOK = humanCertain && !humanActive
	}
	panelKey := string(instance.ID)
	workEntries := entries
	workCensusReady := true
	if instance.Harness == protocol.HarnessCodex {
		var harnessStart uint64
		if instance.HarnessStart > 0 {
			harnessStart = uint64(instance.HarnessStart)
		}
		workEntries, workCensusReady = procguard.CodexWorkCensus(entries,
			int(instance.HarnessPID), harnessStart,
			instance.HarnessArgv0, instance.HarnessArgv1)
	}
	baselineReady := e.tmux.baselineIsReady(panelKey)
	processVerdict := e.tmux.processGuard.Check(panelKey, workEntries)
	if !workCensusReady {
		processVerdict = procguard.Busy
	}
	confirmedRest := state.Certain && !state.Busy && !state.BoxBusy && state.IsCLI &&
		humanOK && workCensusReady
	if !baselineReady && confirmedRest {
		e.tmux.processGuard.Rebaseline(panelKey, workEntries)
		e.tmux.markBaselineReady(panelKey)
		baselineReady = true
	}
	if !baselineReady || processVerdict != procguard.Free {
		return fmt.Errorf("tmux process baseline is not quiet: %w", errTmuxNoWrite)
	}
	if state.Busy || state.BoxBusy || !state.Certain {
		return fmt.Errorf("tmux screen is busy or uncertain: %w", errTmuxNoWrite)
	}
	return nil
}

// analystRevalidate is the safety-only revalidation of the analyst route. It
// runs under the pane lock on the delivery-opening capture: the analyzed
// screen hash was already compared by Revalidate, and the choosing already
// happened, so this checks only that the world still allows a write. It
// never decides, persists or plans.
func (g *tmuxDeliveryGuard) analystRevalidate(
	ctx context.Context,
	target tmuxattach.Target,
	observation tmuxattach.PaneObservation,
	clients []byte,
	screen []byte,
) error {
	if target != g.target || observation.PaneID != target.Pane {
		return fmt.Errorf("tmux target changed: %w", errTmuxNoWrite)
	}
	if err := g.executor.checkTmuxWriteSafety(ctx, g.instance, target,
		observation, clients, screen); err != nil {
		return err
	}
	g.observation = observation
	return nil
}
