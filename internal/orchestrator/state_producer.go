package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/screenproj"
)

const (
	// StateProducerVersion marks rows written by this sole state_snapshots
	// writer. NULL marks legacy rows and is never eligible.
	StateProducerVersion = "state-producer-5.5a"
	stateAnalysisPolicy  = "state-analysis-v1"
	// StateAnalysisPolicyVersion is sealed into selectable ANALYZE tuples and
	// every PTY-backed state snapshot.
	StateAnalysisPolicyVersion = stateAnalysisPolicy
	stateNativeAnalyst         = "state-native-v2"
	stateTTL                   = 10 * time.Second
	stateCommitInterval        = time.Second
	statePollEvery             = 100 * time.Millisecond
)

var (
	// ErrStateProducerOptions reports missing or inconsistent dependencies.
	ErrStateProducerOptions = errors.New("invalid state producer options")
	// ErrObservationSuperseded reports that newer generations arrived before commit.
	ErrObservationSuperseded = errors.New("state observation superseded")
	// ErrObservationUnavailable reports a missing session or an already-stored replay.
	ErrObservationUnavailable = errors.New("state observation unavailable")
	errStateCommitRateLimited = errors.New("state snapshot commit rate limited")
)

type stateProducerClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// StateAnalyzer is the Analyst surface used by StateProducer.
type StateAnalyzer interface {
	Analyze(context.Context, analyst.Observation) (analyst.Analysis, error)
}

// StateChannelReader reads an effective channel view through the producer's
// transaction. *channel.Controller satisfies it.
type StateChannelReader interface {
	StateContextTx(context.Context, channel.Queryer, protocol.AgentID, time.Time) (
		protocol.AgentChannel, []protocol.LoopSummary, int64, error,
	)
}

// StateAnalysisProfile pins evidence that does not live in a gateway call
// record: the deployed profile document and the calibration containing T and
// theta. It is absent on deterministic native observations.
type StateAnalysisProfile struct {
	SelectionGeneration int64
	Revision            string
	SHA256              protocol.SHA256Hex
	CalibrationRevision string
	CalibrationSHA256   protocol.SHA256Hex
	Temperature         float64
	IdleThreshold       float64
}

func (p StateAnalysisProfile) valid() bool {
	return p.Revision != "" && len(p.Revision) <= 100 && p.SHA256.Valid() &&
		p.CalibrationRevision != "" && len(p.CalibrationRevision) <= 100 &&
		p.CalibrationSHA256.Valid() && p.Temperature > 0 && p.Temperature <= 100 &&
		p.IdleThreshold > 0 && p.IdleThreshold <= 1 &&
		!math.IsNaN(p.Temperature) && !math.IsInf(p.Temperature, 0) &&
		!math.IsNaN(p.IdleThreshold) && !math.IsInf(p.IdleThreshold, 0)
}

// StateProducerOptions configures the unique state_snapshots writer. Analyzer
// may be nil; the producer then commits honest UNKNOWN phase observations.
type StateProducerOptions struct {
	DB          *sql.DB
	Clock       stateProducerClock
	Channel     StateChannelReader
	Analyzer    StateAnalyzer
	Profile     StateAnalysisProfile
	Selection   ModelSelectionProvider
	Persistence *PersistenceGate
	Entropy     io.Reader
	ErrorSink   func(error)
}

// ModelSelectionProvider publishes the immutable ANALYZE tuple shared by the
// Gateway, StateProducer and Executor.
type ModelSelectionProvider interface {
	CurrentModelSelection() protocol.ModelSelection
}

type pendingStateObservation struct {
	report protocol.StateObservationReport
	head   stateEvidenceHead
}

type cachedStateAnalysis struct {
	key        [sha256.Size]byte
	report     protocol.StateObservationReport
	prepared   statePreparation
	profile    StateAnalysisProfile
	generation int64
	analysis   analyst.Analysis
	expiresAt  time.Time
	stateID    protocol.ULID
}

type selectedStateAnalysis struct {
	key                [sha256.Size]byte
	analysis           analyst.Analysis
	inferenceExpiresAt time.Time
	profile            StateAnalysisProfile
	generation         int64
	reused             bool
	demandSatisfied    bool
}

type stateSessionKey struct {
	agent   protocol.AgentID
	session protocol.OpaqueID
}

type stateEvidenceHead struct {
	fingerprint           [sha256.Size]byte
	observationID         protocol.OpaqueID
	observedAt            time.Time
	observationGeneration int64
	outputGeneration      int64
	eventGeneration       int64
	inputGuardVersion     int64
}

type reconciledStateEvidence struct {
	stateID protocol.ULID
	head    stateEvidenceHead
}

// StateProducer normalises observations and serialises every snapshot commit.
// Submit is safe for concurrent synthetic sources; Run is called exactly once.
type StateProducer struct {
	db          *sql.DB
	clock       stateProducerClock
	channel     StateChannelReader
	analyzer    StateAnalyzer
	profile     StateAnalysisProfile
	selection   ModelSelectionProvider
	persistence *PersistenceGate
	entropy     io.Reader
	errSink     func(error)
	wake        chan struct{}

	mu          sync.Mutex
	pending     map[protocol.AgentID]pendingStateObservation
	inFlight    map[protocol.AgentID]pendingStateObservation
	latest      map[stateSessionKey]stateEvidenceHead
	reconciled  map[stateSessionKey]reconciledStateEvidence
	demanded    map[protocol.AgentID]bool
	lastCommit  map[protocol.AgentID]time.Time
	analysis    map[protocol.AgentID]cachedStateAnalysis
	nativeCache nativeHistoryCache
	idMu        sync.Mutex
	lastMillis  int64
	lastEntropy [10]byte
}

// NewStateProducer validates opts. A configured Analyzer requires exact
// profile and calibration evidence; a disabled Analyzer requires neither.
func NewStateProducer(opts StateProducerOptions) (*StateProducer, error) {
	if opts.DB == nil || opts.Clock == nil || opts.Channel == nil ||
		(opts.Analyzer != nil && !opts.Profile.valid() && opts.Selection == nil) {
		return nil, ErrStateProducerOptions
	}
	if opts.Analyzer != nil && opts.Selection != nil {
		selected := opts.Selection.CurrentModelSelection()
		profile, _ := stateProfileFromRecord(&protocol.GlimmerCallRecord{
			SelectionGeneration: selected.Generation,
			ProfileRevision:     selected.ProfileRevision, ProfileSHA256: selected.ProfileSHA256,
			CalibrationRevision:    selected.CalibrationRevision,
			CalibrationSHA256:      selected.CalibrationSHA256,
			CalibrationTemperature: selected.CalibrationTemperature,
			IdleThreshold:          selected.IdleThreshold,
		}, StateAnalysisProfile{})
		if selected.Validate() != nil || !profile.valid() {
			return nil, ErrStateProducerOptions
		}
	}
	if opts.Entropy == nil {
		opts.Entropy = rand.Reader
	}
	if opts.ErrorSink == nil {
		opts.ErrorSink = func(error) {}
	}
	if opts.Persistence == nil {
		opts.Persistence = NewPersistenceGate()
	}
	return &StateProducer{
		db: opts.DB, clock: opts.Clock, channel: opts.Channel,
		analyzer: opts.Analyzer, profile: opts.Profile, selection: opts.Selection, persistence: opts.Persistence,
		entropy: opts.Entropy,
		errSink: opts.ErrorSink, wake: make(chan struct{}, 1),
		pending:    make(map[protocol.AgentID]pendingStateObservation),
		inFlight:   make(map[protocol.AgentID]pendingStateObservation),
		latest:     make(map[stateSessionKey]stateEvidenceHead),
		reconciled: make(map[stateSessionKey]reconciledStateEvidence),
		demanded:   make(map[protocol.AgentID]bool),
		lastCommit: make(map[protocol.AgentID]time.Time),
		analysis:   make(map[protocol.AgentID]cachedStateAnalysis),
	}, nil
}

func (p *StateProducer) currentAnalysisProfile() (StateAnalysisProfile, int64) {
	if p.selection == nil {
		return p.profile, 0
	}
	selected := p.selection.CurrentModelSelection()
	if selected.Generation < 1 || selected.Validate() != nil {
		return StateAnalysisProfile{}, selected.Generation
	}
	return StateAnalysisProfile{
		SelectionGeneration: selected.Generation,
		Revision:            selected.ProfileRevision, SHA256: selected.ProfileSHA256,
		CalibrationRevision: selected.CalibrationRevision,
		CalibrationSHA256:   selected.CalibrationSHA256,
		Temperature:         selected.CalibrationTemperature, IdleThreshold: selected.IdleThreshold,
	}, selected.Generation
}

func stateProfileFromRecord(
	record *protocol.GlimmerCallRecord,
	fallback StateAnalysisProfile,
) (StateAnalysisProfile, int64) {
	if record == nil || record.SelectionGeneration < 1 {
		return fallback, 0
	}
	return StateAnalysisProfile{
		SelectionGeneration: record.SelectionGeneration,
		Revision:            record.ProfileRevision, SHA256: record.ProfileSHA256,
		CalibrationRevision: record.CalibrationRevision,
		CalibrationSHA256:   record.CalibrationSHA256,
		Temperature:         record.CalibrationTemperature, IdleThreshold: record.IdleThreshold,
	}, record.SelectionGeneration
}

// Submit accepts a safe, bounded observation and keeps only the newest one per
// agent. It does not write synchronously and never blocks a producer of input.
func (p *StateProducer) Submit(report protocol.StateObservationReport) error {
	if err := report.Validate(); err != nil {
		return fmt.Errorf("submit state observation: %w", err)
	}
	head, err := newStateEvidenceHead(report)
	if err != nil {
		return err
	}
	p.mu.Lock()
	key := stateSessionKey{agent: report.AgentID, session: report.SessionID}
	current, ok := p.pending[report.AgentID]
	latest, seen := p.latest[key]
	if (!ok || newerObservation(report, current.report)) &&
		(!seen || newerStateEvidenceHead(head, latest)) {
		entry := pendingStateObservation{
			report: cloneObservation(report),
			head:   head,
		}
		p.pending[report.AgentID] = entry
		p.latest[key] = head
		p.reconcileCompatibleRefreshLocked(key, entry)
	}
	p.mu.Unlock()
	p.notify()
	return nil
}

// Demand asks the next quiet observation for agent to run through Analyst.
// Demand does not create a snapshot and survives an observation supersession.
func (p *StateProducer) Demand(agent protocol.AgentID) error {
	if !agent.Valid() {
		return fmt.Errorf("demand state analysis: %w", ErrStateProducerOptions)
	}
	p.mu.Lock()
	p.demanded[agent] = true
	p.mu.Unlock()
	p.notify()
	return nil
}

// ReadLatestForAutomatic combines the pure durable read with pending, in-flight
// and last-accepted evidence. The caller supplies its decision transaction;
// holding the producer lock across that read prevents queue cleanup from
// exposing an older row as eligible between the two views. Human/manual audit
// paths use ReadLatestState directly and do not depend on producer memory.
func (p *StateProducer) ReadLatestForAutomatic(
	ctx context.Context,
	q stateQueryer,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	now time.Time,
	expected *StateAnalysisProfile,
) (StateRead, error) {
	if p == nil {
		return StateRead{}, ErrStateProducerOptions
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := ReadLatestState(ctx, q, agent, session, now, expected)
	if err != nil {
		return StateRead{}, err
	}
	key := stateSessionKey{agent: agent, session: session}
	latest, latestOK := p.latest[key]
	reconciled, reconciledOK := p.reconciled[key]
	pending, pendingOK := p.pending[agent]
	inFlight, inFlightOK := p.inFlight[agent]
	queueInvalid := (pendingOK && pending.report.SessionID == session &&
		(!reconciledOK || pending.head != reconciled.head)) ||
		(inFlightOK && inFlight.report.SessionID == session &&
			(!reconciledOK || inFlight.head != reconciled.head))
	if state.AutomaticEligible && (!latestOK || !reconciledOK ||
		latest != reconciled.head || reconciled.stateID != state.Snapshot.StateID || queueInvalid) {
		state.AutomaticEligible = false
		state.EligibilityReason = StateEligibilityObservationNewer
	}
	if p.persistence.Blocked() {
		state.AutomaticEligible = false
		state.EligibilityReason = StateEligibilityPersistence
	}
	return state, nil
}

func newStateEvidenceHead(report protocol.StateObservationReport) (stateEvidenceHead, error) {
	encoded, err := json.Marshal(report)
	if err != nil {
		return stateEvidenceHead{}, fmt.Errorf("hash state observation evidence: %w", err)
	}
	return stateEvidenceHead{
		fingerprint:           sha256.Sum256(encoded),
		observationID:         report.ObservationID,
		observedAt:            report.ObservedAt,
		observationGeneration: report.ObservationGeneration,
		outputGeneration:      report.OutputGeneration,
		eventGeneration:       report.EventGeneration,
		inputGuardVersion:     report.InputGuardVersion,
	}, nil
}

func newerStateEvidenceHead(next, current stateEvidenceHead) bool {
	// Every generation is monotonic within a session. Treat the head as a
	// partial order: a fresher transport identity must never hide a rollback
	// of output, event, or guard evidence that already invalidated a snapshot.
	if next.observationGeneration < current.observationGeneration ||
		next.outputGeneration < current.outputGeneration ||
		next.eventGeneration < current.eventGeneration ||
		next.inputGuardVersion < current.inputGuardVersion {
		return false
	}
	return next.observationGeneration > current.observationGeneration ||
		next.outputGeneration > current.outputGeneration ||
		next.eventGeneration > current.eventGeneration ||
		next.inputGuardVersion > current.inputGuardVersion ||
		next.observedAt.After(current.observedAt)
}

func (p *StateProducer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run is the producer's sole commit loop. It bounds global model concurrency
// at one and, therefore, also guarantees at most one ANALYZE call per agent.
func (p *StateProducer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		p.flush(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		case <-p.persistence.Changed():
		case <-p.clock.After(statePollEvery):
		}
	}
	return nil
}

func (p *StateProducer) flush(ctx context.Context) {
	if err := p.publishInitialStates(ctx); err != nil && ctx.Err() == nil {
		p.errSink(err)
	}
	if err := p.ensurePersistenceDegraded(ctx); err != nil && ctx.Err() == nil &&
		!errors.Is(err, errStateCommitRateLimited) &&
		!errors.Is(err, ErrObservationUnavailable) &&
		!errors.Is(err, ErrObservationSuperseded) {
		p.errSink(err)
	}
	for _, agent := range p.dueAgents() {
		if ctx.Err() != nil {
			return
		}
		if err := p.produceOne(ctx, agent); err != nil &&
			!errors.Is(err, ErrObservationSuperseded) &&
			!errors.Is(err, ErrObservationUnavailable) &&
			!errors.Is(err, errStateCommitRateLimited) {
			p.errSink(err)
		}
	}
}

func (p *StateProducer) ensurePersistenceDegraded(ctx context.Context) error {
	plan := p.persistence.snapshotPlan()
	if !plan.forceDegraded || p.hasAnyQueuedObservation() {
		return nil
	}
	reports, err := p.persistenceStateReports(ctx, plan.epoch)
	if err != nil {
		return err
	}
	for _, report := range reports {
		if !p.persistence.snapshotPlan().forceDegraded {
			return nil
		}
		prepared, prepareErr := p.prepare(ctx, report)
		if prepareErr != nil {
			if errors.Is(prepareErr, ErrObservationUnavailable) ||
				errors.Is(prepareErr, ErrObservationSuperseded) {
				continue
			}
			return prepareErr
		}
		result, commitErr := p.commit(
			ctx, report, prepared, unknownStateAnalysis(report.AgentID), time.Time{}, false, false,
		)
		if commitErr != nil {
			return commitErr
		}
		if result.inserted {
			p.mu.Lock()
			p.lastCommit[report.AgentID] = p.clock.Now()
			p.mu.Unlock()
		}
		return nil
	}
	return nil
}

func (p *StateProducer) hasAnyQueuedObservation() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending) != 0 || len(p.inFlight) != 0
}

func (p *StateProducer) persistenceStateReports(
	ctx context.Context,
	epoch uint64,
) ([]protocol.StateObservationReport, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT s.agent_id, s.session_id, s.manifest_json,
		COALESCE(g.version, 0), st.observation_generation,
		st.output_generation, st.event_generation
		FROM sessions s
		LEFT JOIN input_guards g
			ON g.agent_id = s.agent_id AND g.session_id = s.session_id
		JOIN state_snapshots st ON st.state_id = (
			SELECT latest.state_id FROM state_snapshots latest
			WHERE latest.agent_id = s.agent_id AND latest.session_id = s.session_id
			AND latest.producer_version IS NOT NULL
			ORDER BY latest.observed_at DESC, latest.state_version DESC LIMIT 1
		)
		WHERE s.state IN ('CONNECTED', 'OBSERVED')
		ORDER BY s.started_at, s.session_id LIMIT 64`)
	if err != nil {
		return nil, fmt.Errorf("select persistence recovery sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	now := p.clock.Now().UTC()
	reports := make([]protocol.StateObservationReport, 0, 64)
	for rows.Next() {
		var agentID, sessionID, manifestJSON string
		var guardVersion, observationGeneration, outputGeneration, eventGeneration int64
		if err := rows.Scan(
			&agentID, &sessionID, &manifestJSON, &guardVersion,
			&observationGeneration, &outputGeneration, &eventGeneration,
		); err != nil {
			return nil, fmt.Errorf("scan persistence recovery session: %w", err)
		}
		agent, session := protocol.AgentID(agentID), protocol.OpaqueID(sessionID)
		var manifest protocol.CapabilityManifest
		if !agent.Valid() || !session.Valid() ||
			json.Unmarshal([]byte(manifestJSON), &manifest) != nil ||
			manifest.Validate() != nil || manifest.AgentID != agent {
			continue
		}
		report := protocol.StateObservationReport{
			AgentID: agent, SessionID: session,
			ObservationID: persistenceStateObservationID(epoch, agent, session),
			ObservedAt:    now, Harness: manifest.Harness,
			HarnessVersion:        manifest.HarnessVersion,
			ObservationGeneration: observationGeneration,
			OutputGeneration:      outputGeneration, EventGeneration: eventGeneration,
			InputGuardVersion: guardVersion,
			ScreenState:       protocol.StateObservationScreenWithheld, ScreenRows: []string{},
			Activity:     protocol.StateObservationActivity{},
			NativeEvents: []protocol.StateObservationEvent{},
		}
		if report.Validate() == nil {
			reports = append(reports, report)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read persistence recovery sessions: %w", err)
	}
	return reports, nil
}

func persistenceStateObservationID(
	epoch uint64,
	agent protocol.AgentID,
	session protocol.OpaqueID,
) protocol.OpaqueID {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s", epoch, agent, session)))
	return protocol.OpaqueID(fmt.Sprintf(
		"state-persistence-%d-%s", epoch, hex.EncodeToString(sum[:16]),
	))
}

// publishInitialStates makes the human DEFER path available as soon as a
// durable session exists. It never reads run output and never calls Analyst:
// the resulting UNKNOWN snapshot is deliberately stale.
func (p *StateProducer) publishInitialStates(ctx context.Context) error {
	reports, err := p.initialStateReports(ctx)
	if err != nil {
		return err
	}
	for _, report := range reports {
		if p.hasQueuedObservation(report.AgentID) {
			continue
		}
		prepared, prepareErr := p.prepare(ctx, report)
		if prepareErr != nil {
			if errors.Is(prepareErr, ErrObservationUnavailable) ||
				errors.Is(prepareErr, ErrObservationSuperseded) {
				continue
			}
			return prepareErr
		}
		if p.hasQueuedObservation(report.AgentID) {
			continue
		}
		result, commitErr := p.commit(
			ctx, report, prepared, unknownStateAnalysis(report.AgentID), time.Time{}, false, true,
		)
		if commitErr != nil {
			if errors.Is(commitErr, ErrObservationUnavailable) ||
				errors.Is(commitErr, ErrObservationSuperseded) ||
				errors.Is(commitErr, errStateCommitRateLimited) {
				continue
			}
			return commitErr
		}
		if result.inserted {
			p.mu.Lock()
			p.lastCommit[report.AgentID] = p.clock.Now()
			p.mu.Unlock()
		}
	}
	return nil
}

func (p *StateProducer) initialStateReports(
	ctx context.Context,
) ([]protocol.StateObservationReport, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT s.agent_id, s.session_id, s.manifest_json,
		COALESCE((SELECT g.version FROM input_guards g
			WHERE g.agent_id = s.agent_id AND g.session_id = s.session_id), 0)
		FROM sessions s
		WHERE s.state IN ('CONNECTED', 'OBSERVED')
		AND NOT EXISTS (SELECT 1 FROM state_snapshots st
			WHERE st.agent_id = s.agent_id AND st.session_id = s.session_id
			AND st.producer_version IS NOT NULL)
		ORDER BY s.started_at, s.session_id LIMIT 64`)
	if err != nil {
		return nil, fmt.Errorf("select initial state sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	now := p.clock.Now().UTC()
	reports := make([]protocol.StateObservationReport, 0, 64)
	for rows.Next() {
		var agentID, sessionID, manifestJSON string
		var guardVersion int64
		if err := rows.Scan(&agentID, &sessionID, &manifestJSON, &guardVersion); err != nil {
			return nil, fmt.Errorf("scan initial state session: %w", err)
		}
		agent, session := protocol.AgentID(agentID), protocol.OpaqueID(sessionID)
		var manifest protocol.CapabilityManifest
		if !agent.Valid() || !session.Valid() || json.Unmarshal([]byte(manifestJSON), &manifest) != nil ||
			manifest.Validate() != nil || manifest.AgentID != agent {
			continue
		}
		report := protocol.StateObservationReport{
			AgentID: agent, SessionID: session,
			ObservationID: initialStateObservationID(agent, session), ObservedAt: now,
			Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion,
			ObservationGeneration: 1, InputGuardVersion: guardVersion,
			ScreenState: protocol.StateObservationScreenWithheld, ScreenRows: []string{},
			Activity: protocol.StateObservationActivity{}, NativeEvents: []protocol.StateObservationEvent{},
		}
		if report.Validate() == nil {
			reports = append(reports, report)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read initial state sessions: %w", err)
	}
	return reports, nil
}

func initialStateObservationID(agent protocol.AgentID, session protocol.OpaqueID) protocol.OpaqueID {
	sum := sha256.Sum256([]byte(string(agent) + "\x00" + string(session)))
	return protocol.OpaqueID("state-initial-" + hex.EncodeToString(sum[:16]))
}

func (p *StateProducer) hasQueuedObservation(agent protocol.AgentID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, pending := p.pending[agent]
	_, inFlight := p.inFlight[agent]
	return pending || inFlight
}

func (p *StateProducer) dueAgents() []protocol.AgentID {
	now := p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	agents := make([]protocol.AgentID, 0, len(p.pending))
	for agent := range p.pending {
		if last := p.lastCommit[agent]; last.IsZero() || now.Sub(last) >= stateCommitInterval {
			agents = append(agents, agent)
		}
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i] < agents[j] })
	return agents
}

func (p *StateProducer) produceOne(ctx context.Context, agent protocol.AgentID) error {
	report, demand, ok := p.current(agent)
	if !ok {
		return nil
	}
	if report.ObservedAt.After(p.clock.Now()) {
		p.discardIfCurrent(report)
		return ErrObservationUnavailable
	}
	if !p.startInFlight(report) {
		return ErrObservationSuperseded
	}
	defer p.finishInFlight(report)
	evidence, err := p.prepare(ctx, report)
	if err != nil {
		p.discardIfCurrent(report)
		return err
	}
	selected, err := p.selectStateAnalysis(ctx, report, evidence, demand)
	if err != nil {
		return err
	}
	if !p.isCurrentOrEquivalent(report, evidence) {
		return ErrObservationSuperseded
	}
	result, err := p.commit(
		ctx, report, evidence, selected.analysis, selected.inferenceExpiresAt, true, false,
	)
	if result.verified.Route == analyst.RoutePTY && result.verified.CallRecord != nil &&
		!selected.inferenceExpiresAt.IsZero() {
		if selected.reused {
			p.bindCachedAnalysis(report.AgentID, result.stateID)
		} else {
			p.cacheAnalysis(
				report.AgentID, selected.key, report, evidence, result.verified,
				selected.inferenceExpiresAt, result.stateID,
				selected.profile, selected.generation,
			)
		}
	}
	if err != nil {
		return err
	}
	p.reconcileAndDiscard(report, result, selected.demandSatisfied, evidence)
	return nil
}

type uncachedStateAnalysis struct {
	analysis           analyst.Analysis
	inferenceExpiresAt time.Time
	profile            StateAnalysisProfile
	generation         int64
	key                [sha256.Size]byte
	demandSatisfied    bool
}

func (p *StateProducer) selectStateAnalysis(
	ctx context.Context,
	report protocol.StateObservationReport,
	evidence statePreparation,
	demand bool,
) (selectedStateAnalysis, error) {
	reduced := evidence.nativeHistory.Reduce(nil, report.ObservedAt)
	if native, veto := analyst.NativeVeto(report.AgentID, reduced); veto && report.ExitCode == nil {
		p.discardCachedAnalysis(report.AgentID)
		return selectedStateAnalysis{analysis: native, demandSatisfied: demand}, nil
	}
	return p.selectPTYStateAnalysis(ctx, report, evidence, demand)
}

func (p *StateProducer) selectPTYStateAnalysis(
	ctx context.Context,
	report protocol.StateObservationReport,
	evidence statePreparation,
	demand bool,
) (selectedStateAnalysis, error) {
	profile, generation := p.currentAnalysisProfile()
	if p.analyzer != nil && !profile.valid() {
		return selectedStateAnalysis{}, ErrObservationUnavailable
	}
	key, err := stateAnalysisCacheKeySelected(report, evidence, profile, generation)
	if err != nil {
		return selectedStateAnalysis{}, err
	}
	analysis, expiresAt, reused := p.cachedAnalysis(
		report.AgentID, key, report, evidence, p.clock.Now(), demand,
	)
	selected := selectedStateAnalysis{
		key: key, analysis: analysis, inferenceExpiresAt: expiresAt,
		profile: profile, generation: generation,
		reused: reused, demandSatisfied: demand && reused,
	}
	if reused {
		return selected, nil
	}
	fresh, err := p.analyzeUncached(
		ctx, report, evidence, demand, profile, generation, key,
	)
	if err != nil {
		return selectedStateAnalysis{}, err
	}
	selected.analysis = fresh.analysis
	selected.inferenceExpiresAt = fresh.inferenceExpiresAt
	selected.profile = fresh.profile
	selected.generation = fresh.generation
	selected.key = fresh.key
	selected.demandSatisfied = fresh.demandSatisfied
	return selected, nil
}

func (p *StateProducer) analyzeUncached(
	ctx context.Context,
	report protocol.StateObservationReport,
	evidence statePreparation,
	demand bool,
	profile StateAnalysisProfile,
	generation int64,
	key [sha256.Size]byte,
) (uncachedStateAnalysis, error) {
	result := uncachedStateAnalysis{
		analysis: unknownStateAnalysis(report.AgentID),
		profile:  profile, generation: generation, key: key,
	}
	if !demand || report.Activity.BytesLast500ms != 0 || p.analyzer == nil {
		return result, nil
	}
	result.demandSatisfied = true
	result.analysis, _ = p.analyzer.Analyze(ctx, evidence.observation(report))
	result.profile, result.generation = stateProfileFromRecord(result.analysis.CallRecord, profile)
	if !result.profile.valid() {
		result.analysis = unknownStateAnalysis(report.AgentID)
	} else {
		refreshed, err := stateAnalysisCacheKeySelected(
			report, evidence, result.profile, result.generation,
		)
		if err != nil {
			return result, err
		}
		result.key = refreshed
	}
	if result.analysis.Route == analyst.RoutePTY && result.analysis.CallRecord != nil {
		result.inferenceExpiresAt = report.ObservedAt.Add(stateTTL)
	}
	return result, nil
}

func (p *StateProducer) reconcileAndDiscard(
	report protocol.StateObservationReport,
	result stateCommitResult,
	demandSatisfied bool,
	native statePreparation,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if result.inserted {
		p.lastCommit[report.AgentID] = p.clock.Now()
	}
	current, exists := p.pending[report.AgentID]
	if !exists || !compatibleInferenceRefresh(report, current.report) ||
		!sameNativeInferenceState(native, report.ObservedAt, current.report.ObservedAt) {
		return
	}
	key := stateSessionKey{agent: current.report.AgentID, session: current.report.SessionID}
	reconciled := false
	if latest, ok := p.latest[key]; ok && latest == current.head && result.stateID.Valid() {
		p.reconciled[key] = reconciledStateEvidence{stateID: result.stateID, head: latest}
		reconciled = true
	}
	delete(p.pending, report.AgentID)
	if demandSatisfied && reconciled {
		delete(p.demanded, report.AgentID)
	}
}

func (p *StateProducer) startInFlight(report protocol.StateObservationReport) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.pending[report.AgentID]
	if !ok || !sameObservation(current.report, report) {
		return false
	}
	p.inFlight[report.AgentID] = pendingStateObservation{
		report: cloneObservation(report),
		head:   current.head,
	}
	return true
}

func (p *StateProducer) finishInFlight(report protocol.StateObservationReport) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.inFlight[report.AgentID]; ok && sameObservation(current.report, report) {
		delete(p.inFlight, report.AgentID)
	}
}

func (p *StateProducer) current(agent protocol.AgentID) (protocol.StateObservationReport, bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.pending[agent]
	return cloneObservation(entry.report), p.demanded[agent], ok
}

func (p *StateProducer) isCurrentOrEquivalent(
	report protocol.StateObservationReport,
	native statePreparation,
) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.pending[report.AgentID]
	return ok && compatibleInferenceRefresh(report, entry.report) &&
		sameNativeInferenceState(native, report.ObservedAt, entry.report.ObservedAt)
}

func (p *StateProducer) cachedAnalysis(
	agent protocol.AgentID,
	key [sha256.Size]byte,
	report protocol.StateObservationReport,
	prepared statePreparation,
	now time.Time,
	demand bool,
) (analyst.Analysis, time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cached, ok := p.analysis[agent]
	profile, generation := p.currentAnalysisProfile()
	if !ok || cached.profile != profile || cached.generation != generation || !now.Before(cached.expiresAt) {
		return analyst.Analysis{}, time.Time{}, false
	}
	// UNKNOWN remains useful audit evidence for ordinary timer refreshes, but
	// cannot consume an explicit request for fresh automatic-state evidence.
	if demand && cached.analysis.Phase == protocol.AgentPhaseUnknown {
		return analyst.Analysis{}, time.Time{}, false
	}
	if cached.key != key {
		sessionKey := stateSessionKey{agent: report.AgentID, session: report.SessionID}
		reconciled, reconciledOK := p.reconciled[sessionKey]
		previousCompatible := cached.stateID.Valid() && reconciledOK &&
			reconciled.stateID == cached.stateID
		if !previousCompatible {
			previousCompatible = samePreviousState(cached.prepared.previous, prepared.previous)
		}
		if !previousCompatible || !sameInferenceConfiguration(cached.prepared, prepared) ||
			!compatibleInferenceRefresh(cached.report, report) ||
			!sameNativeInferenceState(cached.prepared, cached.report.ObservedAt, report.ObservedAt) {
			return analyst.Analysis{}, time.Time{}, false
		}
	}
	return cloneStateAnalysis(cached.analysis), cached.expiresAt, true
}

func (p *StateProducer) cacheAnalysis(
	agent protocol.AgentID,
	key [sha256.Size]byte,
	report protocol.StateObservationReport,
	prepared statePreparation,
	analysis analyst.Analysis,
	expiresAt time.Time,
	stateID protocol.ULID,
	profile StateAnalysisProfile,
	generation int64,
) {
	p.mu.Lock()
	p.analysis[agent] = cachedStateAnalysis{
		key: key, report: cloneObservation(report), prepared: cloneStatePreparation(prepared),
		profile: profile, generation: generation, analysis: cloneStateAnalysis(analysis),
		expiresAt: expiresAt, stateID: stateID,
	}
	p.mu.Unlock()
}

func (p *StateProducer) bindCachedAnalysis(agent protocol.AgentID, stateID protocol.ULID) {
	if !stateID.Valid() {
		return
	}
	p.mu.Lock()
	cached, ok := p.analysis[agent]
	if ok {
		cached.stateID = stateID
		p.analysis[agent] = cached
	}
	p.mu.Unlock()
}

func (p *StateProducer) discardCachedAnalysis(agent protocol.AgentID) {
	p.mu.Lock()
	delete(p.analysis, agent)
	p.mu.Unlock()
}

// reconcileCompatibleRefreshLocked lets an unexpired durable row remain
// eligible while its timer-only successor waits in the queue. The successor
// is still produced so non-model snapshot context can be refreshed.
func (p *StateProducer) reconcileCompatibleRefreshLocked(
	key stateSessionKey,
	current pendingStateObservation,
) {
	cached, cachedOK := p.analysis[key.agent]
	reconciled, reconciledOK := p.reconciled[key]
	now := p.clock.Now()
	profile, generation := p.currentAnalysisProfile()
	if !cachedOK || !reconciledOK || cached.profile != profile || cached.generation != generation || !cached.stateID.Valid() ||
		reconciled.stateID != cached.stateID || !now.Before(cached.expiresAt) ||
		current.report.ObservedAt.After(now) ||
		!compatibleInferenceRefresh(cached.report, current.report) ||
		!sameNativeInferenceState(
			cached.prepared, cached.report.ObservedAt, current.report.ObservedAt,
		) {
		return
	}
	reconciled.head = current.head
	p.reconciled[key] = reconciled
}

func (p *StateProducer) discardIfCurrent(report protocol.StateObservationReport) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.pending[report.AgentID]; ok && sameObservation(current.report, report) {
		delete(p.pending, report.AgentID)
	}
}

func newerObservation(next, current protocol.StateObservationReport) bool {
	if next.SessionID != current.SessionID {
		return next.ObservedAt.After(current.ObservedAt)
	}
	return newerStateEvidenceHead(stateEvidenceHead{
		observedAt:            next.ObservedAt,
		observationGeneration: next.ObservationGeneration,
		outputGeneration:      next.OutputGeneration,
		eventGeneration:       next.EventGeneration,
		inputGuardVersion:     next.InputGuardVersion,
	}, stateEvidenceHead{
		observedAt:            current.ObservedAt,
		observationGeneration: current.ObservationGeneration,
		outputGeneration:      current.OutputGeneration,
		eventGeneration:       current.EventGeneration,
		inputGuardVersion:     current.InputGuardVersion,
	})
}

func sameObservation(a, b protocol.StateObservationReport) bool {
	return a.AgentID == b.AgentID && a.SessionID == b.SessionID &&
		a.ObservationID == b.ObservationID &&
		a.ObservationGeneration == b.ObservationGeneration &&
		a.OutputGeneration == b.OutputGeneration && a.EventGeneration == b.EventGeneration &&
		a.ObservedAt.Equal(b.ObservedAt)
}

func sameInferenceReportEvidence(a, b protocol.StateObservationReport) bool {
	a.ObservationID, b.ObservationID = "", ""
	a.ObservationGeneration, b.ObservationGeneration = 0, 0
	return reflect.DeepEqual(a, b)
}

// compatibleInferenceRefresh is the single equivalence rule for in-flight,
// queued and cached refreshes. It accepts only a newer transport clock and
// activity windows aging around the same output evidence. The cached report,
// exact ANALYZE key and inference expiry always retain the original payload.
func compatibleInferenceRefresh(
	inference protocol.StateObservationReport,
	current protocol.StateObservationReport,
) bool {
	if current.ObservedAt.Before(inference.ObservedAt) {
		return false
	}
	if !compatibleRefreshActivity(inference, current) {
		return false
	}
	inference.Activity = protocol.StateObservationActivity{}
	current.Activity = protocol.StateObservationActivity{}
	current.ObservedAt = inference.ObservedAt
	return sameInferenceReportEvidence(inference, current)
}

// sameNativeInferenceState prevents a timer-only refresh from carrying an
// analysis or decision across the instant native evidence withdraws its veto.
// The new PTY analysis may live longer, but it must begin after that change.
func sameNativeInferenceState(
	native statePreparation,
	before time.Time,
	after time.Time,
) bool {
	prior := native.nativeHistory.Reduce(nil, before)
	next := native.nativeHistory.Reduce(nil, after)
	return prior.Phase == next.Phase && prior.Blocks == next.Blocks &&
		prior.Confidence == next.Confidence && prior.Stale == next.Stale &&
		prior.ExpiresAt.Equal(next.ExpiresAt) && slices.Equal(prior.Evidence, next.Evidence)
}

// Durations cross a float-seconds boundary in the observation contract. A
// millisecond absorbs that rounding without treating a real output as quiet;
// output and event generations remain exact guards.
const stateRefreshLastOutputTolerance = time.Millisecond

func compatibleRefreshActivity(
	inference protocol.StateObservationReport,
	current protocol.StateObservationReport,
) bool {
	if inference.Activity == current.Activity {
		// Some sources refresh only the transport timestamp. This is the
		// original canary and carries no new activity evidence.
		return true
	}
	if current.Activity.BytesLast500ms > inference.Activity.BytesLast500ms ||
		current.Activity.BytesLast2s > inference.Activity.BytesLast2s ||
		current.Activity.BytesLast10s > inference.Activity.BytesLast10s {
		return false
	}
	inferenceLastOutput := inference.ObservedAt.Add(-time.Duration(
		inference.Activity.SecondsSinceLastOutput * float64(time.Second),
	))
	currentLastOutput := current.ObservedAt.Add(-time.Duration(
		current.Activity.SecondsSinceLastOutput * float64(time.Second),
	))
	delta := currentLastOutput.Sub(inferenceLastOutput)
	if delta < 0 {
		delta = -delta
	}
	return delta <= stateRefreshLastOutputTolerance
}

func stateAnalysisCacheKey(
	report protocol.StateObservationReport,
	prepared statePreparation,
	profile StateAnalysisProfile,
) ([sha256.Size]byte, error) {
	return stateAnalysisCacheKeySelected(report, prepared, profile, 0)
}

func stateAnalysisCacheKeySelected(
	report protocol.StateObservationReport,
	prepared statePreparation,
	profile StateAnalysisProfile,
	generation int64,
) ([sha256.Size]byte, error) {
	report.ObservationID = ""
	report.ObservationGeneration = 0
	encoded, err := json.Marshal(struct {
		Report              protocol.StateObservationReport
		SessionUpdatedAt    string
		ManifestSHA256      string
		Guard               *protocol.InputGuardReport
		Previous            *analyst.PreviousState
		Profile             StateAnalysisProfile
		SelectionGeneration int64
		PromptVersion       string
		AnalysisPolicy      string
	}{
		Report: report, SessionUpdatedAt: prepared.sessionUpdatedAt,
		ManifestSHA256: prepared.manifestSHA256, Guard: prepared.guard,
		Previous: prepared.previous,
		Profile:  profile, SelectionGeneration: generation, PromptVersion: analyst.PromptVersion,
		AnalysisPolicy: stateAnalysisPolicy,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("hash state analysis evidence: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func cloneStateAnalysis(in analyst.Analysis) analyst.Analysis {
	out := in
	out.Evidence = append([]string(nil), in.Evidence...)
	out.NativeEvidence = append([]protocol.ULID(nil), in.NativeEvidence...)
	if in.CallRecord != nil {
		record := *in.CallRecord
		record.Probabilities = maps.Clone(in.CallRecord.Probabilities)
		out.CallRecord = &record
	}
	return out
}

func cloneObservation(in protocol.StateObservationReport) protocol.StateObservationReport {
	out := in
	out.ScreenRows = append([]string(nil), in.ScreenRows...)
	out.NativeEvents = append([]protocol.StateObservationEvent(nil), in.NativeEvents...)
	if in.Cursor != nil {
		cursor := *in.Cursor
		out.Cursor = &cursor
	}
	if in.ExitCode != nil {
		exit := *in.ExitCode
		out.ExitCode = &exit
	}
	return out
}

type statePreparation struct {
	sessionState     protocol.SessionState
	sessionUpdatedAt string
	manifestSHA256   string
	guard            *protocol.InputGuardReport
	previous         *analyst.PreviousState
	nativeLineage    nativeStateLineage
	nativeHistory    *analyst.NativeHistory
}

func sameInferenceConfiguration(a, b statePreparation) bool {
	return a.sessionState == b.sessionState &&
		a.sessionUpdatedAt == b.sessionUpdatedAt &&
		a.manifestSHA256 == b.manifestSHA256 &&
		a.nativeLineage == b.nativeLineage &&
		sameGuard(a.guard, b.guard)
}

// samePreviousState is used only while a cached call is not yet attached to a
// row. Once attached, publishing that row is allowed to become Previous; it
// is the producer's own consequence, not external evidence.
func samePreviousState(a, b *analyst.PreviousState) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Phase == b.Phase && a.Since.Equal(b.Since)
}

func cloneStatePreparation(in statePreparation) statePreparation {
	out := in
	if in.guard != nil {
		guard := *in.guard
		out.guard = &guard
	}
	if in.previous != nil {
		previous := *in.previous
		out.previous = &previous
	}
	out.nativeHistory = in.nativeHistory.Clone()
	return out
}

func (p *StateProducer) prepare(ctx context.Context, report protocol.StateObservationReport) (statePreparation, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return statePreparation{}, fmt.Errorf("begin state preparation: %w", err)
	}
	defer rollback(tx)
	if duplicate, err := observationAlreadyStored(ctx, tx, report); err != nil {
		return statePreparation{}, err
	} else if duplicate {
		return statePreparation{}, ErrObservationUnavailable
	}
	prepared, err := p.loadStatePreparation(ctx, tx, report)
	if err != nil {
		return statePreparation{}, err
	}
	prepared.previous, err = loadPreviousPhase(ctx, tx, report)
	if err != nil {
		return statePreparation{}, err
	}
	return prepared, nil
}

func (p *StateProducer) loadStatePreparation(ctx context.Context, q nativeStateQueryer,
	report protocol.StateObservationReport,
) (statePreparation, error) {
	prepared, err := loadStateConfiguration(ctx, q, report)
	if err != nil {
		return statePreparation{}, err
	}
	prepared.nativeHistory, err = p.nativeCache.load(ctx, q, prepared.nativeLineage, report)
	if err != nil {
		return statePreparation{}, err
	}
	return prepared, nil
}

// loadStateConfiguration reads mutable commit guards and the immutable run
// lineage, but no native event rows. Commit calls it again to prove that the
// prepared cut still belongs to the same session; the reducer and high-water
// used for analysis are deliberately not reread.
func loadStateConfiguration(ctx context.Context, q nativeStateQueryer,
	report protocol.StateObservationReport,
) (statePreparation, error) {
	var state string
	var manifestJSON, processJSON, startedAtRaw, updatedAt, bootID, nodeID string
	err := q.QueryRowContext(ctx, `SELECT s.state, s.manifest_json,
		COALESCE(s.process_json, ''), s.started_at, s.updated_at,
		COALESCE(s.boot_id, ''), a.node_id
		FROM sessions s JOIN agents a ON a.agent_id = s.agent_id
		WHERE s.session_id = ? AND s.agent_id = ?`, report.SessionID, report.AgentID).
		Scan(&state, &manifestJSON, &processJSON, &startedAtRaw, &updatedAt, &bootID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return statePreparation{}, ErrObservationUnavailable
	}
	if err != nil {
		return statePreparation{}, fmt.Errorf("read observation session: %w", err)
	}
	sessionState := protocol.SessionState(state)
	if !sessionState.Valid() {
		return statePreparation{}, fmt.Errorf("read observation session: invalid state")
	}
	var manifest protocol.CapabilityManifest
	if json.Unmarshal([]byte(manifestJSON), &manifest) != nil || manifest.Validate() != nil ||
		manifest.AgentID != report.AgentID || manifest.Harness != report.Harness ||
		manifest.HarnessVersion != report.HarnessVersion {
		return statePreparation{}, ErrObservationUnavailable
	}
	startedAt, err := eventstore.ParseTime(startedAtRaw)
	if err != nil || startedAt.After(report.ObservedAt) || len(bootID) > 128 ||
		!protocol.NodeID(nodeID).Valid() {
		return statePreparation{}, ErrObservationUnavailable
	}
	processCanonical := []byte("null")
	if processJSON != "" {
		var process *protocol.ProcessIdentity
		if json.Unmarshal([]byte(processJSON), &process) != nil ||
			(process != nil && process.Validate() != nil) {
			return statePreparation{}, ErrObservationUnavailable
		}
		if process != nil {
			processCanonical, err = json.Marshal(process)
			if err != nil {
				return statePreparation{}, ErrObservationUnavailable
			}
		}
	}
	sum := sha256.Sum256([]byte(manifestJSON))
	prepared := statePreparation{
		sessionState: sessionState, sessionUpdatedAt: updatedAt,
		manifestSHA256: hex.EncodeToString(sum[:]),
		nativeLineage: nativeStateLineage{
			nodeID: protocol.NodeID(nodeID), sessionID: report.SessionID,
			agentID: report.AgentID, harness: report.Harness,
			harnessVersion: report.HarnessVersion, adapterType: manifest.AdapterType,
			bootID: bootID, startedAt: startedAt,
			processSHA256: sha256.Sum256(processCanonical),
		},
	}
	if sessionState == protocol.SessionStateObserved {
		if report.InputGuardVersion != 0 {
			return statePreparation{}, ErrObservationSuperseded
		}
		return prepared, nil
	}
	prepared.guard, err = loadObservationGuard(ctx, q, report)
	if err != nil {
		return statePreparation{}, err
	}
	return prepared, nil
}

func loadObservationGuard(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, report protocol.StateObservationReport,
) (*protocol.InputGuardReport, error) {
	var draft, activity, source string
	var lastKey sql.NullString
	var draftGeneration, version int64
	var confidence float64
	err := q.QueryRowContext(ctx, `SELECT draft_generation, draft_state,
		activity_state, last_human_key_at, detection_source, confidence, version
		FROM input_guards WHERE session_id = ? AND agent_id = ?`, report.SessionID, report.AgentID).
		Scan(&draftGeneration, &draft, &activity, &lastKey, &source, &confidence, &version)
	if errors.Is(err, sql.ErrNoRows) {
		if report.InputGuardVersion == 0 {
			return nil, nil
		}
		return nil, ErrObservationSuperseded
	}
	if err != nil {
		return nil, fmt.Errorf("read observation input guard: %w", err)
	}
	if version != report.InputGuardVersion {
		return nil, ErrObservationSuperseded
	}
	guard := &protocol.InputGuardReport{
		AgentID: report.AgentID, SessionID: report.SessionID,
		DraftState: protocol.DraftState(draft), ActivityState: protocol.ActivityState(activity),
		DraftGeneration: draftGeneration, InputGuardVersion: version,
		DetectionSource: protocol.InputGuardProvider(source), Confidence: confidence,
	}
	if lastKey.Valid {
		guard.LastHumanKeyAt, err = eventstore.ParseTime(lastKey.String)
		if err != nil {
			return nil, fmt.Errorf("parse observation human key time: %w", err)
		}
	}
	if guard.Validate() != nil {
		return nil, fmt.Errorf("read observation input guard: invalid row")
	}
	return guard, nil
}

func loadPreviousPhase(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, report protocol.StateObservationReport,
) (*analyst.PreviousState, error) {
	var phase, observed string
	err := q.QueryRowContext(ctx, `SELECT agent_phase, observed_at
		FROM state_snapshots WHERE agent_id = ? AND session_id = ?
		AND producer_version IS NOT NULL
		ORDER BY observed_at DESC, state_version DESC LIMIT 1`, report.AgentID, report.SessionID).
		Scan(&phase, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read previous state phase: %w", err)
	}
	since, err := eventstore.ParseTime(observed)
	if err != nil || !protocol.AgentPhase(phase).Valid() {
		return nil, fmt.Errorf("read previous state phase: invalid row")
	}
	return &analyst.PreviousState{Phase: protocol.AgentPhase(phase), Since: since}, nil
}

func (s statePreparation) observation(report protocol.StateObservationReport) analyst.Observation {
	obs := analyst.Observation{
		AgentID: report.AgentID, Harness: report.Harness,
		HarnessVersion: report.HarnessVersion, ObservedAt: report.ObservedAt,
		Activity: analyst.Activity{
			BytesLast500ms:         report.Activity.BytesLast500ms,
			BytesLast2s:            report.Activity.BytesLast2s,
			BytesLast10s:           report.Activity.BytesLast10s,
			SecondsSinceLastOutput: report.Activity.SecondsSinceLastOutput,
		},
		Previous: s.previous, Guard: s.guard, GuardVersion: report.InputGuardVersion,
	}
	if report.ScreenState == protocol.StateObservationScreenAvailable {
		obs.Screen = append([]string(nil), report.ScreenRows...)
		obs.Cols = int(report.Cols)
		obs.Cursor = screenproj.Cursor{Row: int(report.Cursor.Row), Known: true, Visible: true}
	}
	if report.ExitCode != nil {
		exit := int(*report.ExitCode)
		obs.ExitCode = &exit
	}
	obs.NativeEvents = []analyst.NativeEvent{}
	obs.NativePrefix = s.nativeHistory.Clone()
	return obs
}

func observationAlreadyStored(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, report protocol.StateObservationReport,
) (bool, error) {
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM state_snapshots
		WHERE observation_id = ? AND producer_version IS NOT NULL LIMIT 1`,
		report.ObservationID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read stored observation: %w", err)
	}
	return true, nil
}

type stateCommitResult struct {
	inserted bool
	stateID  protocol.ULID
	verified analyst.Analysis
}

func (p *StateProducer) commit(
	ctx context.Context,
	report protocol.StateObservationReport,
	prepared statePreparation,
	analysis analyst.Analysis,
	inferenceExpiresAt time.Time,
	requireCurrent bool,
	initialState bool,
) (stateCommitResult, error) {
	result := stateCommitResult{}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin state snapshot: %w", err)
	}
	defer rollback(tx)
	if duplicate, err := observationAlreadyStored(ctx, tx, report); err != nil {
		return result, err
	} else if duplicate {
		return result, nil
	}
	current, err := loadStateConfiguration(ctx, tx, report)
	if err != nil {
		return result, err
	}
	if current.sessionState != prepared.sessionState ||
		current.sessionUpdatedAt != prepared.sessionUpdatedAt ||
		current.manifestSHA256 != prepared.manifestSHA256 ||
		current.nativeLineage != prepared.nativeLineage ||
		!sameGuard(current.guard, prepared.guard) ||
		(requireCurrent && !p.isCurrentOrEquivalent(report, prepared)) {
		return result, ErrObservationSuperseded
	}
	analysis, err = verifyStateAnalysis(ctx, tx, report, prepared, analysis)
	if err != nil {
		return result, err
	}
	result.verified = analysis
	channelView, loops, approvals, err := p.channel.StateContextTx(ctx, tx, report.AgentID, report.ObservedAt)
	if err != nil {
		return result, fmt.Errorf("read state channel context: %w", err)
	}
	var stateVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(state_version), 0) + 1
		FROM state_snapshots WHERE agent_id = ? AND session_id = ?
		AND producer_version IS NOT NULL`, report.AgentID, report.SessionID).Scan(&stateVersion); err != nil {
		return result, fmt.Errorf("read state version: %w", err)
	}
	now := p.clock.Now().UTC()
	stateID, err := p.mintULID(now)
	if err != nil {
		return result, err
	}
	expiresAt := report.ObservedAt.Add(stateTTL)
	if analysis.Route == analyst.RouteNative {
		if !analysis.NativeExpiresAt.IsZero() {
			expiresAt = analysis.NativeExpiresAt
		} else {
			expiresAt = report.ObservedAt.Add(analyst.NativeOperationTTL)
		}
	} else if analysis.Route == analyst.RoutePTY && analysis.CallRecord != nil &&
		!inferenceExpiresAt.IsZero() && inferenceExpiresAt.Before(expiresAt) {
		expiresAt = inferenceExpiresAt
	}
	snapshot, model := p.normalise(
		report, current, channelView, loops, approvals, stateID, stateVersion,
		analysis, now, expiresAt,
	)
	persistencePlan := p.persistence.snapshotPlan()
	if persistencePlan.forceDegraded {
		snapshot.HealthState = protocol.HealthStateDegraded
	}
	if initialState {
		snapshot.HealthState = protocol.HealthStateDegraded
		snapshot.DeliverySafety = initialDeliverySafety(current.guard)
	}
	if err := snapshot.Validate(); err != nil {
		return result, fmt.Errorf("normalise state snapshot: %w", err)
	}
	equal, existingStateID, err := latestStateSemanticallyEqual(
		ctx, tx, report, current, snapshot, model, expiresAt,
	)
	if err != nil {
		return result, err
	}
	if equal && !persistencePlan.forceDegraded {
		result.stateID = existingStateID
		return result, nil
	}
	var previousCommit sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT committed_at FROM state_snapshots
		WHERE agent_id = ? AND producer_version IS NOT NULL
		ORDER BY committed_at DESC LIMIT 1`, report.AgentID).Scan(&previousCommit)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("read last state commit: %w", err)
	}
	if previousCommit.Valid {
		last, parseErr := eventstore.ParseTime(previousCommit.String)
		if parseErr != nil {
			return result, fmt.Errorf("parse last state commit: %w", parseErr)
		}
		if now.Sub(last) < stateCommitInterval {
			return result, errStateCommitRateLimited
		}
	}
	confidenceJSON, _ := json.Marshal(snapshot.Confidence)
	evidenceJSON, _ := json.Marshal(snapshot.EvidenceEventIDs)
	channelJSON, _ := json.Marshal(snapshot.ChannelEffective)
	loopsJSON, _ := json.Marshal(snapshot.ActiveLoops)
	var lastHumanKey any
	if !snapshot.LastHumanKeyAt.IsZero() {
		lastHumanKey = eventstore.FormatTime(snapshot.LastHumanKeyAt)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO state_snapshots (
		state_id, agent_id, observed_at, session_state, human_draft_state,
		human_activity_state, draft_generation, input_guard_version, agent_phase,
		attention_state, health_state, delivery_safety, confidence_json,
		evidence_json, analyst_version, stale, state_version, last_human_key_at,
		background_activity, channel_effective_json, active_loops_json,
		pending_approvals, session_id, session_updated_at, manifest_sha256,
		observation_id, observation_generation, output_generation, event_generation,
		committed_at, expires_at, producer_version, analysis_route, glimmer_call_id,
		inference_profile, base_model_revision, adapter_revision, adapter_sha256,
		profile_revision, profile_sha256, calibration_revision, calibration_sha256,
			calibration_temperature, idle_threshold, idle_probability, prompt_version,
			analysis_policy_version, selection_generation
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?
	)`,
		snapshot.StateID, snapshot.AgentID, eventstore.FormatTime(snapshot.ObservedAt),
		snapshot.SessionState, snapshot.HumanDraftState, snapshot.HumanActivityState,
		snapshot.DraftGeneration, snapshot.InputGuardVersion, snapshot.AgentPhase,
		snapshot.AttentionState, snapshot.HealthState, snapshot.DeliverySafety,
		string(confidenceJSON), string(evidenceJSON), snapshot.AnalystVersion, snapshot.Stale,
		snapshot.StateVersion, lastHumanKey, snapshot.BackgroundActivity,
		string(channelJSON), string(loopsJSON), snapshot.PendingApprovals,
		report.SessionID, current.sessionUpdatedAt, current.manifestSHA256,
		report.ObservationID, report.ObservationGeneration, report.OutputGeneration,
		report.EventGeneration, eventstore.FormatTime(now),
		eventstore.FormatTime(expiresAt),
		StateProducerVersion, model.route, model.callID, model.inferenceProfile,
		model.baseRevision, model.adapterRevision, model.adapterSHA256,
		model.profileRevision, model.profileSHA256, model.calibrationRevision,
		model.calibrationSHA256, model.temperature, model.threshold,
		model.idleProbability, model.promptVersion, model.policyVersion,
		model.selectionGeneration)
	if err != nil {
		return result, fmt.Errorf("insert state snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit state snapshot: %w", err)
	}
	p.persistence.snapshotCommitted(persistencePlan, snapshot.HealthState)
	result.inserted = true
	result.stateID = stateID
	return result, nil
}

func latestStateSemanticallyEqual(
	ctx context.Context,
	q stateQueryer,
	report protocol.StateObservationReport,
	prepared statePreparation,
	snapshot protocol.StateSnapshot,
	model snapshotModelEvidence,
	expiresAt time.Time,
) (bool, protocol.ULID, error) {
	latest, err := ReadLatestState(
		ctx, q, report.AgentID, report.SessionID, report.ObservedAt, nil,
	)
	if errors.Is(err, ErrStateObservationNotFound) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("compare latest state snapshot: %w", err)
	}
	previousSnapshot := latest.Snapshot
	previousSnapshot.StateID, snapshot.StateID = "", ""
	previousSnapshot.StateVersion, snapshot.StateVersion = 0, 0
	previousSnapshot.ObservedAt, snapshot.ObservedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(previousSnapshot, snapshot) ||
		!latest.ExpiresAt.Equal(expiresAt) ||
		latest.SessionID != report.SessionID ||
		latest.OutputGeneration != report.OutputGeneration ||
		latest.EventGeneration != report.EventGeneration ||
		latest.sessionUpdatedAt != prepared.sessionUpdatedAt ||
		latest.manifestSHA256 != prepared.manifestSHA256 ||
		latest.ProducerVersion != StateProducerVersion {
		return false, "", nil
	}
	equal := latest.AnalysisRoute == modelString(model.route) &&
		string(latest.GlimmerCallID) == modelString(model.callID) &&
		latest.SelectionGeneration == modelInt64(model.selectionGeneration) &&
		string(latest.InferenceProfile) == modelString(model.inferenceProfile) &&
		latest.BaseModelRevision == modelString(model.baseRevision) &&
		latest.AdapterRevision == modelString(model.adapterRevision) &&
		string(latest.AdapterSHA256) == modelString(model.adapterSHA256) &&
		latest.ProfileRevision == modelString(model.profileRevision) &&
		string(latest.ProfileSHA256) == modelString(model.profileSHA256) &&
		latest.CalibrationRevision == modelString(model.calibrationRevision) &&
		string(latest.CalibrationSHA256) == modelString(model.calibrationSHA256) &&
		latest.CalibrationTemperature == modelFloat(model.temperature) &&
		latest.IdleThreshold == modelFloat(model.threshold) &&
		latest.IdleProbability == modelFloat(model.idleProbability) &&
		latest.PromptVersion == modelString(model.promptVersion) &&
		latest.AnalysisPolicyVersion == modelString(model.policyVersion)
	if !equal {
		return false, "", nil
	}
	return true, latest.Snapshot.StateID, nil
}

func modelString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func modelFloat(value any) float64 {
	if value == nil {
		return 0
	}
	number, _ := value.(float64)
	return number
}

func modelInt64(value any) int64 {
	if value == nil {
		return 0
	}
	number, _ := value.(int64)
	return number
}

func verifyStateAnalysis(
	ctx context.Context,
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	report protocol.StateObservationReport,
	native statePreparation,
	analysis analyst.Analysis,
) (analyst.Analysis, error) {
	if analysis.AgentID != report.AgentID || !analysis.Phase.Valid() ||
		math.IsNaN(analysis.Confidence) || math.IsInf(analysis.Confidence, 0) ||
		analysis.Confidence < 0 || analysis.Confidence > 1 {
		return unknownStateAnalysis(report.AgentID), nil
	}
	reduced := native.nativeHistory.Reduce(nil, report.ObservedAt)
	switch analysis.Route {
	case analyst.RouteNative:
		return verifyNativeStateAnalysis(report, native, analysis)
	case analyst.RoutePTY:
		if reduced.Blocks {
			return unknownStateAnalysis(report.AgentID), nil
		}
		nativeUnknown := reduced.Phase == protocol.AgentPhaseUnknown
		if (nativeUnknown && !slices.Equal(analysis.NativeEvidence, reduced.Evidence)) ||
			(!nativeUnknown && len(analysis.NativeEvidence) != 0) {
			return unknownStateAnalysisWithNative(report.AgentID, reduced), nil
		}
		verified, err := verifyPTYStateAnalysis(ctx, q, report, analysis)
		if nativeUnknown {
			verified.NativeEvidence = append([]protocol.ULID(nil), reduced.Evidence...)
		}
		return verified, err
	default:
		return unknownStateAnalysisWithNative(report.AgentID, reduced), nil
	}
}

func unknownStateAnalysis(agent protocol.AgentID) analyst.Analysis {
	return analyst.Analysis{
		AgentID: agent, Phase: protocol.AgentPhaseUnknown, PreviousStale: true,
	}
}

func unknownStateAnalysisWithNative(
	agent protocol.AgentID,
	reduced analyst.NativeState,
) analyst.Analysis {
	unknown := unknownStateAnalysis(agent)
	if reduced.Phase == protocol.AgentPhaseUnknown {
		unknown.NativeEvidence = append([]protocol.ULID(nil), reduced.Evidence...)
	}
	return unknown
}

func verifyNativeStateAnalysis(
	report protocol.StateObservationReport,
	native statePreparation,
	analysis analyst.Analysis,
) (analyst.Analysis, error) {
	unknown := unknownStateAnalysis(report.AgentID)
	if report.ExitCode != nil && analysis.Phase == protocol.AgentPhaseErrorOrExited {
		if analysis.Confidence != 1 || len(analysis.NativeEvidence) != 0 ||
			!analysis.NativeExpiresAt.IsZero() {
			return unknown, nil
		}
		return analysis, nil
	}
	reduced := native.nativeHistory.Reduce(nil, report.ObservedAt)
	if !reduced.Blocks || analysis.Phase != reduced.Phase ||
		analysis.Confidence != reduced.Confidence || analysis.PreviousStale != reduced.Stale ||
		!analysis.NativeExpiresAt.Equal(reduced.ExpiresAt) ||
		!slices.Equal(analysis.NativeEvidence, reduced.Evidence) {
		return unknown, nil
	}
	return analysis, nil
}

func verifyPTYStateAnalysis(
	ctx context.Context,
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	report protocol.StateObservationReport,
	analysis analyst.Analysis,
) (analyst.Analysis, error) {
	unknown := unknownStateAnalysis(report.AgentID)
	if analysis.CallRecord == nil {
		return unknown, nil
	}
	record := analysis.CallRecord
	recordValid := record.Validate() == nil
	if !recordValid || record.Contract != protocol.ModelContractAnalyzeAgentState ||
		record.AgentID != report.AgentID {
		return unknown, nil
	}
	var contract, agentID, profile, baseRepo, baseRevision, adapterName, adapterRevision, adapterSHA string
	var datasetVersion, datasetSHA, trainingRunID, trainingCodeRevision string
	var evaluationRunID, evaluationSHA, projectionVersion, projectionSHA string
	var profileRevision, profileSHA, calibrationRevision, calibrationSHA string
	var analysisPolicy, runtime, quantization, status string
	var selectionGeneration sql.NullInt64
	var promptVersion sql.NullString
	var temperature, threshold sql.NullFloat64
	var probabilitiesJSON sql.NullString
	err := q.QueryRowContext(ctx, `SELECT contract, COALESCE(agent_id, ''),
		inference_profile, base_model_repo, base_model_revision,
		COALESCE(adapter_name, ''), COALESCE(adapter_revision, ''),
		COALESCE(adapter_sha256, ''), COALESCE(dataset_version, ''),
		COALESCE(dataset_sha256, ''), COALESCE(training_run_id, ''),
		COALESCE(training_code_revision, ''), selection_generation,
		COALESCE(evaluation_run_id, ''), COALESCE(evaluation_sha256, ''),
		prompt_version, COALESCE(projection_policy_version, ''),
		COALESCE(projection_policy_sha256, ''), COALESCE(profile_revision, ''),
		COALESCE(profile_sha256, ''), COALESCE(calibration_revision, ''),
		COALESCE(calibration_sha256, ''), calibration_temperature, idle_threshold,
		COALESCE(analysis_policy_version, ''), runtime, COALESCE(quantization, ''),
		probabilities_json, status
		FROM glimmer_calls WHERE call_id = ?`, record.CallID).Scan(
		&contract, &agentID, &profile, &baseRepo, &baseRevision, &adapterName,
		&adapterRevision, &adapterSHA, &datasetVersion, &datasetSHA, &trainingRunID,
		&trainingCodeRevision, &selectionGeneration, &evaluationRunID, &evaluationSHA,
		&promptVersion, &projectionVersion, &projectionSHA, &profileRevision, &profileSHA,
		&calibrationRevision, &calibrationSHA, &temperature, &threshold, &analysisPolicy,
		&runtime, &quantization, &probabilitiesJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return unknown, nil
	}
	if err != nil {
		return analyst.Analysis{}, fmt.Errorf("verify state analysis call: %w", err)
	}
	durableProbabilities, probabilitiesValid := decodeStateProbabilities(probabilitiesJSON)
	if !probabilitiesValid {
		return unknown, nil
	}
	if contract != string(record.Contract) || agentID != string(record.AgentID) ||
		profile != string(record.InferenceProfile) || baseRepo != record.BaseModelRepo ||
		baseRevision != record.BaseModelRevision || adapterName != record.AdapterName ||
		adapterRevision != record.AdapterRevision || adapterSHA != string(record.AdapterSHA256) ||
		datasetVersion != record.DatasetVersion || datasetSHA != string(record.DatasetSHA256) ||
		trainingRunID != string(record.TrainingRunID) || trainingCodeRevision != record.TrainingCodeRevision ||
		!storedPositiveInt64Matches(selectionGeneration, record.SelectionGeneration) ||
		evaluationRunID != string(record.EvaluationRunID) || evaluationSHA != string(record.EvaluationSHA256) ||
		projectionVersion != record.ProjectionPolicyVersion || projectionSHA != string(record.ProjectionPolicySHA256) ||
		profileRevision != record.ProfileRevision || profileSHA != string(record.ProfileSHA256) ||
		calibrationRevision != record.CalibrationRevision || calibrationSHA != string(record.CalibrationSHA256) ||
		!storedPositiveFloatMatches(temperature, record.CalibrationTemperature) ||
		!storedPositiveFloatMatches(threshold, record.IdleThreshold) ||
		analysisPolicy != record.AnalysisPolicyVersion || runtime != record.Runtime ||
		quantization != record.Quantization ||
		status != string(record.Status) || !maps.Equal(durableProbabilities, record.Probabilities) {
		return unknown, nil
	}
	if analysis.PromptVersion == "" || !promptVersion.Valid ||
		analysis.PromptVersion != promptVersion.String ||
		(record.PromptVersion != "" && promptVersion.String != record.PromptVersion) {
		return unknown, nil
	}
	if record.Status != protocol.GlimmerCallStatusOK {
		analysis.Phase = protocol.AgentPhaseUnknown
		analysis.Confidence = 0
		analysis.PreviousStale = true
		return analysis, nil
	}
	confidence, ok := record.Probabilities[string(analysis.Phase)]
	if !ok || confidence != analysis.Confidence {
		analysis.Phase = protocol.AgentPhaseUnknown
		analysis.Confidence = 0
		analysis.PreviousStale = true
	}
	return analysis, nil
}

func storedPositiveInt64Matches(stored sql.NullInt64, value int64) bool {
	return stored.Valid == (value > 0) && (!stored.Valid || stored.Int64 == value)
}

func storedPositiveFloatMatches(stored sql.NullFloat64, value float64) bool {
	return stored.Valid == (value > 0) && (!stored.Valid || stored.Float64 == value)
}

func decodeStateProbabilities(value sql.NullString) (map[string]float64, bool) {
	probabilities := map[string]float64{}
	if !value.Valid {
		return probabilities, true
	}
	if json.Unmarshal([]byte(value.String), &probabilities) != nil {
		return nil, false
	}
	return probabilities, true
}

func sameGuard(a, b *protocol.InputGuardReport) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

type snapshotModelEvidence struct {
	route, callID, inferenceProfile, baseRevision any
	selectionGeneration                           any
	adapterRevision, adapterSHA256                any
	profileRevision, profileSHA256                any
	calibrationRevision, calibrationSHA256        any
	temperature, threshold, idleProbability       any
	promptVersion, policyVersion                  any
}

func (p *StateProducer) normalise(
	report protocol.StateObservationReport,
	prepared statePreparation,
	view protocol.AgentChannel,
	loops []protocol.LoopSummary,
	approvals int64,
	stateID protocol.ULID,
	stateVersion int64,
	analysis analyst.Analysis,
	now time.Time,
	expiresAt time.Time,
) (protocol.StateSnapshot, snapshotModelEvidence) {
	draft, activity := protocol.DraftStateUnknown, protocol.ActivityStateUnknown
	var draftGeneration, guardVersion int64
	var lastKey time.Time
	humanConfidence := float64(0)
	if prepared.guard != nil {
		draft, activity = prepared.guard.DraftState, prepared.guard.ActivityState
		draftGeneration, guardVersion = prepared.guard.DraftGeneration, prepared.guard.InputGuardVersion
		lastKey, humanConfidence = prepared.guard.LastHumanKeyAt, prepared.guard.Confidence
	}
	phase := analysis.Phase
	if !phase.Valid() {
		phase = protocol.AgentPhaseUnknown
	}
	attention := protocol.AttentionStateNone
	switch phase {
	case protocol.AgentPhaseWaitingApproval:
		attention = protocol.AttentionStatePermissionRequired
	case protocol.AgentPhaseAskingQuestion:
		attention = protocol.AttentionStateQuestionPending
	default:
	}
	health := healthForSession(prepared.sessionState)
	idleProbability, model, idleThreshold := p.modelEvidence(analysis)
	stale := !expiresAt.After(now) || phase == protocol.AgentPhaseUnknown || analysis.PreviousStale ||
		(prepared.sessionState != protocol.SessionStateObserved && prepared.guard == nil)
	safety := protocol.DeliverySafetyUncertain
	if draft != protocol.DraftStateClosed || activity == protocol.ActivityStateActive {
		safety = protocol.DeliverySafetyUnsafe
	} else if !stale && prepared.sessionState == protocol.SessionStateConnected && view.Deliverable &&
		analysis.Route == analyst.RoutePTY && phase == protocol.AgentPhaseIdleAtPrompt &&
		idleProbability != nil && *idleProbability >= idleThreshold &&
		analysis.CallRecord != nil && analysis.CallRecord.Status == protocol.GlimmerCallStatusOK {
		safety = protocol.DeliverySafetySafe
	}
	evidenceIDs := append([]protocol.ULID{}, analysis.NativeEvidence...)
	analystVersion := StateProducerVersion
	if analysis.Route == analyst.RouteNative {
		analystVersion = stateNativeAnalyst
	} else if analysis.Route == analyst.RoutePTY && analysis.CallRecord != nil {
		analystVersion = analysis.PromptVersion
	}
	return protocol.StateSnapshot{
		StateID: stateID, StateVersion: stateVersion, AgentID: report.AgentID,
		ObservedAt: report.ObservedAt.UTC(), SessionState: prepared.sessionState,
		HumanDraftState: draft, HumanActivityState: activity,
		DraftGeneration: draftGeneration, InputGuardVersion: guardVersion,
		LastHumanKeyAt: lastKey, AgentPhase: phase, AttentionState: attention,
		HealthState: health, DeliverySafety: safety,
		BackgroundActivity: phase == protocol.AgentPhaseBackgroundWork,
		ChannelEffective:   view.Effective, ActiveLoops: loops, PendingApprovals: approvals,
		Confidence: protocol.Confidence{
			HumanDraftState: humanConfidence, AgentPhase: analysis.Confidence,
			AttentionState: analysis.Confidence, DeliverySafety: valueOrZero(idleProbability),
		},
		EvidenceEventIDs: evidenceIDs, AnalystVersion: analystVersion,
		Stale: stale,
	}, model
}

func healthForSession(state protocol.SessionState) protocol.HealthState {
	switch state {
	case protocol.SessionStateConnected, protocol.SessionStateObserved:
		return protocol.HealthStateHealthy
	case protocol.SessionStateStarting:
		return protocol.HealthStateDegraded
	case protocol.SessionStateTerminated:
		return protocol.HealthStateCrashed
	default:
		return protocol.HealthStateDisconnected
	}
}

func initialDeliverySafety(guard *protocol.InputGuardReport) protocol.DeliverySafety {
	if guard != nil {
		switch guard.DraftState {
		case protocol.DraftStateOpen, protocol.DraftStatePasting, protocol.DraftStateSubmitting:
			return protocol.DeliverySafetyUnsafe
		default:
		}
		if guard.ActivityState == protocol.ActivityStateActive {
			return protocol.DeliverySafetyUnsafe
		}
	}
	return protocol.DeliverySafetyUncertain
}

func (p *StateProducer) modelEvidence(analysis analyst.Analysis) (*float64, snapshotModelEvidence, float64) {
	model := snapshotModelEvidence{}
	profile, _ := p.currentAnalysisProfile()
	if analysis.Route == analyst.RouteNative {
		model.route = string(analyst.RouteNative)
		return nil, model, profile.IdleThreshold
	}
	if analysis.Route != analyst.RoutePTY || analysis.CallRecord == nil {
		return nil, model, profile.IdleThreshold
	}
	rec := analysis.CallRecord
	profile, generation := stateProfileFromRecord(rec, profile)
	model.route = string(analyst.RoutePTY)
	model.callID = string(rec.CallID)
	model.inferenceProfile = string(rec.InferenceProfile)
	model.baseRevision = rec.BaseModelRevision
	model.selectionGeneration = nullablePositive(generation)
	model.adapterRevision = nullableString(rec.AdapterRevision)
	model.adapterSHA256 = nullableString(string(rec.AdapterSHA256))
	model.profileRevision = profile.Revision
	model.profileSHA256 = string(profile.SHA256)
	model.calibrationRevision = profile.CalibrationRevision
	model.calibrationSHA256 = string(profile.CalibrationSHA256)
	model.temperature = profile.Temperature
	model.threshold = profile.IdleThreshold
	model.promptVersion = nullableString(analysis.PromptVersion)
	model.policyVersion = stateAnalysisPolicy
	if rec.Status != protocol.GlimmerCallStatusOK {
		return nil, model, profile.IdleThreshold
	}
	idle, ok := calibratedProbability(rec.Probabilities, string(protocol.AgentPhaseIdleAtPrompt), profile.Temperature)
	if !ok {
		return nil, model, profile.IdleThreshold
	}
	model.idleProbability = idle
	return &idle, model, profile.IdleThreshold
}

func calibratedProbability(probabilities map[string]float64, label string, temperature float64) (float64, bool) {
	if temperature <= 0 || len(probabilities) == 0 {
		return 0, false
	}
	total, selected := float64(0), float64(0)
	for name, probability := range probabilities {
		if probability < 0 || probability > 1 || math.IsNaN(probability) || math.IsInf(probability, 0) {
			return 0, false
		}
		scaled := math.Pow(probability, 1/temperature)
		total += scaled
		if name == label {
			selected = scaled
		}
	}
	if total == 0 {
		return 0, false
	}
	return selected / total, true
}

func valueOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullablePositive(value int64) any {
	if value < 1 {
		return nil
	}
	return value
}

func (p *StateProducer) mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("mint state id: time outside range")
	}
	p.idMu.Lock()
	defer p.idMu.Unlock()
	var entropy [10]byte
	if _, err := io.ReadFull(p.entropy, entropy[:]); err != nil {
		return "", fmt.Errorf("mint state id: %w", err)
	}
	if millis == p.lastMillis && bytes.Compare(entropy[:], p.lastEntropy[:]) <= 0 {
		entropy = p.lastEntropy
		for index := len(entropy) - 1; index >= 0; index-- {
			entropy[index]++
			if entropy[index] != 0 {
				break
			}
		}
	}
	p.lastMillis, p.lastEntropy = millis, entropy
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
	for i := range len(encoded) {
		char := encoded[i]
		index := int(char - '0')
		if char >= 'a' {
			index = int(char-'a') + 10
		}
		out.WriteByte(digits[index])
	}
	return protocol.ULID(out.String()), nil
}
