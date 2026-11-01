// Package modelprofile owns the durable ANALYZE_AGENT_STATE model selection.
// Registry imports are immutable and every selection change is committed with
// its control event before it is published to readers.
package modelprofile

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
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	evaluationSplitTest    = "test"
	gateStatusFailed       = "failed"
	gateStatusPassed       = "passed"
	trainingStatusComplete = "completed"
	trainingStatusRunning  = "running"
	trainingUnderpowered   = "underpowered"
)

var (
	// ErrAdapterNotRegistered means no sealed metadata exists for the requested
	// adapter revision.
	ErrAdapterNotRegistered = errors.New("model adapter is not registered")
	// ErrRegistryInvalid means stored or imported evidence is inconsistent,
	// incomplete, mutable, or has not passed every required gate.
	ErrRegistryInvalid = errors.New("model registry evidence is invalid")
	// ErrProfileNotServed means the candidate could not be confirmed by the
	// serving endpoint. The current selection remains unchanged.
	ErrProfileNotServed = errors.New("model profile was not confirmed served")
	// ErrPipelineNotActivatable means verified pipeline evidence was retained,
	// but its computed gate, readiness, or training state cannot be registered.
	ErrPipelineNotActivatable = errors.New("model pipeline evidence is not activatable")
)

// EventStore is the only durable event write path used by this package.
type EventStore interface {
	NextEventSequenceTx(context.Context, *sql.Tx, protocol.OpaqueID) (int64, error)
	AppendEventsTx(context.Context, *sql.Tx, []protocol.EventEnvelope) error
}

// Publisher atomically exposes a committed selection.
type Publisher interface {
	CurrentModelSelection() protocol.ModelSelection
	Publish(protocol.ModelSelection) error
}

// Verifier confirms a complete candidate tuple at the serving boundary.
type Verifier interface {
	VerifyModelSelection(context.Context, protocol.ModelSelection) error
}

// SelectionValidator checks compatibility with the binary's embedded prompt
// and the configured projection/runtime policies.
type SelectionValidator func(protocol.ModelSelection) error

// Options configures the registry writer and selector.
type Options struct {
	DB        *sql.DB
	Events    EventStore
	NodeID    protocol.NodeID
	Clock     clock.Clock
	Base      protocol.ModelSelection
	Publisher Publisher
	Validate  SelectionValidator
	Verifier  Verifier
}

// Service serializes registry imports and model selection changes.
type Service struct {
	db        *sql.DB
	events    EventStore
	nodeID    protocol.NodeID
	clock     clock.Clock
	base      protocol.ModelSelection
	publisher Publisher
	validate  SelectionValidator

	mu       sync.Mutex
	verifyMu sync.RWMutex
	verifier Verifier
}

// Initialize loads a trustworthy durable selection. With no row it persists a
// known base generation. A malformed or unbound adapter row is replaced by a
// DEGRADED base generation; it is never trusted merely because active=1.
func Initialize(ctx context.Context, opts Options) (protocol.ModelSelection, error) {
	if err := validateOptions(opts, false); err != nil {
		return protocol.ModelSelection{}, err
	}
	var generation int64
	var raw string
	err := opts.DB.QueryRowContext(ctx, `SELECT generation, selection_json
		FROM model_profile_selections WHERE contract = ?`,
		protocol.ModelContractAnalyzeAgentState).Scan(&generation, &raw)
	if err == nil {
		var selected protocol.ModelSelection
		if json.Unmarshal([]byte(raw), &selected) == nil && selected.Generation == generation &&
			selected.Validate() == nil && compatible(opts.Validate, selected) == nil &&
			selectionBound(ctx, opts.DB, selected) == nil {
			return selected, nil
		}
		return persistBase(ctx, opts, generation+1, "startup selection validation failed", "DEGRADED")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return protocol.ModelSelection{}, fmt.Errorf("load model selection: %w", err)
	}
	return persistBase(ctx, opts, 1, "startup base selection", "READY")
}

// New returns the serialized mutation service after Initialize and selector
// construction.
func New(opts Options) (*Service, error) {
	if err := validateOptions(opts, true); err != nil {
		return nil, err
	}
	current := opts.Publisher.CurrentModelSelection()
	if current.Validate() != nil {
		return nil, fmt.Errorf("new model profile service: invalid current selection")
	}
	return &Service{
		db: opts.DB, events: opts.Events, nodeID: opts.NodeID, clock: opts.Clock,
		base: opts.Base, publisher: opts.Publisher, validate: opts.Validate,
		verifier: opts.Verifier,
	}, nil
}

func validateOptions(opts Options, publisher bool) error {
	if opts.DB == nil || opts.Events == nil || opts.Clock == nil || !opts.NodeID.Valid() ||
		(publisher && opts.Publisher == nil) {
		return fmt.Errorf("invalid model profile options")
	}
	probe := opts.Base
	probe.Generation = 1
	probe.GateStatus = "base"
	probe.OperationalState = "READY"
	probe.Reason = "base validation"
	probe.SelectedAt = opts.Clock.Now().UTC()
	probe.EventID = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if probe.Validate() != nil || compatible(opts.Validate, probe) != nil {
		return fmt.Errorf("invalid model profile base")
	}
	return nil
}

// SetVerifier installs the serving verifier before the control socket starts.
func (s *Service) SetVerifier(verifier Verifier) {
	s.verifyMu.Lock()
	s.verifier = verifier
	s.verifyMu.Unlock()
}

// Register imports or confirms one immutable metadata binding.
func (s *Service) Register(
	ctx context.Context,
	req protocol.RegisterModelMetadataRequest,
	_ channel.Actor,
) (protocol.ModelMetadataRegistrationResult, error) {
	report, err := validateRegistration(req)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("begin model metadata import: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := s.registerTx(ctx, tx, req, report)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("commit model metadata: %w", err)
	}
	return result, nil
}

// registerTx writes one already-validated, activatable binding. Pipeline
// imports use this same writer after computing and persisting their evidence.
func (s *Service) registerTx(
	ctx context.Context,
	tx *sql.Tx,
	req protocol.RegisterModelMetadataRequest,
	report eval.Report,
) (protocol.ModelMetadataRegistrationResult, error) {
	if err := validatePipelineEvidence(ctx, tx, req, report); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	canonical, err := json.Marshal(req)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("encode model metadata: %w", err)
	}
	gate, err := json.Marshal(req.Gate)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("encode model gate: %w", err)
	}
	var existing, registered string
	err = tx.QueryRowContext(ctx, `SELECT metadata_json, registered_at
		FROM model_profile_registry WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision).Scan(&existing, &registered)
	if err == nil {
		if !bytes.Equal([]byte(existing), canonical) {
			return protocol.ModelMetadataRegistrationResult{}, ErrRegistryInvalid
		}
		at, parseErr := eventstore.ParseTime(registered)
		if parseErr != nil {
			return protocol.ModelMetadataRegistrationResult{}, ErrRegistryInvalid
		}
		if err := bindAdapter(ctx, tx, req); err != nil {
			return protocol.ModelMetadataRegistrationResult{}, err
		}
		return protocol.ModelMetadataRegistrationResult{
			AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
			RegisteredAt: at,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("read model metadata: %w", err)
	}
	now := s.clock.Now().UTC()
	_, err = tx.ExecContext(ctx, `INSERT INTO model_profile_registry (
		adapter_name, adapter_revision, adapter_sha256, contract, base_model_repo,
		base_model_revision, runtime, quantization, ml_contract_version,
		dataset_version, dataset_sha256, dataset_readiness_status,
		dataset_readiness_sha256, training_run_id, training_code_revision,
		training_readiness_status, training_status, evaluation_run_id,
		evaluation_sha256, evaluation_report_json, gate_verdict_json, prompt_version,
		projection_policy_version, projection_policy_sha256, profile_revision,
		profile_sha256, calibration_revision, calibration_sha256,
		calibration_temperature, idle_threshold, analysis_policy_version,
		metadata_json, registered_at
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
	)`,
		req.AdapterName, req.AdapterRevision, req.AdapterSHA256, req.Contract,
		req.BaseModelRepo, req.BaseModelRevision, req.Runtime, nullString(req.Quantization),
		req.MLContractVersion, req.DatasetVersion, req.DatasetSHA256,
		req.DatasetReadinessStatus, req.DatasetReadinessSHA256, req.TrainingRunID,
		req.TrainingCodeRevision, req.TrainingReadinessStatus, req.TrainingStatus,
		req.EvaluationRunID, req.EvaluationSHA256, string(req.EvaluationReport), string(gate),
		req.PromptVersion, req.ProjectionPolicyVersion, req.ProjectionPolicySHA256,
		req.ProfileRevision, req.ProfileSHA256, req.CalibrationRevision,
		req.CalibrationSHA256, req.CalibrationTemperature, req.IdleThreshold,
		req.AnalysisPolicyVersion, string(canonical), eventstore.FormatTime(now))
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("insert model metadata: %w", err)
	}
	if err := bindAdapter(ctx, tx, req); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	return protocol.ModelMetadataRegistrationResult{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
		Imported: true, RegisteredAt: now,
	}, nil
}

// Activate verifies and atomically selects a sealed adapter revision.
func (s *Service) Activate(
	ctx context.Context,
	req protocol.ActivateAdapterRequest,
	actor channel.Actor,
) (protocol.ModelActivationResult, error) {
	if err := req.Validate(); err != nil {
		return protocol.ModelActivationResult{}, fmt.Errorf("validate activation request: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.publisher.CurrentModelSelection()
	if current.Profile.InferenceProfile == protocol.InferenceProfileAnalystLoRA &&
		current.Profile.AdapterName == req.AdapterName && current.Profile.AdapterRevision == req.AdapterRevision {
		return protocol.ModelActivationResult{Selection: current}, nil
	}
	metadata, err := readRegistration(ctx, s.db, req.AdapterName, req.AdapterRevision)
	if err != nil {
		return protocol.ModelActivationResult{}, err
	}
	next, err := s.adapterSelection(metadata, current.Generation+1, "operator activation", "READY")
	if err != nil {
		return protocol.ModelActivationResult{}, err
	}
	s.verifyMu.RLock()
	verifier := s.verifier
	s.verifyMu.RUnlock()
	if verifier == nil || verifier.VerifyModelSelection(ctx, next) != nil {
		return protocol.ModelActivationResult{}, ErrProfileNotServed
	}
	if err := s.persistChange(ctx, current, next, actor); err != nil {
		return protocol.ModelActivationResult{}, err
	}
	return protocol.ModelActivationResult{Applied: true, Selection: next}, nil
}

// Rollback selects the complete base tuple immediately and durably.
func (s *Service) Rollback(
	ctx context.Context,
	req protocol.RollbackToBaseRequest,
	actor channel.Actor,
) (protocol.ModelActivationResult, error) {
	if err := req.Validate(); err != nil || req.Contract != protocol.ModelContractAnalyzeAgentState {
		if err != nil {
			return protocol.ModelActivationResult{}, fmt.Errorf("validate rollback request: %w", err)
		}
		return protocol.ModelActivationResult{}, ErrRegistryInvalid
	}
	return s.rollback(ctx, 0, "operator rollback", "READY", actor)
}

// FallbackToBase is the registered automatic fallback for a serving or adapter
// breaker failure. It changes state only while the failed call's captured
// generation is still selected, and never reactivates an adapter automatically.
func (s *Service) FallbackToBase(ctx context.Context, generation int64, reason string) error {
	_, err := s.rollback(ctx, generation, reason, "DEGRADED", channel.Actor{Type: protocol.ActorTypeSystem, ID: "model-gateway"})
	return err
}

func (s *Service) rollback(
	ctx context.Context,
	expectedGeneration int64,
	reason string,
	state string,
	actor channel.Actor,
) (protocol.ModelActivationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.publisher.CurrentModelSelection()
	if expectedGeneration > 0 && current.Generation != expectedGeneration {
		return protocol.ModelActivationResult{Selection: current}, nil
	}
	if current.Profile.InferenceProfile == protocol.InferenceProfileBase {
		return protocol.ModelActivationResult{Selection: current}, nil
	}
	next, err := s.baseSelection(current.Generation+1, reason, state)
	if err != nil {
		return protocol.ModelActivationResult{}, err
	}
	if err := s.persistChange(ctx, current, next, actor); err != nil {
		return protocol.ModelActivationResult{}, err
	}
	return protocol.ModelActivationResult{Applied: true, Selection: next}, nil
}

func (s *Service) adapterSelection(
	metadata protocol.RegisterModelMetadataRequest,
	generation int64,
	reason, state string,
) (protocol.ModelSelection, error) {
	id, err := mintULID(s.clock.Now().UTC())
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	next := protocol.ModelSelection{
		Generation: generation,
		Profile: protocol.GatewayProfile{
			Contract:         protocol.ModelContractAnalyzeAgentState,
			InferenceProfile: protocol.InferenceProfileAnalystLoRA,
			AdapterName:      metadata.AdapterName, AdapterRevision: metadata.AdapterRevision,
		},
		AdapterSHA256: metadata.AdapterSHA256,
		BaseModelRepo: metadata.BaseModelRepo, BaseModelRevision: metadata.BaseModelRevision,
		Runtime: metadata.Runtime, Quantization: metadata.Quantization,
		MLContractVersion: metadata.MLContractVersion,
		DatasetVersion:    metadata.DatasetVersion, DatasetSHA256: metadata.DatasetSHA256,
		TrainingRunID: metadata.TrainingRunID, TrainingCodeRevision: metadata.TrainingCodeRevision,
		EvaluationRunID: metadata.EvaluationRunID, EvaluationSHA256: metadata.EvaluationSHA256,
		PromptVersion:           metadata.PromptVersion,
		ProjectionPolicyVersion: metadata.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  metadata.ProjectionPolicySHA256,
		ProfileRevision:         metadata.ProfileRevision, ProfileSHA256: metadata.ProfileSHA256,
		CalibrationRevision:    metadata.CalibrationRevision,
		CalibrationSHA256:      metadata.CalibrationSHA256,
		CalibrationTemperature: metadata.CalibrationTemperature,
		IdleThreshold:          metadata.IdleThreshold,
		AnalysisPolicyVersion:  metadata.AnalysisPolicyVersion,
		GateStatus:             gateStatusPassed, OperationalState: state, Reason: reason,
		SelectedAt: s.clock.Now().UTC(), EventID: id,
	}
	if err := next.Validate(); err != nil {
		return protocol.ModelSelection{}, ErrRegistryInvalid
	}
	if err := compatible(s.validate, next); err != nil {
		return protocol.ModelSelection{}, err
	}
	return next, nil
}

func (s *Service) baseSelection(generation int64, reason, state string) (protocol.ModelSelection, error) {
	return newBaseSelection(s.base, generation, reason, state, s.clock.Now().UTC())
}

func newBaseSelection(
	base protocol.ModelSelection,
	generation int64,
	reason, state string,
	now time.Time,
) (protocol.ModelSelection, error) {
	id, err := mintULID(now)
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	base.Generation = generation
	base.Profile = protocol.GatewayProfile{
		Contract:         protocol.ModelContractAnalyzeAgentState,
		InferenceProfile: protocol.InferenceProfileBase,
	}
	base.AdapterSHA256, base.DatasetVersion, base.DatasetSHA256 = "", "", ""
	base.TrainingRunID, base.TrainingCodeRevision = "", ""
	base.EvaluationRunID, base.EvaluationSHA256 = "", ""
	base.GateStatus, base.OperationalState, base.Reason = "base", state, reason
	base.SelectedAt, base.EventID = now, id
	if err := base.Validate(); err != nil {
		return protocol.ModelSelection{}, fmt.Errorf("build base model selection: %w", err)
	}
	return base, nil
}

func persistBase(
	ctx context.Context,
	opts Options,
	generation int64,
	reason, state string,
) (protocol.ModelSelection, error) {
	next, err := newBaseSelection(opts.Base, generation, reason, state, opts.Clock.Now().UTC())
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	if err := persistSelection(ctx, opts.DB, opts.Events, opts.NodeID, nil, next,
		channel.Actor{Type: protocol.ActorTypeSystem, ID: "startup"}); err != nil {
		return protocol.ModelSelection{}, err
	}
	return next, nil
}

func (s *Service) persistChange(
	ctx context.Context,
	current protocol.ModelSelection,
	next protocol.ModelSelection,
	actor channel.Actor,
) error {
	if err := persistSelection(ctx, s.db, s.events, s.nodeID, &current, next, actor); err != nil {
		return err
	}
	if err := s.publisher.Publish(next); err != nil {
		// The database is authoritative. A publication failure is fail-closed and
		// must stop this process rather than inventing an in-memory rollback.
		return fmt.Errorf("publish committed model selection: %w", err)
	}
	return nil
}

func persistSelection(
	ctx context.Context,
	db *sql.DB,
	events EventStore,
	nodeID protocol.NodeID,
	before *protocol.ModelSelection,
	next protocol.ModelSelection,
	actor channel.Actor,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin model selection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if before != nil {
		var durableGeneration int64
		if err := tx.QueryRowContext(ctx, `SELECT generation FROM model_profile_selections
			WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&durableGeneration); err != nil ||
			durableGeneration != before.Generation {
			return fmt.Errorf("model selection changed concurrently")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE model_adapters SET active = 0,
		deactivated_at = CASE WHEN active = 1 THEN ? ELSE deactivated_at END
		WHERE contract = ?`, eventstore.FormatTime(next.SelectedAt), protocol.ModelContractAnalyzeAgentState); err != nil {
		return fmt.Errorf("deactivate model adapter: %w", err)
	}
	if next.Profile.InferenceProfile == protocol.InferenceProfileAnalystLoRA {
		if _, err := readRegistration(ctx, tx, next.Profile.AdapterName, next.Profile.AdapterRevision); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE model_adapters SET active = 1,
			activated_at = ?, deactivated_at = NULL
			WHERE adapter_name = ? AND adapter_revision = ? AND contract = ? AND gate_status = 'passed'
			AND adapter_sha256 = ? AND training_run_id = ?`,
			eventstore.FormatTime(next.SelectedAt), next.Profile.AdapterName,
			next.Profile.AdapterRevision, protocol.ModelContractAnalyzeAgentState,
			next.AdapterSHA256, next.TrainingRunID)
		if err != nil {
			return fmt.Errorf("activate model adapter: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return ErrRegistryInvalid
		}
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode model selection: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO model_profile_selections (
		contract, generation, inference_profile, adapter_name, adapter_revision,
		reason, gate_status, operational_state, event_id, selected_at, selection_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(contract) DO UPDATE SET
		generation = excluded.generation,
		inference_profile = excluded.inference_profile,
		adapter_name = excluded.adapter_name,
		adapter_revision = excluded.adapter_revision,
		reason = excluded.reason,
		gate_status = excluded.gate_status,
		operational_state = excluded.operational_state,
		event_id = excluded.event_id,
		selected_at = excluded.selected_at,
		selection_json = excluded.selection_json`,
		next.Profile.Contract, next.Generation, next.Profile.InferenceProfile,
		nullString(next.Profile.AdapterName), nullString(next.Profile.AdapterRevision),
		next.Reason, next.GateStatus, next.OperationalState, next.EventID,
		eventstore.FormatTime(next.SelectedAt), string(encoded))
	if err != nil {
		return fmt.Errorf("persist model selection: %w", err)
	}
	if err := appendSelectionEvent(ctx, tx, events, nodeID, before, next, actor); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit model selection: %w", err)
	}
	return nil
}

type changePayload struct {
	ActorType    protocol.ActorType       `json:"actor_type"`
	ActorID      string                   `json:"actor_id"`
	Before       *protocol.ModelSelection `json:"before,omitempty"`
	After        protocol.ModelSelection  `json:"after"`
	Reason       string                   `json:"reason"`
	Generation   int64                    `json:"generation"`
	VerifiedRefs verifiedRefs             `json:"verified_refs"`
}

type verifiedRefs struct {
	AdapterSHA256     protocol.SHA256Hex `json:"adapter_sha256,omitempty"`
	DatasetSHA256     protocol.SHA256Hex `json:"dataset_sha256,omitempty"`
	EvaluationSHA256  protocol.SHA256Hex `json:"evaluation_sha256,omitempty"`
	ProfileSHA256     protocol.SHA256Hex `json:"profile_sha256"`
	CalibrationSHA256 protocol.SHA256Hex `json:"calibration_sha256"`
}

func appendSelectionEvent(
	ctx context.Context,
	tx *sql.Tx,
	events EventStore,
	nodeID protocol.NodeID,
	before *protocol.ModelSelection,
	next protocol.ModelSelection,
	actor channel.Actor,
) error {
	payload, err := json.Marshal(changePayload{
		ActorType: actor.Type, ActorID: actor.ID, Before: before, After: next,
		Reason: next.Reason, Generation: next.Generation,
		VerifiedRefs: verifiedRefs{
			AdapterSHA256: next.AdapterSHA256, DatasetSHA256: next.DatasetSHA256,
			EvaluationSHA256: next.EvaluationSHA256, ProfileSHA256: next.ProfileSHA256,
			CalibrationSHA256: next.CalibrationSHA256,
		},
	})
	if err != nil {
		return fmt.Errorf("encode model selection event: %w", err)
	}
	sequence, err := events.NextEventSequenceTx(ctx, tx, "")
	if err != nil {
		return fmt.Errorf("read model event sequence: %w", err)
	}
	event := protocol.EventEnvelope{
		EventID: next.EventID, SchemaVersion: protocol.SchemaVersion, NodeID: nodeID,
		SourceQuality: protocol.SourceQualityInternal,
		EventType:     protocol.EventTypeControlModelProfileChanged,
		Origin:        protocol.OriginSystem, WallTime: next.SelectedAt, Sequence: sequence,
		Payload: protocol.Payload(payload), Sensitivity: protocol.SensitivityNormal,
	}
	if err := events.AppendEventsTx(ctx, tx, []protocol.EventEnvelope{event}); err != nil {
		return fmt.Errorf("append model selection event: %w", err)
	}
	return nil
}

func validateRegistration(req protocol.RegisterModelMetadataRequest) (eval.Report, error) {
	if err := req.Validate(); err != nil {
		return eval.Report{}, fmt.Errorf("%w: %w", ErrRegistryInvalid, err)
	}
	if req.EvaluationSHA256 != digestBytes(req.EvaluationReport) {
		return eval.Report{}, ErrRegistryInvalid
	}
	report, err := decodeJSON[eval.Report](req.EvaluationReport)
	if err != nil || !validEvaluationReport(req, report) {
		return eval.Report{}, ErrRegistryInvalid
	}
	return report, nil
}

func validEvaluationReport(req protocol.RegisterModelMetadataRequest, report eval.Report) bool {
	if report.PromptComparison || report.Bins < 1 || report.Bins > 1000 || report.LatencyBudgetMS <= 0 ||
		report.Mode != eval.EvaluationFinal || report.Partition == nil || report.Calibrations == nil ||
		report.Dataset == nil || report.Dataset.Digest != protocol.Digest("sha256:"+req.DatasetSHA256) ||
		len(report.Profiles) != 2 || report.Gate.Verdict != eval.VerdictPass ||
		report.Safety == nil || !report.Safety.Digest.Valid() || report.Safety.Total < 1 ||
		!report.Safety.Candidate.Passed {
		return false
	}
	requestedGate := make(map[int]protocol.ModelGateCriterion, len(req.Gate))
	for _, criterion := range req.Gate {
		requestedGate[criterion.ID] = criterion
	}
	seen := [6]bool{}
	for _, criterion := range report.Gate.Criteria {
		if criterion.ID < 0 || criterion.ID >= len(seen) || seen[criterion.ID] ||
			criterion.Verdict != eval.VerdictPass {
			return false
		}
		declared, ok := requestedGate[criterion.ID]
		if !ok || declared.Name != criterion.Name || declared.Verdict != string(criterion.Verdict) {
			return false
		}
		seen[criterion.ID] = true
	}
	for _, present := range seen {
		if !present {
			return false
		}
	}
	base, lora, ok := reportProfiles(report)
	if !ok || base.Validation.All.N < 1 || base.Test.All.N < 1 ||
		lora.Validation.All.N < 1 || lora.Test.All.N < 1 ||
		!lora.TemperatureFitted || !lora.ThetaFitted ||
		lora.Temperature != req.CalibrationTemperature || lora.Theta != req.IdleThreshold {
		return false
	}
	baseInput, baseOK := reportInput(report, eval.ProfileBase, "")
	loraInput, loraOK := reportInput(report, eval.ProfileLoRA, "")
	baseSafety, baseSafetyOK := reportInput(report, eval.ProfileBase, "safety")
	loraSafety, loraSafetyOK := reportInput(report, eval.ProfileLoRA, "safety")
	if !baseOK || !loraOK || !baseSafetyOK || !loraSafetyOK ||
		!validReportInput(req, baseInput, eval.ProfileBase) ||
		!validReportInput(req, loraInput, eval.ProfileLoRA) ||
		!validSafetyReportInput(req, baseSafety, eval.ProfileBase, baseInput.Run) ||
		!validSafetyReportInput(req, loraSafety, eval.ProfileLoRA, loraInput.Run) {
		return false
	}
	return validFinalCalibration(req, report, base, lora, baseInput.Run, loraInput.Run)
}

func reportProfiles(report eval.Report) (eval.ProfileReport, eval.ProfileReport, bool) {
	var base, lora eval.ProfileReport
	var haveBase, haveLoRA bool
	for _, profile := range report.Profiles {
		switch profile.Profile {
		case eval.ProfileBase:
			if haveBase || profile.Role != string(eval.ProfileBase) {
				return eval.ProfileReport{}, eval.ProfileReport{}, false
			}
			base, haveBase = profile, true
		case eval.ProfileLoRA:
			if haveLoRA || profile.Role != string(eval.ProfileLoRA) {
				return eval.ProfileReport{}, eval.ProfileReport{}, false
			}
			lora, haveLoRA = profile, true
		default:
			return eval.ProfileReport{}, eval.ProfileReport{}, false
		}
	}
	return base, lora, haveBase && haveLoRA
}

func reportInput(report eval.Report, profile eval.Profile, role string) (eval.Input, bool) {
	var found eval.Input
	have := false
	for _, input := range report.Inputs {
		if input.Profile != profile || input.Role != role {
			continue
		}
		if have {
			return eval.Input{}, false
		}
		found, have = input, true
	}
	return found, have
}

func validReportInput(
	req protocol.RegisterModelMetadataRequest,
	input eval.Input,
	profile eval.Profile,
) bool {
	if input.Lines <= 0 || !protocol.Digest(input.Digest).Valid() || !input.RunRecorded ||
		len(input.Runs) != 2 || !validReportRun(req, input.Run, profile) {
		return false
	}
	for _, split := range []dataset.Split{dataset.SplitValidation, dataset.SplitTest} {
		run, ok := input.Runs[split]
		if !ok || run != input.Run || !validReportRun(req, run, profile) {
			return false
		}
	}
	return true
}

func validSafetyReportInput(
	req protocol.RegisterModelMetadataRequest,
	input eval.Input,
	profile eval.Profile,
	want eval.RunProvenance,
) bool {
	if input.Lines <= 0 || !protocol.Digest(input.Digest).Valid() || !input.RunRecorded ||
		input.Run != want || len(input.Runs) != 1 {
		return false
	}
	test, ok := input.Runs[dataset.SplitTest]
	return ok && test == want && validReportRun(req, test, profile)
}

func validReportRun(
	req protocol.RegisterModelMetadataRequest,
	run eval.RunProvenance,
	profile eval.Profile,
) bool {
	if run.PromptVersion != req.PromptVersion || run.BaseModelRepo != req.BaseModelRepo ||
		run.BaseModelRevision != req.BaseModelRevision || run.Runtime != req.Runtime ||
		run.Quantization != req.Quantization || strings.TrimSpace(run.RuntimeVersion) == "" ||
		strings.TrimSpace(run.GPU) == "" || run.PredictConcurrency < 1 {
		return false
	}
	switch profile {
	case eval.ProfileBase:
		return run.AdapterName == "" && run.AdapterRevision == "" &&
			run.AdapterSHA256 == "" && run.TrainingRunID == ""
	case eval.ProfileLoRA:
		return run.AdapterName == req.AdapterName && run.AdapterRevision == req.AdapterRevision &&
			run.AdapterSHA256 == req.AdapterSHA256 && run.TrainingRunID == req.TrainingRunID
	default:
		return false
	}
}

func validFinalCalibration(
	req protocol.RegisterModelMetadataRequest,
	report eval.Report,
	base, lora eval.ProfileReport,
	baseRun, loraRun eval.RunProvenance,
) bool {
	partition := report.Partition
	if partition.ValidationSessions < 1 || partition.BlindTestSessions < 1 ||
		partition.CalibrationWindows < 1 || partition.ComparisonWindows < 1 ||
		partition.BlindTestWindows < 1 || partition.HistoricalTestWindows < 1 ||
		!partition.ValidationSessionsSHA256.Valid() || !partition.BlindTestSessionsSHA256.Valid() ||
		partition.ValidationSessionsSHA256 != report.Calibrations.ValidationSessionsSHA256 ||
		partition.CalibrationArtifactSHA256 != protocol.Digest("sha256:"+req.CalibrationSHA256) {
		return false
	}
	raw, err := report.Calibrations.Marshal()
	if err != nil || digestBytes(raw) != req.CalibrationSHA256 || len(report.Calibrations.Calibrations) != 2 {
		return false
	}
	profiles := map[eval.Profile]struct {
		report eval.ProfileReport
		run    eval.RunProvenance
	}{
		eval.ProfileBase: {report: base, run: baseRun},
		eval.ProfileLoRA: {report: lora, run: loraRun},
	}
	seen := map[eval.Profile]bool{}
	for _, calibration := range report.Calibrations.Calibrations {
		profile, ok := profiles[calibration.Profile]
		if !ok || seen[calibration.Profile] || calibration.Role != string(calibration.Profile) ||
			calibration.Run != profile.run ||
			calibration.CalibrationInputLines != partition.CalibrationWindows ||
			!calibration.CalibrationInputSHA256.Valid() ||
			calibration.Temperature != profile.report.Temperature ||
			calibration.TemperatureFitted != profile.report.TemperatureFitted ||
			calibration.Theta != profile.report.Theta || calibration.ThetaFitted != profile.report.ThetaFitted ||
			!validReportRun(req, calibration.Run, calibration.Profile) {
			return false
		}
		seen[calibration.Profile] = true
	}
	return seen[eval.ProfileBase] && seen[eval.ProfileLoRA]
}

type evidenceQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func validatePipelineEvidence(
	ctx context.Context,
	q evidenceQuerier,
	req protocol.RegisterModelMetadataRequest,
	report eval.Report,
) error {
	if !validDatasetEvidence(ctx, q, req, report) || !validTrainingEvidence(ctx, q, req) ||
		!validEvaluationEvidence(ctx, q, req, report) {
		return ErrRegistryInvalid
	}
	return nil
}

func validDatasetEvidence(
	ctx context.Context,
	q evidenceQuerier,
	req protocol.RegisterModelMetadataRequest,
	report eval.Report,
) bool {
	var contract, manifestPath, datasetSHA, readinessStatus, readinessJSON string
	var windowCount int64
	var labelJSON, harnessJSON string
	err := q.QueryRowContext(ctx, `SELECT ml_contract_version, manifest_path, dataset_sha256,
		window_count, counts_by_label_json, counts_by_harness_json, readiness_status, readiness_json
		FROM dataset_versions WHERE dataset_version = ?`, req.DatasetVersion).Scan(
		&contract, &manifestPath, &datasetSHA, &windowCount, &labelJSON, &harnessJSON,
		&readinessStatus, &readinessJSON)
	if err != nil || contract != req.MLContractVersion || readinessStatus != gateStatusPassed ||
		readinessStatus != req.DatasetReadinessStatus || digestBytes([]byte(readinessJSON)) != req.DatasetReadinessSHA256 {
		return false
	}
	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		return false
	}
	manifest, parseErr := dataset.ParseManifest(manifestFile)
	closeErr := manifestFile.Close()
	if parseErr != nil || closeErr != nil || manifest.Readiness == nil || !manifest.Readiness.Passed ||
		len(manifest.Readiness.Faults) != 0 || datasetSHA != string(manifest.DatasetDigest) ||
		manifest.DatasetDigest != report.Dataset.Digest || req.DatasetSHA256 != digestHex(manifest.DatasetDigest) ||
		windowCount != manifest.Counts.Windows ||
		!reflect.DeepEqual(report.Dataset.SplitFile, manifest.SplitFile) ||
		!reflect.DeepEqual(report.Dataset.Align, manifest.Align) ||
		report.Dataset.ScriptedInput != manifest.ScriptedInput {
		return false
	}
	var readiness dataset.Readiness
	decoded, err := decodeJSON[dataset.Readiness]([]byte(readinessJSON))
	if err != nil {
		return false
	}
	readiness = decoded
	if !reflect.DeepEqual(readiness, *manifest.Readiness) {
		return false
	}
	return validDatasetCounts(labelJSON, harnessJSON, manifest)
}

func validDatasetCounts(labelJSON, harnessJSON string, manifest dataset.Manifest) bool {
	labels, err := decodeJSON[map[string]int64]([]byte(labelJSON))
	if err != nil {
		return false
	}
	wantLabels := map[string]int64{
		"A": manifest.Counts.ByLabel.A, "B": manifest.Counts.ByLabel.B,
		"C": manifest.Counts.ByLabel.C, "D": manifest.Counts.ByLabel.D,
		"E": manifest.Counts.ByLabel.E, "F": manifest.Counts.ByLabel.F,
		"G": manifest.Counts.ByLabel.G,
	}
	if !zeroTolerantCounts(labels, wantLabels) {
		return false
	}
	harnesses, err := decodeJSON[map[string]int64]([]byte(harnessJSON))
	if err != nil {
		return false
	}
	wantHarnesses := map[string]int64{}
	for _, cell := range manifest.Counts.BySplitLabelHarness {
		wantHarnesses[string(cell.Harness)] += cell.N
	}
	return zeroTolerantCounts(harnesses, wantHarnesses)
}

func zeroTolerantCounts(got, want map[string]int64) bool {
	for key, value := range got {
		if expected, ok := want[key]; !ok || value != expected {
			return false
		}
	}
	for key, expected := range want {
		if got[key] != expected {
			return false
		}
	}
	return true
}

func validTrainingEvidence(ctx context.Context, q evidenceQuerier, req protocol.RegisterModelMetadataRequest) bool {
	var datasetVersion, repo, revision, method, codeRevision, readiness, status string
	var trainingDatasetSHA, manifestPath, manifestSHA, endedAt sql.NullString
	err := q.QueryRowContext(ctx, `SELECT dataset_version, base_model_repo, base_model_revision,
		method, training_code_revision, readiness_status, status, ended_at,
		training_dataset_sha256, training_manifest_path, training_manifest_sha256
		FROM training_runs WHERE training_run_id = ?`, req.TrainingRunID).Scan(
		&datasetVersion, &repo, &revision, &method, &codeRevision, &readiness, &status, &endedAt,
		&trainingDatasetSHA, &manifestPath, &manifestSHA)
	if err != nil || datasetVersion != req.DatasetVersion || repo != req.BaseModelRepo ||
		revision != req.BaseModelRevision || method != "lora" && method != "qlora" ||
		codeRevision != req.TrainingCodeRevision || readiness != req.TrainingReadinessStatus ||
		status != trainingStatusComplete ||
		status != req.TrainingStatus || !endedAt.Valid || endedAt.String == "" {
		return false
	}
	if !trainingDatasetSHA.Valid || trainingDatasetSHA.String != string(req.TrainingDatasetSHA256) ||
		!manifestPath.Valid || filepath.Clean(manifestPath.String) != manifestPath.String ||
		!filepath.IsAbs(manifestPath.String) || !manifestSHA.Valid ||
		!protocol.SHA256Hex(manifestSHA.String).Valid() {
		return false
	}
	manifest, computed, err := readPipelineManifest(protocol.ModelEvidenceReference{
		Path: manifestPath.String, SHA256: protocol.SHA256Hex(manifestSHA.String),
	})
	if err != nil {
		return false
	}
	return readiness == trainingReadinessStatus(computed) &&
		trainingReadinessAllowsPendingTestCoverage(computed) &&
		digestHex(manifest.DatasetDigest) == req.TrainingDatasetSHA256
}

func validAdapterBinding(ctx context.Context, q evidenceQuerier, req protocol.RegisterModelMetadataRequest) bool {
	var adapterSHA, trainingRunID, contract, gate string
	err := q.QueryRowContext(ctx, `SELECT adapter_sha256, training_run_id, contract, gate_status
		FROM model_adapters WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision).Scan(&adapterSHA, &trainingRunID, &contract, &gate)
	return err == nil && adapterSHA == string(req.AdapterSHA256) &&
		trainingRunID == string(req.TrainingRunID) && contract == string(req.Contract) && gate == gateStatusPassed
}

func validEvaluationEvidence(
	ctx context.Context,
	q evidenceQuerier,
	req protocol.RegisterModelMetadataRequest,
	report eval.Report,
) bool {
	base, lora, ok := reportProfiles(report)
	if !ok {
		return false
	}
	var datasetVersion, profile, split, metricsJSON string
	var adapterRevision, gateJSON, evaluationSHA, reportJSON, evidenceJSON sql.NullString
	err := q.QueryRowContext(ctx, `SELECT dataset_version, inference_profile, adapter_revision,
		split, metrics_json, gate_verdict_json, evaluation_sha256,
		evaluation_report_json, evidence_json FROM evaluation_runs WHERE evaluation_run_id = ?`,
		req.EvaluationRunID).Scan(&datasetVersion, &profile, &adapterRevision, &split,
		&metricsJSON, &gateJSON, &evaluationSHA, &reportJSON, &evidenceJSON)
	if err != nil || datasetVersion != req.DatasetVersion || profile != string(eval.ProfileLoRA) ||
		!adapterRevision.Valid || adapterRevision.String != req.AdapterRevision || split != evaluationSplitTest ||
		!metricsEqual(metricsJSON, lora.Test.All) || !gateJSON.Valid || !evaluationSHA.Valid ||
		evaluationSHA.String != string(req.EvaluationSHA256) || !reportJSON.Valid ||
		reportJSON.String != string(req.EvaluationReport) || !evidenceJSON.Valid || evidenceJSON.String == "" {
		return false
	}
	storedGate, err := decodeJSON[eval.Gate]([]byte(gateJSON.String))
	if err != nil || !sameGateVerdicts(storedGate, report.Gate) {
		return false
	}
	return evaluationMetricsExist(ctx, q, req, eval.ProfileBase, "validation", base.Validation.All) &&
		evaluationMetricsExist(ctx, q, req, eval.ProfileBase, evaluationSplitTest, base.Test.All) &&
		evaluationMetricsExist(ctx, q, req, eval.ProfileLoRA, "validation", lora.Validation.All)
}

func sameGateVerdicts(got, want eval.Gate) bool {
	if got.Verdict != want.Verdict || len(got.Criteria) != len(want.Criteria) {
		return false
	}
	for i := range got.Criteria {
		if got.Criteria[i].ID != want.Criteria[i].ID || got.Criteria[i].Name != want.Criteria[i].Name ||
			got.Criteria[i].Verdict != want.Criteria[i].Verdict {
			return false
		}
	}
	return true
}

func evaluationMetricsExist(
	ctx context.Context,
	q evidenceQuerier,
	req protocol.RegisterModelMetadataRequest,
	profile eval.Profile,
	split string,
	want eval.Metrics,
) bool {
	adapterRevision := ""
	if profile == eval.ProfileLoRA {
		adapterRevision = req.AdapterRevision
	}
	rows, err := q.QueryContext(ctx, `SELECT metrics_json FROM evaluation_runs
		WHERE dataset_version = ? AND inference_profile = ? AND split = ?
		AND COALESCE(adapter_revision, '') = ?`, req.DatasetVersion, profile, split, adapterRevision)
	if err != nil {
		return false
	}
	defer func() { _ = rows.Close() }()
	matched := false
	for rows.Next() {
		var raw string
		if rows.Scan(&raw) == nil && metricsEqual(raw, want) {
			matched = true
		}
	}
	return rows.Err() == nil && matched
}

func metricsEqual(raw string, want eval.Metrics) bool {
	got, err := decodeJSON[eval.Metrics]([]byte(raw))
	return err == nil && reflect.DeepEqual(got, want)
}

func decodeJSON[T any](raw []byte) (T, error) {
	var value T
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&value); err != nil {
		return value, fmt.Errorf("decode JSON: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value, errors.New("trailing JSON data")
	}
	return value, nil
}

func digestBytes(raw []byte) protocol.SHA256Hex {
	sum := sha256.Sum256(raw)
	return protocol.SHA256Hex(hex.EncodeToString(sum[:]))
}

func digestHex(value protocol.Digest) protocol.SHA256Hex {
	if !value.Valid() {
		return ""
	}
	return protocol.SHA256Hex(strings.TrimPrefix(string(value), "sha256:"))
}

func bindAdapter(ctx context.Context, tx *sql.Tx, req protocol.RegisterModelMetadataRequest) error {
	var sha, run, contract, gate string
	err := tx.QueryRowContext(ctx, `SELECT adapter_sha256, training_run_id, contract, gate_status
		FROM model_adapters WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision).Scan(&sha, &run, &contract, &gate)
	if err == nil {
		if sha != string(req.AdapterSHA256) || run != string(req.TrainingRunID) ||
			contract != string(req.Contract) || gate != gateStatusPassed {
			return ErrRegistryInvalid
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read model adapter binding: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO model_adapters (
		adapter_name, adapter_revision, adapter_sha256, training_run_id, contract,
		gate_status, active
	) VALUES (?, ?, ?, ?, ?, 'passed', 0)`, req.AdapterName, req.AdapterRevision,
		req.AdapterSHA256, req.TrainingRunID, req.Contract)
	if err != nil {
		return fmt.Errorf("insert model adapter binding: %w", err)
	}
	return nil
}

func readRegistration(
	ctx context.Context,
	q evidenceQuerier,
	name, revision string,
) (protocol.RegisterModelMetadataRequest, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT metadata_json FROM model_profile_registry
		WHERE adapter_name = ? AND adapter_revision = ?`, name, revision).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.RegisterModelMetadataRequest{}, ErrAdapterNotRegistered
	}
	if err != nil {
		return protocol.RegisterModelMetadataRequest{}, fmt.Errorf("read model registry: %w", err)
	}
	var metadata protocol.RegisterModelMetadataRequest
	if json.Unmarshal([]byte(raw), &metadata) != nil {
		return protocol.RegisterModelMetadataRequest{}, ErrRegistryInvalid
	}
	report, validationErr := validateRegistration(metadata)
	if validationErr != nil || metadata.AdapterName != name || metadata.AdapterRevision != revision ||
		validatePipelineEvidence(ctx, q, metadata, report) != nil || !validAdapterBinding(ctx, q, metadata) {
		return protocol.RegisterModelMetadataRequest{}, ErrRegistryInvalid
	}
	return metadata, nil
}

func selectionBound(ctx context.Context, db *sql.DB, selected protocol.ModelSelection) error {
	if selected.Profile.InferenceProfile == protocol.InferenceProfileBase {
		var active int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_adapters
			WHERE contract = ? AND active = 1`, protocol.ModelContractAnalyzeAgentState).Scan(&active); err != nil || active != 0 {
			return ErrRegistryInvalid
		}
		return nil
	}
	metadata, err := readRegistration(ctx, db, selected.Profile.AdapterName, selected.Profile.AdapterRevision)
	if err != nil {
		return err
	}
	want := protocol.ModelSelection{
		Profile: protocol.GatewayProfile{
			Contract: metadata.Contract, InferenceProfile: protocol.InferenceProfileAnalystLoRA,
			AdapterName: metadata.AdapterName, AdapterRevision: metadata.AdapterRevision,
		},
		AdapterSHA256: metadata.AdapterSHA256,
		BaseModelRepo: metadata.BaseModelRepo, BaseModelRevision: metadata.BaseModelRevision,
		Runtime: metadata.Runtime, Quantization: metadata.Quantization,
		MLContractVersion: metadata.MLContractVersion,
		DatasetVersion:    metadata.DatasetVersion, DatasetSHA256: metadata.DatasetSHA256,
		TrainingRunID: metadata.TrainingRunID, TrainingCodeRevision: metadata.TrainingCodeRevision,
		EvaluationRunID: metadata.EvaluationRunID, EvaluationSHA256: metadata.EvaluationSHA256,
		PromptVersion:           metadata.PromptVersion,
		ProjectionPolicyVersion: metadata.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  metadata.ProjectionPolicySHA256,
		ProfileRevision:         metadata.ProfileRevision, ProfileSHA256: metadata.ProfileSHA256,
		CalibrationRevision:    metadata.CalibrationRevision,
		CalibrationSHA256:      metadata.CalibrationSHA256,
		CalibrationTemperature: metadata.CalibrationTemperature,
		IdleThreshold:          metadata.IdleThreshold,
		AnalysisPolicyVersion:  metadata.AnalysisPolicyVersion,
	}
	got := selected
	got.Generation, got.GateStatus, got.OperationalState = 0, "", ""
	got.Reason, got.SelectedAt, got.EventID = "", time.Time{}, ""
	if got != want {
		return ErrRegistryInvalid
	}
	var adapterSHA, trainingRunID, contract, gate string
	var active int64
	err = db.QueryRowContext(ctx, `SELECT adapter_sha256, training_run_id, contract, gate_status, active
		FROM model_adapters WHERE adapter_name = ? AND adapter_revision = ?`,
		selected.Profile.AdapterName, selected.Profile.AdapterRevision).Scan(
		&adapterSHA, &trainingRunID, &contract, &gate, &active)
	if err != nil || adapterSHA != string(selected.AdapterSHA256) ||
		trainingRunID != string(selected.TrainingRunID) ||
		contract != string(protocol.ModelContractAnalyzeAgentState) || gate != gateStatusPassed || active != 1 {
		return ErrRegistryInvalid
	}
	return nil
}

func compatible(validate SelectionValidator, selection protocol.ModelSelection) error {
	if validate == nil {
		return nil
	}
	if err := validate(selection); err != nil {
		return fmt.Errorf("%w: incompatible selected tuple", ErrRegistryInvalid)
	}
	return nil
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

const crockfordDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("mint model event id: time outside range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", fmt.Errorf("mint model event id: %w", err)
	}
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
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
		out.WriteByte(crockfordDigits[index])
	}
	return protocol.ULID(out.String()), nil
}
