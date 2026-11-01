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
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const gateStatusPassed = "passed"

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
	if err := validateRegistration(req); err != nil {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("begin model metadata import: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
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
		if err := tx.Commit(); err != nil {
			return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("commit model metadata binding: %w", err)
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
	if err := tx.Commit(); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("commit model metadata: %w", err)
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
		result, err := tx.ExecContext(ctx, `UPDATE model_adapters SET active = 1,
			activated_at = ?, deactivated_at = NULL
			WHERE adapter_name = ? AND adapter_revision = ? AND contract = ? AND gate_status = 'passed'`,
			eventstore.FormatTime(next.SelectedAt), next.Profile.AdapterName,
			next.Profile.AdapterRevision, protocol.ModelContractAnalyzeAgentState)
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

func validateRegistration(req protocol.RegisterModelMetadataRequest) error {
	if err := req.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrRegistryInvalid, err)
	}
	sum := sha256.Sum256(req.EvaluationReport)
	if !bytes.Equal([]byte(strings.ToLower(string(req.EvaluationSHA256))), []byte(hex.EncodeToString(sum[:]))) {
		return ErrRegistryInvalid
	}
	return nil
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
	q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
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
	if json.Unmarshal([]byte(raw), &metadata) != nil || validateRegistration(metadata) != nil ||
		metadata.AdapterName != name || metadata.AdapterRevision != revision {
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
