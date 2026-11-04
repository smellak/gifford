package modelprofile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const overrideReasonPrefix = "operator override: "

// activationOverrideRecord is the immutable evidence written in the same
// transaction as its selected generation.
type activationOverrideRecord struct {
	request     protocol.ActivateAdapterOverrideRequest
	adapter     protocol.ModelAdapterMetadata
	training    protocol.ModelTrainingRunMetadata
	adapterRaw  []byte
	trainingRaw []byte
	reportRaw   []byte
	selection   protocol.ModelSelection
	actor       channel.Actor
}

// ActivateOverride selects a served adapter from explicit, hashed operator
// evidence without registering it or representing its evaluation gate as a
// pass.
func (s *Service) ActivateOverride(
	ctx context.Context,
	req protocol.ActivateAdapterOverrideRequest,
	actor channel.Actor,
) (protocol.ModelActivationResult, error) {
	if actor.Type != protocol.ActorTypeHuman || strings.TrimSpace(actor.ID) == "" ||
		utf8.RuneCountInString(actor.ID) > 128 {
		return protocol.ModelActivationResult{}, ErrRegistryInvalid
	}
	record, err := prepareActivationOverride(req)
	if err != nil {
		return protocol.ModelActivationResult{}, fmt.Errorf("%w: override evidence: %w", ErrRegistryInvalid, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.publisher.CurrentModelSelection()
	next, err := s.overrideSelection(
		record.adapter, record.training, req.EvaluationReport.SHA256,
		req.Reason, current.Generation+1,
	)
	if err != nil {
		return protocol.ModelActivationResult{}, err
	}
	s.verifyMu.RLock()
	verifier := s.verifier
	s.verifyMu.RUnlock()
	if verifier == nil || verifier.VerifyModelSelection(ctx, next) != nil {
		return protocol.ModelActivationResult{}, ErrProfileNotServed
	}
	record.selection, record.actor = next, actor
	if err := s.persistOverrideChange(ctx, current, record); err != nil {
		return protocol.ModelActivationResult{}, err
	}
	return protocol.ModelActivationResult{Applied: true, Selection: next}, nil
}

func prepareActivationOverride(
	req protocol.ActivateAdapterOverrideRequest,
) (activationOverrideRecord, error) {
	if err := req.Validate(); err != nil {
		return activationOverrideRecord{}, fmt.Errorf("validate override request: %w", err)
	}
	adapterRaw, err := readPipelineEvidence(req.AdapterMetadata, maxPipelineMetadataBytes)
	if err != nil {
		return activationOverrideRecord{}, err
	}
	adapter, err := decodeJSON[protocol.ModelAdapterMetadata](adapterRaw)
	if err != nil {
		return activationOverrideRecord{}, errors.New("adapter metadata is not the expected JSON document")
	}
	if err := adapter.Validate(); err != nil {
		return activationOverrideRecord{}, fmt.Errorf("adapter metadata: %w", err)
	}
	trainingRaw, err := readPipelineEvidence(req.TrainingRunMetadata, maxPipelineMetadataBytes)
	if err != nil {
		return activationOverrideRecord{}, err
	}
	training, err := decodeJSON[protocol.ModelTrainingRunMetadata](trainingRaw)
	if err != nil {
		return activationOverrideRecord{}, errors.New("training metadata is not the expected JSON document")
	}
	if err := training.Validate(); err != nil {
		return activationOverrideRecord{}, fmt.Errorf("training metadata: %w", err)
	}
	if training.Status != trainingStatusComplete {
		return activationOverrideRecord{}, errors.New("training metadata: status: must be " + trainingStatusComplete)
	}
	if adapter.TrainingRunID != training.TrainingRunID ||
		adapter.BaseModelRepo != training.BaseModelRepo ||
		adapter.BaseModelRevision != training.BaseModelRevision ||
		adapter.Contract != training.Contract {
		return activationOverrideRecord{}, errors.New("adapter and training identities do not match")
	}
	reportRaw, err := readPipelineEvidence(req.EvaluationReport, maxPipelineMetadataBytes)
	if err != nil {
		return activationOverrideRecord{}, err
	}
	report, err := decodeJSON[eval.Report](reportRaw)
	if err != nil || !validOverrideReport(report, adapter) {
		return activationOverrideRecord{}, errors.New("evaluation report does not bind the adapter")
	}
	return activationOverrideRecord{
		request: req, adapter: adapter, training: training,
		adapterRaw: adapterRaw, trainingRaw: trainingRaw, reportRaw: reportRaw,
	}, nil
}

func validOverrideReport(report eval.Report, adapter protocol.ModelAdapterMetadata) bool {
	if _, _, ok := reportProfiles(report); !ok {
		return false
	}
	base, baseOK := reportInput(report, eval.ProfileBase, "")
	lora, loraOK := reportInput(report, eval.ProfileLoRA, "")
	if !baseOK || !loraOK || !base.RunRecorded || !lora.RunRecorded {
		return false
	}
	if base.Run.AdapterName != "" || base.Run.AdapterRevision != "" ||
		base.Run.AdapterSHA256 != "" || base.Run.TrainingRunID != "" {
		return false
	}
	for _, run := range []eval.RunProvenance{base.Run, lora.Run} {
		if run.BaseModelRepo != adapter.BaseModelRepo ||
			run.BaseModelRevision != adapter.BaseModelRevision {
			return false
		}
	}
	return lora.Run.AdapterName == adapter.AdapterName &&
		lora.Run.AdapterRevision == adapter.AdapterRevision &&
		lora.Run.AdapterSHA256 == adapter.AdapterSHA256 &&
		lora.Run.TrainingRunID == adapter.TrainingRunID
}

func (s *Service) overrideSelection(
	adapter protocol.ModelAdapterMetadata,
	training protocol.ModelTrainingRunMetadata,
	evaluationSHA protocol.SHA256Hex,
	reason string,
	generation int64,
) (protocol.ModelSelection, error) {
	now := s.clock.Now().UTC()
	evaluationID, err := mintULID(now)
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	eventID, err := mintULID(now)
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	next := protocol.ModelSelection{
		Generation: generation,
		Profile: protocol.GatewayProfile{
			Contract:         protocol.ModelContractAnalyzeAgentState,
			InferenceProfile: protocol.InferenceProfileAnalystLoRA,
			AdapterName:      adapter.AdapterName, AdapterRevision: adapter.AdapterRevision,
		},
		AdapterSHA256: adapter.AdapterSHA256,
		BaseModelRepo: adapter.BaseModelRepo, BaseModelRevision: adapter.BaseModelRevision,
		Runtime: adapter.Runtime, Quantization: adapter.Quantization,
		MLContractVersion: training.MLContractVersion,
		DatasetVersion:    training.DatasetVersion, DatasetSHA256: training.DatasetSHA256,
		TrainingRunID: training.TrainingRunID, TrainingCodeRevision: training.TrainingCodeRevision,
		EvaluationRunID: protocol.OpaqueID(evaluationID), EvaluationSHA256: evaluationSHA,
		PromptVersion:           adapter.PromptVersion,
		ProjectionPolicyVersion: adapter.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  adapter.ProjectionPolicySHA256,
		ProfileRevision:         adapter.ProfileRevision, ProfileSHA256: adapter.ProfileSHA256,
		CalibrationRevision:    adapter.CalibrationRevision,
		CalibrationSHA256:      adapter.CalibrationSHA256,
		CalibrationTemperature: adapter.CalibrationTemperature,
		IdleThreshold:          adapter.IdleThreshold,
		AnalysisPolicyVersion:  adapter.AnalysisPolicyVersion,
		GateStatus:             protocol.ModelGateStatusOverride, OperationalState: "READY",
		Reason: overrideReasonPrefix + reason, SelectedAt: now, EventID: eventID,
	}
	if err := next.Validate(); err != nil {
		return protocol.ModelSelection{}, fmt.Errorf("%w: selection: %w", ErrRegistryInvalid, err)
	}
	if err := compatible(s.validate, next); err != nil {
		return protocol.ModelSelection{}, err
	}
	return next, nil
}

func (s *Service) persistOverrideChange(
	ctx context.Context,
	current protocol.ModelSelection,
	record activationOverrideRecord,
) error {
	if err := persistSelection(
		ctx, s.db, s.events, s.nodeID, &current, record.selection, record.actor, &record,
	); err != nil {
		return err
	}
	if err := s.publisher.Publish(record.selection); err != nil {
		return fmt.Errorf("publish committed model selection: %w", err)
	}
	return nil
}

func persistActivationOverride(
	ctx context.Context,
	tx *sql.Tx,
	record activationOverrideRecord,
	next protocol.ModelSelection,
	selectionJSON string,
) error {
	if record.selection != next || record.actor.Type != protocol.ActorTypeHuman ||
		record.request.EvaluationReport.SHA256 != next.EvaluationSHA256 ||
		next.Reason != overrideReasonPrefix+record.request.Reason ||
		digestBytes(record.adapterRaw) != record.request.AdapterMetadata.SHA256 ||
		digestBytes(record.trainingRaw) != record.request.TrainingRunMetadata.SHA256 ||
		digestBytes(record.reportRaw) != record.request.EvaluationReport.SHA256 {
		return ErrRegistryInvalid
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO model_activation_overrides (
		evaluation_run_id, contract, generation, adapter_name, adapter_revision,
		adapter_sha256, training_run_id, adapter_metadata_sha256,
		training_run_metadata_sha256, evaluation_report_sha256, reason,
		actor_type, actor_id, created_at, selection_json, adapter_metadata_json,
		training_run_metadata_json, evaluation_report_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		next.EvaluationRunID, next.Profile.Contract, next.Generation,
		next.Profile.AdapterName, next.Profile.AdapterRevision, next.AdapterSHA256,
		next.TrainingRunID, record.request.AdapterMetadata.SHA256,
		record.request.TrainingRunMetadata.SHA256, record.request.EvaluationReport.SHA256,
		next.Reason, record.actor.Type, record.actor.ID,
		eventstore.FormatTime(next.SelectedAt), selectionJSON, string(record.adapterRaw),
		string(record.trainingRaw), string(record.reportRaw))
	if err != nil {
		return fmt.Errorf("persist model activation override: %w", err)
	}
	return nil
}

func overrideSelectionBound(
	ctx context.Context,
	q evidenceQuerier,
	selected protocol.ModelSelection,
) error {
	var contract, adapterName, adapterRevision, adapterSHA, trainingRunID string
	var adapterMetadataSHA, trainingMetadataSHA, reportSHA, reason string
	var actorType, actorID, createdAt, selectionJSON string
	var adapterJSON, trainingJSON, reportJSON string
	var generation int64
	err := q.QueryRowContext(ctx, `SELECT contract, generation, adapter_name,
		adapter_revision, adapter_sha256, training_run_id, adapter_metadata_sha256,
		training_run_metadata_sha256, evaluation_report_sha256, reason, actor_type,
		actor_id, created_at, selection_json, adapter_metadata_json,
		training_run_metadata_json, evaluation_report_json
		FROM model_activation_overrides WHERE evaluation_run_id = ?`,
		selected.EvaluationRunID).Scan(
		&contract, &generation, &adapterName, &adapterRevision, &adapterSHA,
		&trainingRunID, &adapterMetadataSHA, &trainingMetadataSHA, &reportSHA,
		&reason, &actorType, &actorID, &createdAt, &selectionJSON, &adapterJSON,
		&trainingJSON, &reportJSON,
	)
	if err != nil || contract != string(selected.Profile.Contract) ||
		generation != selected.Generation || adapterName != selected.Profile.AdapterName ||
		adapterRevision != selected.Profile.AdapterRevision ||
		adapterSHA != string(selected.AdapterSHA256) || trainingRunID != string(selected.TrainingRunID) ||
		reportSHA != string(selected.EvaluationSHA256) || selected.Reason != reason ||
		protocol.ActorType(actorType) != protocol.ActorTypeHuman ||
		strings.TrimSpace(actorID) == "" || utf8.RuneCountInString(actorID) > 128 {
		return ErrRegistryInvalid
	}
	writtenAt, err := eventstore.ParseTime(createdAt)
	if err != nil || !writtenAt.Equal(selected.SelectedAt) {
		return ErrRegistryInvalid
	}
	encoded, err := json.Marshal(selected)
	if err != nil || selectionJSON != string(encoded) ||
		digestBytes([]byte(adapterJSON)) != protocol.SHA256Hex(adapterMetadataSHA) ||
		digestBytes([]byte(trainingJSON)) != protocol.SHA256Hex(trainingMetadataSHA) ||
		digestBytes([]byte(reportJSON)) != protocol.SHA256Hex(reportSHA) {
		return ErrRegistryInvalid
	}
	adapter, err := decodeJSON[protocol.ModelAdapterMetadata]([]byte(adapterJSON))
	if err != nil || adapter.Validate() != nil || adapter.AdapterName != adapterName ||
		adapter.AdapterRevision != adapterRevision || adapter.AdapterSHA256 != selected.AdapterSHA256 ||
		selected.Profile.Contract != adapter.Contract || selected.BaseModelRepo != adapter.BaseModelRepo ||
		selected.BaseModelRevision != adapter.BaseModelRevision || selected.Runtime != adapter.Runtime ||
		selected.Quantization != adapter.Quantization || selected.PromptVersion != adapter.PromptVersion ||
		selected.ProjectionPolicyVersion != adapter.ProjectionPolicyVersion ||
		selected.ProjectionPolicySHA256 != adapter.ProjectionPolicySHA256 ||
		selected.ProfileRevision != adapter.ProfileRevision || selected.ProfileSHA256 != adapter.ProfileSHA256 ||
		selected.CalibrationRevision != adapter.CalibrationRevision ||
		selected.CalibrationSHA256 != adapter.CalibrationSHA256 ||
		selected.CalibrationTemperature != adapter.CalibrationTemperature ||
		selected.IdleThreshold != adapter.IdleThreshold ||
		selected.AnalysisPolicyVersion != adapter.AnalysisPolicyVersion {
		return ErrRegistryInvalid
	}
	training, err := decodeJSON[protocol.ModelTrainingRunMetadata]([]byte(trainingJSON))
	if err != nil || training.Validate() != nil || training.Status != trainingStatusComplete ||
		training.TrainingRunID != selected.TrainingRunID ||
		training.DatasetVersion != selected.DatasetVersion || training.DatasetSHA256 != selected.DatasetSHA256 ||
		training.MLContractVersion != selected.MLContractVersion ||
		training.TrainingCodeRevision != selected.TrainingCodeRevision ||
		adapter.TrainingRunID != training.TrainingRunID ||
		adapter.BaseModelRepo != training.BaseModelRepo ||
		adapter.BaseModelRevision != training.BaseModelRevision || adapter.Contract != training.Contract {
		return ErrRegistryInvalid
	}
	report, err := decodeJSON[eval.Report]([]byte(reportJSON))
	if err != nil || !validOverrideReport(report, adapter) {
		return ErrRegistryInvalid
	}
	var active int64
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_adapters
		WHERE contract = ? AND active = 1`, selected.Profile.Contract).Scan(&active); err != nil || active != 0 {
		return ErrRegistryInvalid
	}
	return nil
}
