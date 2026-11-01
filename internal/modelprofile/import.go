package modelprofile

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	maxPipelineMetadataBytes = 1 << 20
	maxPipelineEvidenceBytes = 1 << 30
)

type pipelineImport struct {
	request                protocol.RegisterModelPipelineRequest
	registration           protocol.RegisterModelMetadataRequest
	manifest               dataset.Manifest
	manifestSHA256         protocol.SHA256Hex
	readiness              dataset.Readiness
	readinessJSON          []byte
	trainingManifest       dataset.Manifest
	trainingManifestPath   string
	trainingManifestSHA256 protocol.SHA256Hex
	training               protocol.ModelTrainingRunMetadata
	trainingPath           string
	trainingSHA256         protocol.SHA256Hex
	adapter                protocol.ModelAdapterMetadata
	adapterPath            string
	adapterSHA256          protocol.SHA256Hex
	report                 eval.Report
	reportJSON             []byte
	evidenceJSON           []byte
	gateStatus             string
	activatable            bool
}

// RegisterPipeline verifies one local evidence bundle, computes its report,
// persists the linked pipeline rows, and uses the ordinary Register writer only
// when the computed result is activatable. Non-activatable evidence remains
// durable for audit, but no registry row or selection change is made.
func (s *Service) RegisterPipeline(
	ctx context.Context,
	req protocol.RegisterModelPipelineRequest,
	_ channel.Actor,
) (protocol.ModelMetadataRegistrationResult, error) {
	plan, err := preparePipelineImport(req)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("%w: pipeline evidence", ErrRegistryInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("begin model pipeline import: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := persistPipelineImport(ctx, tx, plan, s.clock.Now().UTC()); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	if !plan.activatable {
		if err := tx.Commit(); err != nil {
			return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("commit model pipeline evidence: %w", err)
		}
		return protocol.ModelMetadataRegistrationResult{}, ErrPipelineNotActivatable
	}
	result, err := s.registerTx(ctx, tx, plan.registration, plan.report)
	if err != nil {
		return protocol.ModelMetadataRegistrationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.ModelMetadataRegistrationResult{}, fmt.Errorf("commit model pipeline registration: %w", err)
	}
	return result, nil
}

func preparePipelineImport(req protocol.RegisterModelPipelineRequest) (pipelineImport, error) {
	if err := req.Validate(); err != nil {
		return pipelineImport{}, fmt.Errorf("validate pipeline request: %w", err)
	}
	manifest, readiness, err := readPipelineManifest(req.Manifest)
	if err != nil {
		return pipelineImport{}, err
	}
	readinessJSON, err := json.Marshal(readiness)
	if err != nil {
		return pipelineImport{}, fmt.Errorf("encode readiness: %w", err)
	}

	trainingRaw, err := readPipelineEvidence(req.TrainingRunMetadata, maxPipelineMetadataBytes)
	if err != nil {
		return pipelineImport{}, err
	}
	training, err := decodeJSON[protocol.ModelTrainingRunMetadata](trainingRaw)
	if err != nil || training.Validate() != nil {
		return pipelineImport{}, errors.New("invalid training run metadata")
	}
	adapterRaw, err := readPipelineEvidence(req.AdapterMetadata, maxPipelineMetadataBytes)
	if err != nil {
		return pipelineImport{}, err
	}
	adapter, err := decodeJSON[protocol.ModelAdapterMetadata](adapterRaw)
	if err != nil || adapter.Validate() != nil {
		return pipelineImport{}, errors.New("invalid adapter metadata")
	}
	trainingManifest := manifest
	trainingReadiness := readiness
	trainingManifestRef := req.Manifest
	var finalAdditions map[protocol.ULID]struct{}
	if req.FinalEvaluation != nil {
		trainingManifestRef = req.FinalEvaluation.TrainingManifest
		trainingManifest, trainingReadiness, err = readPipelineManifest(trainingManifestRef)
		if err != nil {
			return pipelineImport{}, fmt.Errorf("read training manifest: %w", err)
		}
		finalAdditions, err = validateFinalManifestExtension(trainingManifest, manifest)
		if err != nil {
			return pipelineImport{}, err
		}
	}
	if err := validatePipelineIdentities(
		manifest, trainingManifest, trainingReadiness, training, adapter,
	); err != nil {
		return pipelineImport{}, err
	}

	base, baseRaw, err := readPipelinePredictions(req.BasePredictions, eval.ProfileBase)
	if err != nil {
		return pipelineImport{}, err
	}
	candidate, candidateRaw, err := readPipelinePredictions(req.CandidatePredictions, eval.ProfileLoRA)
	if err != nil {
		return pipelineImport{}, err
	}
	safetyManifestRaw, err := readPipelineEvidence(req.SafetyManifest, maxPipelineEvidenceBytes)
	if err != nil {
		return pipelineImport{}, err
	}
	safetyManifest, err := dataset.ParseManifest(bytes.NewReader(safetyManifestRaw))
	if err != nil {
		return pipelineImport{}, errors.New("invalid safety manifest")
	}
	safetyBase, safetyBaseRaw, err := readPipelinePredictions(req.SafetyBasePredictions, eval.ProfileBase)
	if err != nil {
		return pipelineImport{}, err
	}
	safetyCandidate, safetyCandidateRaw, err := readPipelinePredictions(req.SafetyCandidatePredictions, eval.ProfileLoRA)
	if err != nil {
		return pipelineImport{}, err
	}
	if err := validatePredictionIdentities(base, candidate, safetyBase, safetyCandidate, training, adapter); err != nil {
		return pipelineImport{}, err
	}
	valCalSessions, blindSessions, sealedCalibrations, sealedCalibrationsRaw, err := readFinalEvaluationEvidence(req.FinalEvaluation, manifest.ScreenForm, adapter, finalAdditions, base)
	if err != nil {
		return pipelineImport{}, err
	}
	report, err := eval.Evaluate(eval.Options{
		Base: base, LoRA: candidate, BaseRaw: baseRaw, LoRARaw: candidateRaw,
		SafetyBase: safetyBase, SafetyCandidate: safetyCandidate,
		SafetyBaseRaw: safetyBaseRaw, SafetyCandidateRaw: safetyCandidateRaw,
		Manifest:       eval.ManifestCheck{Given: true, Manifest: manifest},
		SafetyManifest: eval.ManifestCheck{Given: true, Manifest: safetyManifest},
		Bins:           eval.DefaultBins, LatencyBudget: eval.DefaultLatencyBudget,
		ValidationCalibrationSessions: valCalSessions,
		BlindTestSessions:             blindSessions,
		SealedCalibrations:            sealedCalibrations,
		SealedCalibrationsRaw:         sealedCalibrationsRaw,
	})
	if err != nil {
		return pipelineImport{}, fmt.Errorf("evaluate predictions: %w", err)
	}
	_, lora, ok := reportProfiles(report)
	if !ok || lora.Temperature != adapter.CalibrationTemperature || lora.Theta != adapter.IdleThreshold {
		return pipelineImport{}, errors.New("adapter calibration does not match computed evaluation")
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return pipelineImport{}, fmt.Errorf("encode evaluation report: %w", err)
	}
	evidenceJSON, err := json.Marshal(req)
	if err != nil {
		return pipelineImport{}, fmt.Errorf("encode pipeline evidence: %w", err)
	}
	gate := make([]protocol.ModelGateCriterion, 0, len(report.Gate.Criteria))
	for _, criterion := range report.Gate.Criteria {
		gate = append(gate, protocol.ModelGateCriterion{
			ID: criterion.ID, Name: criterion.Name, Verdict: string(criterion.Verdict),
		})
	}
	registration := protocol.RegisterModelMetadataRequest{
		AdapterName: adapter.AdapterName, AdapterRevision: adapter.AdapterRevision,
		AdapterSHA256: adapter.AdapterSHA256, Contract: adapter.Contract,
		BaseModelRepo: adapter.BaseModelRepo, BaseModelRevision: adapter.BaseModelRevision,
		Runtime: adapter.Runtime, Quantization: adapter.Quantization,
		MLContractVersion: training.MLContractVersion,
		DatasetVersion:    training.DatasetVersion, DatasetSHA256: digestHex(manifest.DatasetDigest),
		TrainingDatasetSHA256:  training.DatasetSHA256,
		DatasetReadinessStatus: readinessStatus(readiness), DatasetReadinessSHA256: digestBytes(readinessJSON),
		TrainingRunID: training.TrainingRunID, TrainingCodeRevision: training.TrainingCodeRevision,
		TrainingReadinessStatus: training.ReadinessStatus, TrainingStatus: training.Status,
		EvaluationRunID: req.EvaluationRunID, EvaluationSHA256: digestBytes(reportJSON),
		EvaluationReport: reportJSON, Gate: gate,
		PromptVersion:           adapter.PromptVersion,
		ProjectionPolicyVersion: adapter.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  adapter.ProjectionPolicySHA256,
		ProfileRevision:         adapter.ProfileRevision, ProfileSHA256: adapter.ProfileSHA256,
		CalibrationRevision: adapter.CalibrationRevision, CalibrationSHA256: adapter.CalibrationSHA256,
		CalibrationTemperature: adapter.CalibrationTemperature, IdleThreshold: adapter.IdleThreshold,
		AnalysisPolicyVersion: adapter.AnalysisPolicyVersion,
	}
	trainingReady := trainingReadinessAllowsPendingTestCoverage(trainingReadiness)
	activatable := readiness.Passed && trainingReady && training.Status == trainingStatusComplete &&
		report.Gate.Verdict == eval.VerdictPass
	if activatable {
		_, err = validateRegistration(registration)
		activatable = err == nil
	}
	return pipelineImport{
		request: req, registration: registration, manifest: manifest,
		manifestSHA256: req.Manifest.SHA256, readiness: readiness, readinessJSON: readinessJSON,
		trainingManifest: trainingManifest, trainingManifestPath: trainingManifestRef.Path,
		trainingManifestSHA256: trainingManifestRef.SHA256,
		training:               training, trainingPath: req.TrainingRunMetadata.Path,
		trainingSHA256: req.TrainingRunMetadata.SHA256,
		adapter:        adapter, adapterPath: req.AdapterMetadata.Path,
		adapterSHA256: req.AdapterMetadata.SHA256,
		report:        report, reportJSON: reportJSON, evidenceJSON: evidenceJSON,
		gateStatus: pipelineGateStatus(readiness, trainingReadiness, training, report), activatable: activatable,
	}, nil
}

func readPipelineEvidence(ref protocol.ModelEvidenceReference, limit int64) ([]byte, error) {
	clean := filepath.Clean(ref.Path)
	if !filepath.IsAbs(clean) || clean != ref.Path {
		return nil, errors.New("evidence path must be clean and absolute")
	}
	file, err := os.Open(clean)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filepath.Base(clean), err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("evidence file is not a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("read bounded evidence file")
	}
	if digestBytes(raw) != ref.SHA256 {
		return nil, errors.New("evidence digest mismatch")
	}
	return raw, nil
}

func readPipelineManifest(
	ref protocol.ModelEvidenceReference,
) (dataset.Manifest, dataset.Readiness, error) {
	raw, err := readPipelineEvidence(ref, maxPipelineEvidenceBytes)
	if err != nil {
		return dataset.Manifest{}, dataset.Readiness{}, err
	}
	manifest, err := dataset.ParseManifest(bytes.NewReader(raw))
	if err != nil || manifest.Readiness == nil {
		return dataset.Manifest{}, dataset.Readiness{}, errors.New("manifest has no valid readiness policy")
	}
	readiness := dataset.CheckReadiness(manifest.Entries, manifest.Readiness.Thresholds)
	if !reflect.DeepEqual(readiness, *manifest.Readiness) {
		return dataset.Manifest{}, dataset.Readiness{}, errors.New("manifest readiness does not recompute")
	}
	return manifest, readiness, nil
}

func readFinalEvaluationEvidence(
	final *protocol.ModelFinalEvaluationEvidence,
	screenForm string,
	adapter protocol.ModelAdapterMetadata,
	finalAdditions map[protocol.ULID]struct{},
	base []eval.Prediction,
) (eval.SessionSet, eval.SessionSet, *eval.CalibrationSet, []byte, error) {
	if final == nil {
		if screenForm == dataset.ScreenFormLiveV2 {
			return eval.SessionSet{}, eval.SessionSet{}, nil, nil,
				errors.New("live-input-v2 pipeline requires final evaluation evidence")
		}
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil, nil
	}
	valCalRaw, err := readPipelineEvidence(final.ValidationCalibrationSessions, maxPipelineMetadataBytes)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil, err
	}
	valCal, err := eval.ParseSessionSet(valCalRaw)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil,
			errors.New("invalid val-cal session evidence")
	}
	blindRaw, err := readPipelineEvidence(final.BlindTestSessions, maxPipelineMetadataBytes)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil, err
	}
	blind, err := eval.ParseSessionSet(blindRaw)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil,
			errors.New("invalid blind-test session evidence")
	}
	if err := validateFinalManifestBlindExtension(finalAdditions, base, blind); err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil, err
	}
	sealedRaw, err := readPipelineEvidence(final.SealedCalibrations, maxPipelineMetadataBytes)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil, err
	}
	sealed, err := eval.ParseCalibrationSet(sealedRaw)
	if err != nil {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil,
			errors.New("invalid sealed calibration evidence")
	}
	if adapter.CalibrationSHA256 != final.SealedCalibrations.SHA256 {
		return eval.SessionSet{}, eval.SessionSet{}, nil, nil,
			errors.New("adapter calibration digest does not match sealed calibration evidence")
	}
	return valCal, blind, &sealed, sealedRaw, nil
}

func readPipelinePredictions(
	ref protocol.ModelEvidenceReference,
	profile eval.Profile,
) ([]eval.Prediction, []byte, error) {
	raw, err := readPipelineEvidence(ref, maxPipelineEvidenceBytes)
	if err != nil {
		return nil, nil, err
	}
	predictions, err := eval.ReadPredictions(bytes.NewReader(raw), profile)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s predictions: %w", profile, err)
	}
	return predictions, raw, nil
}

func validatePipelineIdentities(
	manifest, trainingManifest dataset.Manifest,
	trainingReadiness dataset.Readiness,
	training protocol.ModelTrainingRunMetadata,
	adapter protocol.ModelAdapterMetadata,
) error {
	if manifest.Contract != training.Contract || trainingManifest.Contract != training.Contract ||
		training.Contract != adapter.Contract ||
		training.DatasetSHA256 != digestHex(trainingManifest.DatasetDigest) ||
		training.ReadinessStatus != trainingReadinessStatus(trainingReadiness) ||
		adapter.TrainingRunID != training.TrainingRunID ||
		adapter.BaseModelRepo != training.BaseModelRepo ||
		adapter.BaseModelRevision != training.BaseModelRevision {
		return errors.New("pipeline identities do not match")
	}
	return nil
}

func validateFinalManifestExtension(
	training, final dataset.Manifest,
) (map[protocol.ULID]struct{}, error) {
	if training.Readiness == nil || final.Readiness == nil ||
		training.Readiness.Thresholds != final.Readiness.Thresholds {
		return nil, errors.New("training and final manifests use different readiness policy")
	}
	trainingEnvelope := training
	finalEnvelope := final
	for _, manifest := range []*dataset.Manifest{&trainingEnvelope, &finalEnvelope} {
		manifest.Entries = nil
		manifest.Counts = dataset.ManifestCounts{}
		manifest.ObservationCounts = nil
		manifest.Readiness = nil
		manifest.DatasetDigest = ""
	}
	if !reflect.DeepEqual(trainingEnvelope, finalEnvelope) {
		return nil, errors.New("final manifest changes the training manifest envelope")
	}
	finalEntries := make(map[protocol.ULID]dataset.ManifestEntry, len(final.Entries))
	for _, entry := range final.Entries {
		finalEntries[entry.WindowID] = entry
	}
	trainingEntries := make(map[protocol.ULID]struct{}, len(training.Entries))
	for _, entry := range training.Entries {
		finalEntry, ok := finalEntries[entry.WindowID]
		if !ok || !reflect.DeepEqual(entry, finalEntry) {
			return nil, errors.New("final manifest removes or changes a training window")
		}
		trainingEntries[entry.WindowID] = struct{}{}
	}
	added := make(map[protocol.ULID]struct{})
	for _, entry := range final.Entries {
		if _, exists := trainingEntries[entry.WindowID]; exists {
			continue
		}
		if entry.Split != dataset.SplitTest {
			return nil, errors.New("final manifest adds a non-test window")
		}
		added[entry.WindowID] = struct{}{}
	}
	if len(added) == 0 {
		return nil, errors.New("final manifest does not add blind-test windows")
	}
	return added, nil
}

func validateFinalManifestBlindExtension(
	added map[protocol.ULID]struct{},
	base []eval.Prediction,
	blind eval.SessionSet,
) error {
	predictions := make(map[protocol.ULID]eval.Prediction, len(base))
	for _, prediction := range base {
		predictions[prediction.WindowID] = prediction
		_, isAdded := added[prediction.WindowID]
		if blind.Contains(prediction.SessionID) != isAdded {
			return errors.New("final manifest extension does not exactly match blind-test sessions")
		}
	}
	for id := range added {
		prediction, ok := predictions[id]
		if !ok || prediction.Split != dataset.SplitTest || !blind.Contains(prediction.SessionID) {
			return errors.New("final manifest extension is not covered by blind-test predictions")
		}
	}
	return nil
}

func validatePredictionIdentities(
	base, candidate, safetyBase, safetyCandidate []eval.Prediction,
	training protocol.ModelTrainingRunMetadata,
	adapter protocol.ModelAdapterMetadata,
) error {
	for _, group := range []struct {
		predictions []eval.Prediction
		candidate   bool
	}{
		{predictions: base},
		{predictions: candidate, candidate: true},
		{predictions: safetyBase},
		{predictions: safetyCandidate, candidate: true},
	} {
		for _, prediction := range group.predictions {
			run := prediction.Run
			if !prediction.RunRecorded || run.PromptVersion != adapter.PromptVersion ||
				run.BaseModelRepo != training.BaseModelRepo ||
				run.BaseModelRevision != training.BaseModelRevision || run.Runtime != adapter.Runtime ||
				run.Quantization != adapter.Quantization || strings.TrimSpace(run.RuntimeVersion) == "" ||
				strings.TrimSpace(run.GPU) == "" || run.PredictConcurrency < 1 {
				return errors.New("prediction serving identity does not match adapter metadata")
			}
			if group.candidate {
				if !prediction.AdapterRecorded || run.AdapterName != adapter.AdapterName ||
					run.AdapterRevision != adapter.AdapterRevision || run.AdapterSHA256 != adapter.AdapterSHA256 ||
					run.TrainingRunID != adapter.TrainingRunID {
					return errors.New("candidate predictions do not bind the imported adapter")
				}
			} else if prediction.AdapterRecorded {
				return errors.New("base predictions unexpectedly bind an adapter")
			}
		}
	}
	baseTest, ok := onePredictionRun(base, dataset.SplitTest)
	if !ok || !allPredictionRun(safetyBase, baseTest) {
		return errors.New("base safety predictions use another serving identity")
	}
	candidateTest, ok := onePredictionRun(candidate, dataset.SplitTest)
	if !ok || !allPredictionRun(safetyCandidate, candidateTest) {
		return errors.New("candidate safety predictions use another serving identity")
	}
	return nil
}

func onePredictionRun(predictions []eval.Prediction, split dataset.Split) (eval.RunProvenance, bool) {
	var run eval.RunProvenance
	found := false
	for _, prediction := range predictions {
		if prediction.Split != split || !prediction.RunRecorded {
			continue
		}
		if found && prediction.Run != run {
			return eval.RunProvenance{}, false
		}
		run, found = prediction.Run, true
	}
	return run, found
}

func allPredictionRun(predictions []eval.Prediction, want eval.RunProvenance) bool {
	if len(predictions) == 0 {
		return false
	}
	for _, prediction := range predictions {
		if !prediction.RunRecorded || prediction.Run != want {
			return false
		}
	}
	return true
}

func readinessStatus(readiness dataset.Readiness) string {
	if readiness.Passed {
		return gateStatusPassed
	}
	return gateStatusFailed
}

func trainingReadinessStatus(readiness dataset.Readiness) string {
	if readiness.Passed {
		return gateStatusPassed
	}
	return trainingUnderpowered
}

// trainingReadinessAllowsPendingTestCoverage distinguishes a dataset that was
// safe and sufficiently covered for training from the final dataset that must
// also close the blind-test gate. The original readiness result remains
// underpowered and is persisted unchanged; only test coverage faults may be
// deferred until the sealed final manifest is evaluated.
func trainingReadinessAllowsPendingTestCoverage(readiness dataset.Readiness) bool {
	for _, fault := range readiness.Faults {
		if fault.Split != dataset.SplitTest {
			return false
		}
		switch fault.Rule {
		case dataset.RuleSplitSize,
			dataset.RuleLabelTest,
			dataset.RuleLabelHarnesses,
			dataset.RuleHarnessInTest:
		default:
			return false
		}
	}
	return readiness.Passed == (len(readiness.Faults) == 0)
}

func pipelineGateStatus(
	readiness dataset.Readiness,
	trainingReadiness dataset.Readiness,
	training protocol.ModelTrainingRunMetadata,
	report eval.Report,
) string {
	if !readiness.Passed || !trainingReadinessAllowsPendingTestCoverage(trainingReadiness) ||
		training.Status == gateStatusFailed || training.Status == "aborted" ||
		report.Gate.Verdict == eval.VerdictFail {
		return gateStatusFailed
	}
	if training.Status != trainingStatusComplete ||
		report.Gate.Verdict != eval.VerdictPass {
		return "pending"
	}
	return gateStatusPassed
}

func persistPipelineImport(
	ctx context.Context,
	tx *sql.Tx,
	plan pipelineImport,
	now time.Time,
) error {
	labels, harnesses := pipelineManifestCounts(plan.manifest)
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("encode dataset label counts: %w", err)
	}
	harnessesJSON, err := json.Marshal(harnesses)
	if err != nil {
		return fmt.Errorf("encode dataset harness counts: %w", err)
	}
	splitStrategy := "split-v1"
	if plan.manifest.SplitFile != nil {
		splitStrategy = "split-file:" + string(*plan.manifest.SplitFile)
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO dataset_versions (
		dataset_version, ml_contract_version, manifest_path, dataset_sha256, window_count,
		counts_by_label_json, counts_by_harness_json, split_strategy, redaction_policy_version,
		readiness_status, readiness_json, created_at, manifest_sha256
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		plan.training.DatasetVersion, plan.training.MLContractVersion, plan.request.Manifest.Path,
		plan.manifest.DatasetDigest, plan.manifest.Counts.Windows, labelsJSON, harnessesJSON,
		splitStrategy, plan.manifest.Policies.RedactionPolicyVersion, readinessStatus(plan.readiness),
		plan.readinessJSON, eventstore.FormatTime(now), plan.manifestSHA256)
	if err != nil {
		return fmt.Errorf("insert dataset evidence: %w", err)
	}
	if !sameDatasetImport(ctx, tx, plan, string(labelsJSON), string(harnessesJSON), splitStrategy) {
		return ErrRegistryInvalid
	}

	endedAt := any(nil)
	if plan.training.EndedAt != nil {
		endedAt = eventstore.FormatTime(plan.training.EndedAt.UTC())
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO training_runs (
		training_run_id, dataset_version, training_dataset_sha256, training_manifest_path,
		training_manifest_sha256, base_model_repo, base_model_revision, method,
		hyperparameters_json, seed, training_code_revision, gpu, readiness_status,
		started_at, ended_at, status, log_path, metadata_path, metadata_sha256
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		plan.training.TrainingRunID, plan.training.DatasetVersion, plan.training.DatasetSHA256,
		plan.trainingManifestPath, plan.trainingManifestSHA256, plan.training.BaseModelRepo,
		plan.training.BaseModelRevision, plan.training.Method, string(plan.training.Hyperparameters),
		plan.training.Seed, plan.training.TrainingCodeRevision, nullString(plan.training.GPU),
		plan.training.ReadinessStatus, eventstore.FormatTime(plan.training.StartedAt.UTC()), endedAt,
		plan.training.Status, nullString(plan.training.LogPath), plan.trainingPath, plan.trainingSHA256)
	if err != nil {
		return fmt.Errorf("insert training evidence: %w", err)
	}
	if err := reconcileTrainingImport(ctx, tx, plan); err != nil {
		return err
	}
	if err := persistTrainingEvidence(ctx, tx, plan, now); err != nil {
		return err
	}
	if err := persistImportedAdapter(ctx, tx, plan); err != nil {
		return err
	}
	if err := persistEvaluationRows(ctx, tx, plan, now); err != nil {
		return err
	}
	return nil
}

func pipelineManifestCounts(manifest dataset.Manifest) (map[string]int64, map[string]int64) {
	labels := map[string]int64{
		"A": manifest.Counts.ByLabel.A, "B": manifest.Counts.ByLabel.B,
		"C": manifest.Counts.ByLabel.C, "D": manifest.Counts.ByLabel.D,
		"E": manifest.Counts.ByLabel.E, "F": manifest.Counts.ByLabel.F,
		"G": manifest.Counts.ByLabel.G,
	}
	harnesses := map[string]int64{}
	for _, cell := range manifest.Counts.BySplitLabelHarness {
		harnesses[string(cell.Harness)] += cell.N
	}
	return labels, harnesses
}

func sameDatasetImport(
	ctx context.Context,
	tx *sql.Tx,
	plan pipelineImport,
	labelsJSON, harnessesJSON, splitStrategy string,
) bool {
	var contract, path, datasetSHA, labels, harnesses, split, redaction, status, readiness string
	var manifestSHA sql.NullString
	var windows int64
	err := tx.QueryRowContext(ctx, `SELECT ml_contract_version, manifest_path, dataset_sha256,
		window_count, counts_by_label_json, counts_by_harness_json, split_strategy,
		redaction_policy_version, readiness_status, readiness_json, manifest_sha256
		FROM dataset_versions WHERE dataset_version = ?`, plan.training.DatasetVersion).Scan(
		&contract, &path, &datasetSHA, &windows, &labels, &harnesses, &split,
		&redaction, &status, &readiness, &manifestSHA)
	return err == nil && contract == plan.training.MLContractVersion && path == plan.request.Manifest.Path &&
		datasetSHA == string(plan.manifest.DatasetDigest) && windows == plan.manifest.Counts.Windows &&
		labels == labelsJSON && harnesses == harnessesJSON && split == splitStrategy &&
		redaction == plan.manifest.Policies.RedactionPolicyVersion && status == readinessStatus(plan.readiness) &&
		readiness == string(plan.readinessJSON) && manifestSHA.Valid &&
		manifestSHA.String == string(plan.manifestSHA256)
}

func reconcileTrainingImport(ctx context.Context, tx *sql.Tx, plan pipelineImport) error {
	var datasetVersion, repo, revision, method, hyperparameters, codeRevision, readiness, started, status string
	var seed int64
	var trainingDatasetSHA, trainingManifestPath, trainingManifestSHA sql.NullString
	var gpu, ended, logPath, metadataPath, metadataSHA sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT dataset_version, training_dataset_sha256,
		training_manifest_path, training_manifest_sha256, base_model_repo, base_model_revision,
		method, hyperparameters_json, seed, training_code_revision, gpu, readiness_status,
		started_at, ended_at, status, log_path, metadata_path, metadata_sha256
		FROM training_runs WHERE training_run_id = ?`, plan.training.TrainingRunID).Scan(
		&datasetVersion, &trainingDatasetSHA, &trainingManifestPath, &trainingManifestSHA,
		&repo, &revision, &method, &hyperparameters, &seed, &codeRevision,
		&gpu, &readiness, &started, &ended, &status, &logPath, &metadataPath, &metadataSHA)
	if err != nil || datasetVersion != plan.training.DatasetVersion || repo != plan.training.BaseModelRepo ||
		!trainingDatasetSHA.Valid || trainingDatasetSHA.String != string(plan.training.DatasetSHA256) ||
		!trainingManifestPath.Valid || trainingManifestPath.String != plan.trainingManifestPath ||
		!trainingManifestSHA.Valid || trainingManifestSHA.String != string(plan.trainingManifestSHA256) ||
		revision != plan.training.BaseModelRevision || method != plan.training.Method ||
		hyperparameters != string(plan.training.Hyperparameters) || seed != plan.training.Seed ||
		codeRevision != plan.training.TrainingCodeRevision || !sameOptional(gpu, plan.training.GPU) ||
		readiness != plan.training.ReadinessStatus ||
		started != eventstore.FormatTime(plan.training.StartedAt.UTC()) ||
		!sameOptional(logPath, plan.training.LogPath) || !metadataPath.Valid || !metadataSHA.Valid {
		return ErrRegistryInvalid
	}
	wantEnded := ""
	if plan.training.EndedAt != nil {
		wantEnded = eventstore.FormatTime(plan.training.EndedAt.UTC())
	}
	exactEnded := plan.training.EndedAt == nil && !ended.Valid ||
		plan.training.EndedAt != nil && ended.Valid && ended.String == wantEnded
	if status == plan.training.Status && exactEnded && metadataPath.String == plan.trainingPath &&
		metadataSHA.String == string(plan.trainingSHA256) {
		return nil
	}
	if status != trainingStatusRunning || ended.Valid || plan.training.Status == trainingStatusRunning ||
		plan.training.EndedAt == nil {
		return ErrRegistryInvalid
	}
	result, err := tx.ExecContext(ctx, `UPDATE training_runs
		SET ended_at = ?, status = ?, metadata_path = ?, metadata_sha256 = ?
		WHERE training_run_id = ? AND status = 'running' AND ended_at IS NULL
			AND metadata_path = ? AND metadata_sha256 = ?`,
		wantEnded, plan.training.Status, plan.trainingPath, plan.trainingSHA256,
		plan.training.TrainingRunID, metadataPath.String, metadataSHA.String)
	if err != nil {
		return fmt.Errorf("finish imported training: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrRegistryInvalid
	}
	return nil
}

func persistTrainingEvidence(
	ctx context.Context,
	tx *sql.Tx,
	plan pipelineImport,
	now time.Time,
) error {
	endedAt := any(nil)
	if plan.training.EndedAt != nil {
		endedAt = eventstore.FormatTime(plan.training.EndedAt.UTC())
	}
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO training_run_evidence (
		training_run_id, metadata_sha256, metadata_path, status, ended_at, imported_at
	) VALUES (?, ?, ?, ?, ?, ?)`, plan.training.TrainingRunID, plan.trainingSHA256,
		plan.trainingPath, plan.training.Status, endedAt, eventstore.FormatTime(now))
	if err != nil {
		return fmt.Errorf("insert training evidence history: %w", err)
	}
	var path, status string
	var ended sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT metadata_path, status, ended_at
		FROM training_run_evidence WHERE training_run_id = ? AND metadata_sha256 = ?`,
		plan.training.TrainingRunID, plan.trainingSHA256).Scan(&path, &status, &ended)
	if err != nil || path != plan.trainingPath || status != plan.training.Status {
		return ErrRegistryInvalid
	}
	if plan.training.EndedAt == nil {
		if ended.Valid {
			return ErrRegistryInvalid
		}
		return nil
	}
	if !ended.Valid || ended.String != eventstore.FormatTime(plan.training.EndedAt.UTC()) {
		return ErrRegistryInvalid
	}
	return nil
}

func sameOptional(stored sql.NullString, want string) bool {
	if want == "" {
		return !stored.Valid
	}
	return stored.Valid && stored.String == want
}

func persistImportedAdapter(ctx context.Context, tx *sql.Tx, plan pipelineImport) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO model_adapters (
		adapter_name, adapter_revision, adapter_sha256, training_run_id, contract,
		gate_status, active, metadata_path, metadata_sha256
	) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`, plan.adapter.AdapterName,
		plan.adapter.AdapterRevision, plan.adapter.AdapterSHA256, plan.adapter.TrainingRunID,
		plan.adapter.Contract, plan.gateStatus, plan.adapterPath, plan.adapterSHA256)
	if err != nil {
		return fmt.Errorf("insert adapter evidence: %w", err)
	}
	var adapterSHA, trainingRun, contract, gate string
	var metadataPath, metadataSHA sql.NullString
	var active int64
	err = tx.QueryRowContext(ctx, `SELECT adapter_sha256, training_run_id, contract,
		gate_status, active, metadata_path, metadata_sha256 FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, plan.adapter.AdapterName,
		plan.adapter.AdapterRevision).Scan(&adapterSHA, &trainingRun, &contract, &gate,
		&active, &metadataPath, &metadataSHA)
	if err != nil || adapterSHA != string(plan.adapter.AdapterSHA256) ||
		trainingRun != string(plan.adapter.TrainingRunID) || contract != string(plan.adapter.Contract) ||
		!metadataPath.Valid || metadataPath.String != plan.adapterPath || !metadataSHA.Valid ||
		metadataSHA.String != string(plan.adapterSHA256) {
		return ErrRegistryInvalid
	}
	if plan.gateStatus == gateStatusPassed && gate != gateStatusPassed {
		return passImportedAdapterGate(ctx, tx, plan, active)
	}
	if plan.gateStatus == gateStatusFailed && gate == "pending" {
		_, err = tx.ExecContext(ctx, `UPDATE model_adapters SET gate_status = 'failed'
			WHERE adapter_name = ? AND adapter_revision = ? AND active = 0`,
			plan.adapter.AdapterName, plan.adapter.AdapterRevision)
		if err != nil {
			return fmt.Errorf("fail imported adapter gate: %w", err)
		}
	}
	return nil
}

func passImportedAdapterGate(ctx context.Context, tx *sql.Tx, plan pipelineImport, active int64) error {
	if active != 0 {
		return ErrRegistryInvalid
	}
	result, err := tx.ExecContext(ctx, `UPDATE model_adapters SET gate_status = 'passed'
		WHERE adapter_name = ? AND adapter_revision = ? AND active = 0`,
		plan.adapter.AdapterName, plan.adapter.AdapterRevision)
	if err != nil {
		return fmt.Errorf("pass imported adapter gate: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrRegistryInvalid
	}
	return nil
}

type evaluationImportRow struct {
	id        string
	profile   eval.Profile
	adapter   string
	split     string
	metrics   eval.Metrics
	gate      []byte
	report    []byte
	reportSHA protocol.SHA256Hex
	evidence  []byte
}

func persistEvaluationRows(ctx context.Context, tx *sql.Tx, plan pipelineImport, now time.Time) error {
	base, lora, ok := reportProfiles(plan.report)
	if !ok {
		return ErrRegistryInvalid
	}
	gateJSON, err := json.Marshal(plan.report.Gate)
	if err != nil {
		return fmt.Errorf("encode evaluation gate: %w", err)
	}
	root := string(plan.request.EvaluationRunID)
	rows := []evaluationImportRow{
		{id: relatedEvaluationID(root, "base-validation"), profile: eval.ProfileBase, split: "validation", metrics: base.Validation.All},
		{id: relatedEvaluationID(root, "base-test"), profile: eval.ProfileBase, split: evaluationSplitTest, metrics: base.Test.All},
		{id: relatedEvaluationID(root, "lora-validation"), profile: eval.ProfileLoRA, adapter: plan.adapter.AdapterRevision, split: "validation", metrics: lora.Validation.All},
		{
			id: root, profile: eval.ProfileLoRA, adapter: plan.adapter.AdapterRevision, split: evaluationSplitTest, metrics: lora.Test.All,
			gate: gateJSON, report: plan.reportJSON, reportSHA: plan.registration.EvaluationSHA256, evidence: plan.evidenceJSON,
		},
	}
	for _, row := range rows {
		if !protocol.OpaqueID(row.id).Valid() {
			return ErrRegistryInvalid
		}
		metricsJSON, marshalErr := json.Marshal(row.metrics)
		if marshalErr != nil {
			return fmt.Errorf("encode evaluation metrics: %w", marshalErr)
		}
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO evaluation_runs (
			evaluation_run_id, dataset_version, inference_profile, adapter_revision, split,
			metrics_json, gate_verdict_json, created_at, evaluation_sha256,
			evaluation_report_json, evidence_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, row.id, plan.training.DatasetVersion,
			row.profile, nullString(row.adapter), row.split, metricsJSON, nullableBytes(row.gate),
			eventstore.FormatTime(now), nullString(string(row.reportSHA)), nullableBytes(row.report),
			nullableBytes(row.evidence))
		if err != nil {
			return fmt.Errorf("insert evaluation evidence: %w", err)
		}
		if !sameEvaluationRow(ctx, tx, plan, row, string(metricsJSON)) {
			return ErrRegistryInvalid
		}
	}
	return nil
}

func relatedEvaluationID(root, role string) string {
	return "evaluation-" + string(digestBytes([]byte("gifford.model.evaluation.v1\x00"+root+"\x00"+role)))
}

func nullableBytes(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func sameEvaluationRow(
	ctx context.Context,
	tx *sql.Tx,
	plan pipelineImport,
	row evaluationImportRow,
	metricsJSON string,
) bool {
	var datasetVersion, profile, split, metrics string
	var adapter, gate, reportSHA, report, evidence sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT dataset_version, inference_profile, adapter_revision,
		split, metrics_json, gate_verdict_json, evaluation_sha256,
		evaluation_report_json, evidence_json FROM evaluation_runs WHERE evaluation_run_id = ?`,
		row.id).Scan(&datasetVersion, &profile, &adapter, &split, &metrics, &gate,
		&reportSHA, &report, &evidence)
	return err == nil && datasetVersion == plan.training.DatasetVersion && profile == string(row.profile) &&
		sameOptional(adapter, row.adapter) && split == row.split && metrics == metricsJSON &&
		sameOptional(gate, string(row.gate)) && sameOptional(reportSHA, string(row.reportSHA)) &&
		sameOptional(report, string(row.report)) && sameOptional(evidence, string(row.evidence))
}
