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
	request        protocol.RegisterModelPipelineRequest
	registration   protocol.RegisterModelMetadataRequest
	manifest       dataset.Manifest
	manifestSHA256 protocol.SHA256Hex
	readiness      dataset.Readiness
	readinessJSON  []byte
	training       protocol.ModelTrainingRunMetadata
	trainingPath   string
	trainingSHA256 protocol.SHA256Hex
	adapter        protocol.ModelAdapterMetadata
	adapterPath    string
	adapterSHA256  protocol.SHA256Hex
	report         eval.Report
	reportJSON     []byte
	evidenceJSON   []byte
	gateStatus     string
	activatable    bool
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
	manifestRaw, err := readPipelineEvidence(req.Manifest, maxPipelineEvidenceBytes)
	if err != nil {
		return pipelineImport{}, err
	}
	manifest, err := dataset.ParseManifest(bytes.NewReader(manifestRaw))
	if err != nil || manifest.Readiness == nil {
		return pipelineImport{}, errors.New("manifest has no valid readiness policy")
	}
	readiness := dataset.CheckReadiness(manifest.Entries, manifest.Readiness.Thresholds)
	if !reflect.DeepEqual(readiness, *manifest.Readiness) {
		return pipelineImport{}, errors.New("manifest readiness does not recompute")
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
	if err := validatePipelineIdentities(manifest, readiness, training, adapter); err != nil {
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
	report, err := eval.Evaluate(eval.Options{
		Base: base, LoRA: candidate, BaseRaw: baseRaw, LoRARaw: candidateRaw,
		SafetyBase: safetyBase, SafetyCandidate: safetyCandidate,
		SafetyBaseRaw: safetyBaseRaw, SafetyCandidateRaw: safetyCandidateRaw,
		Manifest:       eval.ManifestCheck{Given: true, Manifest: manifest},
		SafetyManifest: eval.ManifestCheck{Given: true, Manifest: safetyManifest},
		Bins:           eval.DefaultBins, LatencyBudget: eval.DefaultLatencyBudget,
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
	activatable := training.ReadinessStatus == gateStatusPassed && training.Status == trainingStatusComplete &&
		report.Gate.Verdict == eval.VerdictPass
	if activatable {
		_, err = validateRegistration(registration)
		activatable = err == nil
	}
	return pipelineImport{
		request: req, registration: registration, manifest: manifest,
		manifestSHA256: req.Manifest.SHA256, readiness: readiness, readinessJSON: readinessJSON,
		training: training, trainingPath: req.TrainingRunMetadata.Path,
		trainingSHA256: req.TrainingRunMetadata.SHA256,
		adapter:        adapter, adapterPath: req.AdapterMetadata.Path,
		adapterSHA256: req.AdapterMetadata.SHA256,
		report:        report, reportJSON: reportJSON, evidenceJSON: evidenceJSON,
		gateStatus: pipelineGateStatus(readiness, training, report), activatable: activatable,
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
	manifest dataset.Manifest,
	readiness dataset.Readiness,
	training protocol.ModelTrainingRunMetadata,
	adapter protocol.ModelAdapterMetadata,
) error {
	if manifest.Contract != training.Contract || training.Contract != adapter.Contract ||
		training.DatasetSHA256 != digestHex(manifest.DatasetDigest) ||
		training.ReadinessStatus != trainingReadinessStatus(readiness) ||
		adapter.TrainingRunID != training.TrainingRunID ||
		adapter.BaseModelRepo != training.BaseModelRepo ||
		adapter.BaseModelRevision != training.BaseModelRevision {
		return errors.New("pipeline identities do not match")
	}
	return nil
}

func validatePredictionIdentities(
	base, candidate, safetyBase, safetyCandidate []eval.Prediction,
	training protocol.ModelTrainingRunMetadata,
	adapter protocol.ModelAdapterMetadata,
) error {
	for _, predictions := range [][]eval.Prediction{base, candidate, safetyBase, safetyCandidate} {
		for _, prediction := range predictions {
			run := prediction.Run
			if !prediction.RunRecorded || run.PromptVersion != adapter.PromptVersion ||
				run.BaseModelRepo != training.BaseModelRepo ||
				run.BaseModelRevision != training.BaseModelRevision || run.Runtime != adapter.Runtime ||
				run.Quantization != adapter.Quantization || strings.TrimSpace(run.RuntimeVersion) == "" ||
				strings.TrimSpace(run.GPU) == "" || run.PredictConcurrency < 1 {
				return errors.New("prediction serving identity does not match adapter metadata")
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

func pipelineGateStatus(
	readiness dataset.Readiness,
	training protocol.ModelTrainingRunMetadata,
	report eval.Report,
) string {
	if !readiness.Passed || training.Status == gateStatusFailed || training.Status == "aborted" ||
		report.Gate.Verdict == eval.VerdictFail {
		return gateStatusFailed
	}
	if training.ReadinessStatus != gateStatusPassed || training.Status != trainingStatusComplete ||
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
		training_run_id, dataset_version, base_model_repo, base_model_revision, method,
		hyperparameters_json, seed, training_code_revision, gpu, readiness_status,
		started_at, ended_at, status, log_path, metadata_path, metadata_sha256
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		plan.training.TrainingRunID, plan.training.DatasetVersion, plan.training.BaseModelRepo,
		plan.training.BaseModelRevision, plan.training.Method, string(plan.training.Hyperparameters),
		plan.training.Seed, plan.training.TrainingCodeRevision, nullString(plan.training.GPU),
		plan.training.ReadinessStatus, eventstore.FormatTime(plan.training.StartedAt.UTC()), endedAt,
		plan.training.Status, nullString(plan.training.LogPath), plan.trainingPath, plan.trainingSHA256)
	if err != nil {
		return fmt.Errorf("insert training evidence: %w", err)
	}
	if !sameTrainingImport(ctx, tx, plan) {
		return ErrRegistryInvalid
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

func sameTrainingImport(ctx context.Context, tx *sql.Tx, plan pipelineImport) bool {
	var datasetVersion, repo, revision, method, hyperparameters, codeRevision, readiness, started, status string
	var seed int64
	var gpu, ended, logPath, metadataPath, metadataSHA sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT dataset_version, base_model_repo, base_model_revision,
		method, hyperparameters_json, seed, training_code_revision, gpu, readiness_status,
		started_at, ended_at, status, log_path, metadata_path, metadata_sha256
		FROM training_runs WHERE training_run_id = ?`, plan.training.TrainingRunID).Scan(
		&datasetVersion, &repo, &revision, &method, &hyperparameters, &seed, &codeRevision,
		&gpu, &readiness, &started, &ended, &status, &logPath, &metadataPath, &metadataSHA)
	if err != nil || datasetVersion != plan.training.DatasetVersion || repo != plan.training.BaseModelRepo ||
		revision != plan.training.BaseModelRevision || method != plan.training.Method ||
		hyperparameters != string(plan.training.Hyperparameters) || seed != plan.training.Seed ||
		codeRevision != plan.training.TrainingCodeRevision || !sameOptional(gpu, plan.training.GPU) ||
		readiness != plan.training.ReadinessStatus ||
		started != eventstore.FormatTime(plan.training.StartedAt.UTC()) || status != plan.training.Status ||
		!sameOptional(logPath, plan.training.LogPath) || !metadataPath.Valid ||
		metadataPath.String != plan.trainingPath || !metadataSHA.Valid ||
		metadataSHA.String != string(plan.trainingSHA256) {
		return false
	}
	if plan.training.EndedAt == nil {
		return !ended.Valid
	}
	return ended.Valid && ended.String == eventstore.FormatTime(plan.training.EndedAt.UTC())
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
