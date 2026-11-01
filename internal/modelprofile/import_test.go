package modelprofile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

type pipelineTestBundle struct {
	request protocol.RegisterModelPipelineRequest
	report  eval.Report
}

type pipelineTestOptions struct {
	failSafety     bool
	underpowered   bool
	trainingStatus string
}

type pipelineTestPrediction struct {
	WindowID               protocol.ULID             `json:"window_id"`
	Split                  dataset.Split             `json:"split"`
	Harness                protocol.Harness          `json:"harness"`
	HarnessVersion         string                    `json:"harness_version"`
	SessionID              protocol.OpaqueID         `json:"session_id"`
	SourceQuality          protocol.SourceQuality    `json:"source_quality"`
	Label                  dataset.Label             `json:"label"`
	Probabilities          map[dataset.Label]float64 `json:"probabilities"`
	ValidMass              float64                   `json:"valid_mass"`
	Invalid                bool                      `json:"invalid"`
	LatencyMS              float64                   `json:"latency_ms"`
	Profile                eval.Profile              `json:"profile"`
	PromptVersion          string                    `json:"prompt_version"`
	BaseModelRepo          string                    `json:"base_model_repo"`
	BaseModelRevision      string                    `json:"base_model_revision"`
	Runtime                string                    `json:"runtime"`
	RuntimeVersion         string                    `json:"runtime_version"`
	Quantization           string                    `json:"quantization"`
	GPU                    string                    `json:"gpu"`
	PredictConcurrency     int                       `json:"predict_concurrency"`
	AdapterName            string                    `json:"adapter_name,omitempty"`
	AdapterRevision        string                    `json:"adapter_revision,omitempty"`
	AdapterSHA256          protocol.SHA256Hex        `json:"adapter_sha256,omitempty"`
	TrainingRunID          protocol.OpaqueID         `json:"training_run_id,omitempty"`
	SecondsSinceLastOutput float64                   `json:"seconds_since_last_output"`
}

const (
	pipelineValCalSession     = "pipeline-val-cal-session"
	pipelineValCmpSession     = "pipeline-val-cmp-session"
	pipelineBlindSession      = "pipeline-blind-session"
	pipelineHistoricalSession = "pipeline-historical-session"
)

func TestRegisterPipelineImportsComputedEvidenceOnCleanDatabase(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writePipelineTestBundle(t, fixture, pipelineTestOptions{})

	registered, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
	require.NoError(t, err)
	require.True(t, registered.Imported)
	require.Equal(t, testAdapterName, registered.AdapterName)
	require.Equal(t, "pipeline-rev-1", registered.AdapterRevision)

	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 1)
	var reportJSON, evidenceJSON, evaluationSHA string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		evaluation_report_json, evidence_json, evaluation_sha256
		FROM evaluation_runs WHERE evaluation_run_id = ?`, bundle.request.EvaluationRunID).Scan(
		&reportJSON, &evidenceJSON, &evaluationSHA))
	require.NotEmpty(t, evidenceJSON)
	require.Equal(t, string(digestBytes([]byte(reportJSON))), evaluationSHA)
	var stored eval.Report
	require.NoError(t, json.Unmarshal([]byte(reportJSON), &stored))
	require.Equal(t, bundle.report, stored)
	require.Equal(t, eval.VerdictPass, stored.Gate.Verdict)
	var trainingDatasetSHA, trainingManifestPath, trainingManifestSHA string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		training_dataset_sha256, training_manifest_path, training_manifest_sha256
		FROM training_runs WHERE training_run_id = 'pipeline-training-1'`).Scan(
		&trainingDatasetSHA, &trainingManifestPath, &trainingManifestSHA))
	require.NotEqual(t, digestHex(stored.Dataset.Digest), trainingDatasetSHA)
	require.Equal(t, bundle.request.FinalEvaluation.TrainingManifest.Path, trainingManifestPath)
	require.Equal(t, string(bundle.request.FinalEvaluation.TrainingManifest.SHA256), trainingManifestSHA)

	again, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
	require.NoError(t, err)
	require.False(t, again.Imported)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 1)
}

func TestRegisterPipelineRetainsFailedEvaluationWithoutRegistrationOrSelection(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writePipelineTestBundle(t, fixture, pipelineTestOptions{failSafety: true})

	_, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrPipelineNotActivatable)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 0)

	var gate string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT gate_status
		FROM model_adapters WHERE adapter_name = 'analyst' AND adapter_revision = 'pipeline-rev-1'`).Scan(&gate))
	require.Equal(t, gateStatusFailed, gate)
	var reportJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT evaluation_report_json
		FROM evaluation_runs WHERE evaluation_run_id = ?`, bundle.request.EvaluationRunID).Scan(&reportJSON))
	var stored eval.Report
	require.NoError(t, json.Unmarshal([]byte(reportJSON), &stored))
	require.Equal(t, eval.VerdictFail, stored.Gate.Verdict)
	require.NotNil(t, stored.Safety)
	require.False(t, stored.Safety.Candidate.Passed)
	require.Equal(t, 1, stored.Safety.Candidate.Failures)

	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, int64(1), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
}

func TestRegisterPipelineRetainsPendingAndUnderpoweredEvidence(t *testing.T) {
	for _, test := range []struct {
		name        string
		options     pipelineTestOptions
		wantGate    string
		wantVerdict eval.Verdict
	}{
		{
			name:     "training still running",
			options:  pipelineTestOptions{trainingStatus: trainingStatusRunning},
			wantGate: "pending", wantVerdict: eval.VerdictPass,
		},
		{
			name:     "dataset and training underpowered",
			options:  pipelineTestOptions{underpowered: true},
			wantGate: gateStatusFailed, wantVerdict: eval.VerdictFail,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newServiceFixture(t)
			bundle := writePipelineTestBundle(t, fixture, test.options)
			require.Equal(t, test.wantVerdict, bundle.report.Gate.Verdict)

			_, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
			require.ErrorIs(t, err, ErrPipelineNotActivatable)
			assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 0)
			var gate string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT gate_status
				FROM model_adapters WHERE adapter_name = 'analyst' AND adapter_revision = 'pipeline-rev-1'`).Scan(&gate))
			require.Equal(t, test.wantGate, gate)
			selected := fixture.publisher.CurrentModelSelection()
			require.Equal(t, int64(1), selected.Generation)
			require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
		})
	}
}

func TestRegisterPipelineRejectsBadHashesAndMismatchedIdentitiesBeforeWrites(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *protocol.RegisterModelPipelineRequest)
	}{
		{
			name: "prediction hash mismatch",
			mutate: func(_ *testing.T, request *protocol.RegisterModelPipelineRequest) {
				request.BasePredictions.SHA256 = digest("f")
			},
		},
		{
			name: "adapter training identity mismatch",
			mutate: func(t *testing.T, request *protocol.RegisterModelPipelineRequest) {
				raw, err := os.ReadFile(request.AdapterMetadata.Path)
				require.NoError(t, err)
				var metadata protocol.ModelAdapterMetadata
				require.NoError(t, json.Unmarshal(raw, &metadata))
				metadata.TrainingRunID = "another-training-run"
				updated, err := json.Marshal(metadata)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(request.AdapterMetadata.Path, updated, 0o600))
				request.AdapterMetadata.SHA256 = digestBytes(updated)
			},
		},
		{
			name: "safety candidate checkpoint mismatch",
			mutate: func(t *testing.T, request *protocol.RegisterModelPipelineRequest) {
				raw, err := os.ReadFile(request.SafetyCandidatePredictions.Path)
				require.NoError(t, err)
				updated := bytes.ReplaceAll(raw,
					[]byte(`"adapter_revision":"pipeline-rev-1"`),
					[]byte(`"adapter_revision":"pipeline-rev-2"`))
				require.NotEqual(t, raw, updated)
				require.NoError(t, os.WriteFile(request.SafetyCandidatePredictions.Path, updated, 0o600))
				request.SafetyCandidatePredictions.SHA256 = digestBytes(updated)
			},
		},
		{
			name: "base predictions name an adapter",
			mutate: func(t *testing.T, request *protocol.RegisterModelPipelineRequest) {
				raw, err := os.ReadFile(request.BasePredictions.Path)
				require.NoError(t, err)
				seal := fmt.Sprintf(
					`,"adapter_name":"analyst","adapter_revision":"pipeline-rev-1","adapter_sha256":"%s","training_run_id":"pipeline-training-1","seconds_since_last_output"`,
					digest("a"),
				)
				updated := bytes.ReplaceAll(raw, []byte(`,"seconds_since_last_output"`), []byte(seal))
				require.NotEqual(t, raw, updated)
				require.NoError(t, os.WriteFile(request.BasePredictions.Path, updated, 0o600))
				request.BasePredictions.SHA256 = digestBytes(updated)
			},
		},
		{
			name: "final manifest changes a training window",
			mutate: func(t *testing.T, request *protocol.RegisterModelPipelineRequest) {
				raw, err := os.ReadFile(request.Manifest.Path)
				require.NoError(t, err)
				manifest, err := dataset.ParseManifest(bytes.NewReader(raw))
				require.NoError(t, err)
				manifest.Entries[0].Label = dataset.LabelB
				manifest.Counts = dataset.CountEntries(manifest.Entries)
				readiness := dataset.CheckReadiness(manifest.Entries, manifest.Readiness.Thresholds)
				manifest.Readiness = &readiness
				manifest.DatasetDigest = dataset.ManifestDigest(manifest)
				require.NoError(t, manifest.Validate())
				updated, err := json.Marshal(manifest)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(request.Manifest.Path, updated, 0o600))
				request.Manifest.SHA256 = digestBytes(updated)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newServiceFixture(t)
			bundle := writePipelineTestBundle(t, fixture, pipelineTestOptions{})
			test.mutate(t, &bundle.request)

			_, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
			require.ErrorIs(t, err, ErrRegistryInvalid)
			assertPipelineRowCounts(t, fixture, 0, 0, 0, 0, 0)
		})
	}
}

func TestRegisterPipelineCannotRebindPredictionsToAnotherCheckpoint(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writePipelineTestBundle(t, fixture, pipelineTestOptions{})

	registered, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
	require.NoError(t, err)
	require.True(t, registered.Imported)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 1)

	raw, err := os.ReadFile(bundle.request.AdapterMetadata.Path)
	require.NoError(t, err)
	var rebound protocol.ModelAdapterMetadata
	require.NoError(t, json.Unmarshal(raw, &rebound))
	rebound.AdapterRevision = "pipeline-rev-2"
	rebound.AdapterSHA256 = digest("c")
	reboundRef := writePipelineJSON(t, filepath.Dir(bundle.request.AdapterMetadata.Path), "adapter-rebound.json", rebound)
	reboundRequest := bundle.request
	reboundRequest.EvaluationRunID = "pipeline-evaluation-2"
	reboundRequest.AdapterMetadata = reboundRef

	_, err = fixture.service.RegisterPipeline(t.Context(), reboundRequest, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 1)
	var adapterRevision, adapterSHA string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		adapter_revision, adapter_sha256 FROM model_profile_registry`).Scan(&adapterRevision, &adapterSHA))
	require.Equal(t, "pipeline-rev-1", adapterRevision)
	require.Equal(t, string(digest("a")), adapterSHA)
	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, int64(1), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
}

func TestRegisterPipelineCanFinishRetainedRunningTraining(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writePipelineTestBundle(t, fixture, pipelineTestOptions{trainingStatus: trainingStatusRunning})

	_, err := fixture.service.RegisterPipeline(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrPipelineNotActivatable)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 4, 0)

	raw, err := os.ReadFile(bundle.request.TrainingRunMetadata.Path)
	require.NoError(t, err)
	var completed protocol.ModelTrainingRunMetadata
	require.NoError(t, json.Unmarshal(raw, &completed))
	endedAt := fixture.clock.now.Add(-time.Hour)
	completed.Status = trainingStatusComplete
	completed.EndedAt = &endedAt
	completedRef := writePipelineJSON(t, filepath.Dir(bundle.request.TrainingRunMetadata.Path), "training-completed.json", completed)
	completedRequest := bundle.request
	completedRequest.EvaluationRunID = "pipeline-evaluation-2"
	completedRequest.TrainingRunMetadata = completedRef

	registered, err := fixture.service.RegisterPipeline(t.Context(), completedRequest, operator())
	require.NoError(t, err)
	require.True(t, registered.Imported)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 8, 1)
	var status, ended, metadataPath, metadataSHA string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		status, ended_at, metadata_path, metadata_sha256 FROM training_runs
		WHERE training_run_id = ?`, completed.TrainingRunID).Scan(&status, &ended, &metadataPath, &metadataSHA))
	require.Equal(t, trainingStatusComplete, status)
	require.Equal(t, eventstore.FormatTime(endedAt.UTC()), ended)
	require.Equal(t, completedRef.Path, metadataPath)
	require.Equal(t, string(completedRef.SHA256), metadataSHA)
	var evidenceCount int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*)
		FROM training_run_evidence WHERE training_run_id = ?`, completed.TrainingRunID).Scan(&evidenceCount))
	require.Equal(t, int64(2), evidenceCount)

	otherEnd := endedAt.Add(time.Minute)
	completed.EndedAt = &otherEnd
	differentRef := writePipelineJSON(t, filepath.Dir(bundle.request.TrainingRunMetadata.Path), "training-completed-different.json", completed)
	differentRequest := completedRequest
	differentRequest.EvaluationRunID = "pipeline-evaluation-3"
	differentRequest.TrainingRunMetadata = differentRef
	_, err = fixture.service.RegisterPipeline(t.Context(), differentRequest, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	assertPipelineRowCounts(t, fixture, 1, 1, 1, 8, 1)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*)
		FROM training_run_evidence WHERE training_run_id = ?`, completed.TrainingRunID).Scan(&evidenceCount))
	require.Equal(t, int64(2), evidenceCount)
}

func assertPipelineRowCounts(
	t *testing.T,
	fixture *serviceFixture,
	datasets, training, adapters, evaluations, registrations int64,
) {
	t.Helper()
	for table, want := range map[string]int64{
		"dataset_versions":       datasets,
		"training_runs":          training,
		"model_adapters":         adapters,
		"evaluation_runs":        evaluations,
		"model_profile_registry": registrations,
	} {
		var got int64
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM "+table).Scan(&got))
		require.Equal(t, want, got, table)
	}
}

func writePipelineTestBundle(
	t *testing.T,
	fixture *serviceFixture,
	options pipelineTestOptions,
) pipelineTestBundle {
	t.Helper()
	dir := t.TempDir()
	manifest := pipelineTestManifest(t, options.underpowered)
	manifestRef := writePipelineJSON(t, dir, "manifest.json", manifest)
	trainingManifest := pipelineTrainingManifest(t, manifest)
	trainingManifestRef := writePipelineJSON(t, dir, "training-manifest.json", trainingManifest)

	safetyRaw, err := os.ReadFile(filepath.Join("..", "eval", "testdata", "safety", "manifest.json"))
	require.NoError(t, err)
	safetyManifest, err := dataset.ParseManifest(bytes.NewReader(safetyRaw))
	require.NoError(t, err)
	safetyRef := writePipelineBytes(t, dir, "safety-manifest.json", safetyRaw)

	run := eval.RunProvenance{
		PromptVersion: fixture.base.PromptVersion, BaseModelRepo: fixture.base.BaseModelRepo,
		BaseModelRevision: fixture.base.BaseModelRevision, Runtime: fixture.base.Runtime,
		RuntimeVersion: "test-runtime-1", Quantization: fixture.base.Quantization,
		GPU: "test-gpu", PredictConcurrency: 1,
	}
	candidateRun := run
	candidateRun.AdapterName = testAdapterName
	candidateRun.AdapterRevision = "pipeline-rev-1"
	candidateRun.AdapterSHA256 = digest("a")
	candidateRun.TrainingRunID = "pipeline-training-1"
	baseLines := ordinaryPipelinePredictions(manifest, eval.ProfileBase, run, false)
	candidateLines := ordinaryPipelinePredictions(manifest, eval.ProfileLoRA, candidateRun, true)
	safetyBaseLines := safetyPipelinePredictions(safetyManifest, eval.ProfileBase, run)
	safetyCandidateLines := safetyPipelinePredictions(safetyManifest, eval.ProfileLoRA, candidateRun)
	if options.failSafety {
		safetyCandidateLines[0].Probabilities = probabilities(dataset.LabelA, 1)
	}
	baseRef, baseRaw := writePipelineJSONL(t, dir, "base.jsonl", baseLines)
	candidateRef, candidateRaw := writePipelineJSONL(t, dir, "candidate.jsonl", candidateLines)
	safetyBaseRef, safetyBaseRaw := writePipelineJSONL(t, dir, "safety-base.jsonl", safetyBaseLines)
	safetyCandidateRef, safetyCandidateRaw := writePipelineJSONL(t, dir, "safety-candidate.jsonl", safetyCandidateLines)

	base := readPipelineTestPredictions(t, baseRaw, eval.ProfileBase)
	candidate := readPipelineTestPredictions(t, candidateRaw, eval.ProfileLoRA)
	safetyBase := readPipelineTestPredictions(t, safetyBaseRaw, eval.ProfileBase)
	safetyCandidate := readPipelineTestPredictions(t, safetyCandidateRaw, eval.ProfileLoRA)
	valCalRaw := []byte(`{"session_id":"` + pipelineValCalSession + `"}` + "\n")
	blindRaw := []byte(`{"session_id":"` + pipelineBlindSession + `"}` + "\n")
	valCal, err := eval.ParseSessionSet(valCalRaw)
	require.NoError(t, err)
	blind, err := eval.ParseSessionSet(blindRaw)
	require.NoError(t, err)
	checkpoint, err := eval.Evaluate(eval.Options{
		Base: base, LoRA: candidate, BaseRaw: baseRaw, LoRARaw: candidateRaw,
		Manifest: eval.ManifestCheck{Given: true, Manifest: manifest},
		Bins:     eval.DefaultBins, LatencyBudget: eval.DefaultLatencyBudget,
		ValidationCalibrationSessions: valCal,
	})
	require.NoError(t, err)
	sealedRaw, err := checkpoint.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := eval.ParseCalibrationSet(sealedRaw)
	require.NoError(t, err)
	valCalRef := writePipelineBytes(t, dir, "val-cal.sessions.jsonl", valCalRaw)
	blindRef := writePipelineBytes(t, dir, "test-blind.sessions.jsonl", blindRaw)
	sealedRef := writePipelineBytes(t, dir, "calibrations.json", sealedRaw)
	report, err := eval.Evaluate(eval.Options{
		Base: base, LoRA: candidate, BaseRaw: baseRaw, LoRARaw: candidateRaw,
		SafetyBase: safetyBase, SafetyCandidate: safetyCandidate,
		SafetyBaseRaw: safetyBaseRaw, SafetyCandidateRaw: safetyCandidateRaw,
		Manifest:       eval.ManifestCheck{Given: true, Manifest: manifest},
		SafetyManifest: eval.ManifestCheck{Given: true, Manifest: safetyManifest},
		Bins:           eval.DefaultBins, LatencyBudget: eval.DefaultLatencyBudget,
		ValidationCalibrationSessions: valCal, BlindTestSessions: blind,
		SealedCalibrations: &sealed, SealedCalibrationsRaw: sealedRaw,
	})
	require.NoError(t, err)
	if options.failSafety || options.underpowered {
		require.Equal(t, eval.VerdictFail, report.Gate.Verdict)
	} else {
		require.Equal(t, eval.VerdictPass, report.Gate.Verdict)
	}
	_, candidateProfile, ok := reportProfiles(report)
	require.True(t, ok)
	require.True(t, candidateProfile.TemperatureFitted)
	require.True(t, candidateProfile.ThetaFitted)

	ended := fixture.clock.now.Add(-time.Hour)
	endedAt := &ended
	trainingStatus := trainingStatusComplete
	if options.trainingStatus != "" {
		trainingStatus = options.trainingStatus
	}
	if trainingStatus == trainingStatusRunning {
		endedAt = nil
	}
	readinessStatus := gateStatusPassed
	if options.underpowered {
		readinessStatus = trainingUnderpowered
	}
	training := protocol.ModelTrainingRunMetadata{
		TrainingRunID: "pipeline-training-1", DatasetVersion: "pipeline-dataset-1",
		DatasetSHA256: digestHex(trainingManifest.DatasetDigest), MLContractVersion: fixture.base.MLContractVersion,
		Contract:      protocol.ModelContractAnalyzeAgentState,
		BaseModelRepo: fixture.base.BaseModelRepo, BaseModelRevision: fixture.base.BaseModelRevision,
		Method: "lora", Hyperparameters: protocol.Payload(`{"rank":8}`), Seed: 7,
		TrainingCodeRevision: "pipeline-code-1", GPU: run.GPU, ReadinessStatus: readinessStatus,
		StartedAt: ended.Add(-time.Hour), EndedAt: endedAt, Status: trainingStatus,
	}
	trainingRef := writePipelineJSON(t, dir, "training.json", training)
	adapter := protocol.ModelAdapterMetadata{
		AdapterName: testAdapterName, AdapterRevision: "pipeline-rev-1", AdapterSHA256: digest("a"),
		TrainingRunID: training.TrainingRunID, Contract: training.Contract,
		BaseModelRepo: training.BaseModelRepo, BaseModelRevision: training.BaseModelRevision,
		Runtime: run.Runtime, Quantization: run.Quantization, PromptVersion: run.PromptVersion,
		ProjectionPolicyVersion: fixture.base.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  fixture.base.ProjectionPolicySHA256,
		ProfileRevision:         fixture.base.ProfileRevision, ProfileSHA256: fixture.base.ProfileSHA256,
		CalibrationRevision: "pipeline-calibration-1", CalibrationSHA256: sealedRef.SHA256,
		CalibrationTemperature: candidateProfile.Temperature, IdleThreshold: candidateProfile.Theta,
		AnalysisPolicyVersion: fixture.base.AnalysisPolicyVersion,
	}
	adapterRef := writePipelineJSON(t, dir, "adapter.json", adapter)
	return pipelineTestBundle{
		request: protocol.RegisterModelPipelineRequest{
			EvaluationRunID: "pipeline-evaluation-1", Manifest: manifestRef,
			TrainingRunMetadata: trainingRef, AdapterMetadata: adapterRef,
			BasePredictions: baseRef, CandidatePredictions: candidateRef,
			SafetyManifest: safetyRef, SafetyBasePredictions: safetyBaseRef,
			SafetyCandidatePredictions: safetyCandidateRef,
			FinalEvaluation: &protocol.ModelFinalEvaluationEvidence{
				TrainingManifest:              trainingManifestRef,
				ValidationCalibrationSessions: valCalRef,
				BlindTestSessions:             blindRef,
				SealedCalibrations:            sealedRef,
			},
		},
		report: report,
	}
}

func pipelineTrainingManifest(t *testing.T, final dataset.Manifest) dataset.Manifest {
	t.Helper()
	training := final
	blindGroup := replay.GroupDigest(pipelineBlindSession)
	training.Entries = make([]dataset.ManifestEntry, 0, len(final.Entries))
	for _, entry := range final.Entries {
		if entry.GroupDigest != blindGroup {
			training.Entries = append(training.Entries, entry)
		}
	}
	training.Counts = dataset.CountEntries(training.Entries)
	training.ObservationCounts = pipelineObservationCounts(training.Entries)
	readiness := dataset.CheckReadiness(training.Entries, final.Readiness.Thresholds)
	training.Readiness = &readiness
	training.DatasetDigest = dataset.ManifestDigest(training)
	require.NoError(t, training.Validate())
	return training
}

func pipelineObservationCounts(entries []dataset.ManifestEntry) *dataset.ObservationCounts {
	episodes := map[protocol.Digest]struct{}{}
	var anchors int64
	for _, entry := range entries {
		if entry.ObservationForm != dataset.ObservationFormPermissionDialogV1 {
			continue
		}
		anchors++
		episodes[entry.ObservationEpisodeDigest] = struct{}{}
	}
	return &dataset.ObservationCounts{
		PermissionAnchors: anchors, PermissionEpisodes: int64(len(episodes)),
	}
}

func pipelineTestManifest(t *testing.T, underpowered bool) dataset.Manifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "eval", "testdata", "safety", "manifest.json"))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	manifest.Entries = append([]dataset.ManifestEntry(nil), manifest.Entries[:5]...)
	labels := []dataset.Label{dataset.LabelC, dataset.LabelA, dataset.LabelD, dataset.LabelE, dataset.LabelD}
	sessions := []string{
		pipelineValCalSession, pipelineValCmpSession, pipelineBlindSession,
		pipelineBlindSession, pipelineHistoricalSession,
	}
	for i := range manifest.Entries {
		split := dataset.SplitTest
		if i < 2 {
			split = dataset.SplitValidation
		}
		manifest.Entries[i].Split = split
		if i < 2 {
			manifest.Entries[i].Split = dataset.SplitValidation
		}
		manifest.Entries[i].Label = labels[i]
		manifest.Entries[i].SourceQuality = protocol.SourceQualityPTYScreen
		manifest.Entries[i].LabelSource = dataset.LabelSourceManual
		manifest.Entries[i].GroupDigest = replay.GroupDigest(sessions[i])
		manifest.Entries[i].ObservationForm = dataset.ObservationFormEmptyInputV1
		if labels[i] == dataset.LabelD {
			manifest.Entries[i].ObservationForm = dataset.ObservationFormPermissionDialogV1
			manifest.Entries[i].ObservationPolicyVersion = session.PermissionDialogPolicyVersion
			manifest.Entries[i].ObservationEpisodeDigest = protocol.Digest(fmt.Sprintf("sha256:%064x", i+1))
			manifest.Entries[i].ObservationAnchorDigest = protocol.Digest(fmt.Sprintf("sha256:%064x", i+101))
			manifest.Entries[i].LabelSource = dataset.LabelSourceScript
		}
	}
	manifest.ScreenForm = dataset.ScreenFormLiveV2
	manifest.ObservationCounts = &dataset.ObservationCounts{
		PermissionAnchors: 2, PermissionEpisodes: 2,
	}
	manifest.Counts = dataset.CountEntries(manifest.Entries)
	thresholds := dataset.ReadinessThresholds{}
	if underpowered {
		thresholds.TrainMin = 1
	}
	readiness := dataset.CheckReadiness(manifest.Entries, thresholds)
	require.Equal(t, !underpowered, readiness.Passed)
	manifest.Readiness = &readiness
	manifest.DatasetDigest = dataset.ManifestDigest(manifest)
	require.NoError(t, manifest.Validate())
	return manifest
}

func ordinaryPipelinePredictions(
	manifest dataset.Manifest,
	profile eval.Profile,
	run eval.RunProvenance,
	better bool,
) []pipelineTestPrediction {
	probs := []map[dataset.Label]float64{
		twoProbabilities(dataset.LabelC, 0.8, dataset.LabelA, 0.2),
		twoProbabilities(dataset.LabelA, 0.9, dataset.LabelB, 0.1),
		twoProbabilities(dataset.LabelA, 0.6, dataset.LabelD, 0.4),
		twoProbabilities(dataset.LabelE, 0.8, dataset.LabelA, 0.2),
		twoProbabilities(dataset.LabelD, 0.7, dataset.LabelB, 0.3),
	}
	if better {
		probs[2] = twoProbabilities(dataset.LabelA, 0.3, dataset.LabelD, 0.7)
	}
	out := make([]pipelineTestPrediction, 0, len(manifest.Entries))
	for i, entry := range manifest.Entries {
		out = append(out, newPipelinePrediction(entry, profile, run, probs[i], i+1))
	}
	return out
}

func safetyPipelinePredictions(
	manifest dataset.Manifest,
	profile eval.Profile,
	run eval.RunProvenance,
) []pipelineTestPrediction {
	out := make([]pipelineTestPrediction, 0, len(manifest.Entries))
	for i, entry := range manifest.Entries {
		out = append(out, newPipelinePrediction(entry, profile, run, probabilities(entry.Label, 1), i+100))
	}
	return out
}

func newPipelinePrediction(
	entry dataset.ManifestEntry,
	profile eval.Profile,
	run eval.RunProvenance,
	probs map[dataset.Label]float64,
	n int,
) pipelineTestPrediction {
	sessionID := protocol.OpaqueID(fmt.Sprintf("pipeline-safety-session-%d", n))
	for session, group := range map[string]protocol.Digest{
		pipelineValCalSession:     replay.GroupDigest(pipelineValCalSession),
		pipelineValCmpSession:     replay.GroupDigest(pipelineValCmpSession),
		pipelineBlindSession:      replay.GroupDigest(pipelineBlindSession),
		pipelineHistoricalSession: replay.GroupDigest(pipelineHistoricalSession),
	} {
		if entry.GroupDigest == group {
			sessionID = protocol.OpaqueID(session)
			break
		}
	}
	return pipelineTestPrediction{
		WindowID: entry.WindowID, Split: entry.Split, Harness: entry.Harness,
		HarnessVersion: entry.HarnessVersion, SessionID: sessionID,
		SourceQuality: entry.SourceQuality, Label: entry.Label, Probabilities: probs,
		ValidMass: 1, LatencyMS: 100, Profile: profile,
		PromptVersion: run.PromptVersion, BaseModelRepo: run.BaseModelRepo,
		BaseModelRevision: run.BaseModelRevision, Runtime: run.Runtime,
		RuntimeVersion: run.RuntimeVersion, Quantization: run.Quantization,
		GPU: run.GPU, PredictConcurrency: run.PredictConcurrency,
		AdapterName: run.AdapterName, AdapterRevision: run.AdapterRevision,
		AdapterSHA256: run.AdapterSHA256, TrainingRunID: run.TrainingRunID,
		SecondsSinceLastOutput: 1,
	}
}

func probabilities(label dataset.Label, probability float64) map[dataset.Label]float64 {
	out := map[dataset.Label]float64{
		dataset.LabelA: 0, dataset.LabelB: 0, dataset.LabelC: 0, dataset.LabelD: 0,
		dataset.LabelE: 0, dataset.LabelF: 0, dataset.LabelG: 0,
	}
	out[label] = probability
	return out
}

func twoProbabilities(
	first dataset.Label,
	firstProbability float64,
	second dataset.Label,
	secondProbability float64,
) map[dataset.Label]float64 {
	out := probabilities(first, firstProbability)
	out[second] = secondProbability
	return out
}

func writePipelineJSON(t *testing.T, dir, name string, value any) protocol.ModelEvidenceReference {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return writePipelineBytes(t, dir, name, raw)
}

func writePipelineJSONL(
	t *testing.T,
	dir, name string,
	lines []pipelineTestPrediction,
) (protocol.ModelEvidenceReference, []byte) {
	t.Helper()
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	encoder.SetEscapeHTML(false)
	for _, line := range lines {
		require.NoError(t, encoder.Encode(line))
	}
	return writePipelineBytes(t, dir, name, raw.Bytes()), raw.Bytes()
}

func writePipelineBytes(t *testing.T, dir, name string, raw []byte) protocol.ModelEvidenceReference {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return protocol.ModelEvidenceReference{Path: path, SHA256: digestBytes(raw)}
}

func readPipelineTestPredictions(t *testing.T, raw []byte, profile eval.Profile) []eval.Prediction {
	t.Helper()
	predictions, err := eval.ReadPredictions(bytes.NewReader(raw), profile)
	require.NoError(t, err)
	return predictions
}
