package modelprofile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type testPublisher struct {
	mu      sync.Mutex
	current protocol.ModelSelection
}

func (p *testPublisher) CurrentModelSelection() protocol.ModelSelection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *testPublisher) Publish(next protocol.ModelSelection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if next.Generation <= p.current.Generation {
		return errors.New("generation did not advance")
	}
	p.current = next
	return nil
}

type testVerifier struct {
	err  error
	seen []protocol.ModelSelection
}

func (v *testVerifier) VerifyModelSelection(_ context.Context, selection protocol.ModelSelection) error {
	v.seen = append(v.seen, selection)
	return v.err
}

type failingEvents struct{ err error }

func (f failingEvents) NextEventSequenceTx(context.Context, *sql.Tx, protocol.OpaqueID) (int64, error) {
	return 2, nil
}

func (f failingEvents) AppendEventsTx(context.Context, *sql.Tx, []protocol.EventEnvelope) error {
	return f.err
}

type serviceFixture struct {
	store         *eventstore.Store
	clock         testClock
	base          protocol.ModelSelection
	manifest      dataset.Manifest
	manifestPath  string
	readinessJSON []byte
	publisher     *testPublisher
	verifier      *testVerifier
	service       *Service
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	manifest, manifestPath, readinessJSON := writeReadyManifest(t)
	fixture := &serviceFixture{
		store:    store,
		clock:    testClock{now: time.Date(2026, time.November, 1, 17, 0, 0, 0, time.UTC)},
		base:     validBase(),
		manifest: manifest, manifestPath: manifestPath, readinessJSON: readinessJSON,
		verifier: &testVerifier{},
	}
	opts := fixture.options(store)
	selected, err := Initialize(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, int64(1), selected.Generation)
	fixture.publisher = &testPublisher{current: selected}
	opts.Publisher = fixture.publisher
	opts.Verifier = fixture.verifier
	fixture.service, err = New(opts)
	require.NoError(t, err)
	return fixture
}

func writeReadyManifest(t *testing.T) (dataset.Manifest, string, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "eval", "testdata", "safety", "manifest.json"))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	for i := range manifest.Entries {
		manifest.Entries[i].SourceQuality = protocol.SourceQualityPTYScreen
		manifest.Entries[i].LabelSource = dataset.LabelSourceManual
	}
	readiness := dataset.CheckReadiness(manifest.Entries, dataset.ReadinessThresholds{})
	require.True(t, readiness.Passed)
	manifest.Readiness = &readiness
	manifest.DatasetDigest = dataset.ManifestDigest(manifest)
	require.NoError(t, manifest.Validate())
	encoded, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	readinessJSON, err := json.Marshal(readiness)
	require.NoError(t, err)
	return manifest, path, readinessJSON
}

func (f *serviceFixture) options(events EventStore) Options {
	return Options{
		DB: f.store.DB(), Events: events, NodeID: "node-a", Clock: f.clock,
		Base: f.base,
		Validate: func(selection protocol.ModelSelection) error {
			if selection.PromptVersion != f.base.PromptVersion ||
				selection.ProjectionPolicyVersion != f.base.ProjectionPolicyVersion ||
				selection.ProjectionPolicySHA256 != f.base.ProjectionPolicySHA256 ||
				selection.ProfileRevision != f.base.ProfileRevision ||
				selection.ProfileSHA256 != f.base.ProfileSHA256 ||
				selection.CalibrationRevision != f.base.CalibrationRevision ||
				selection.CalibrationSHA256 != f.base.CalibrationSHA256 ||
				selection.CalibrationTemperature != f.base.CalibrationTemperature ||
				selection.IdleThreshold != f.base.IdleThreshold ||
				selection.AnalysisPolicyVersion != f.base.AnalysisPolicyVersion {
				return errors.New("incompatible tuple")
			}
			return nil
		},
	}
}

func validBase() protocol.ModelSelection {
	return protocol.ModelSelection{
		Profile: protocol.GatewayProfile{
			Contract: protocol.ModelContractAnalyzeAgentState, InferenceProfile: protocol.InferenceProfileBase,
		},
		BaseModelRepo: "example.invalid/model", BaseModelRevision: "base-1", Runtime: "vllm", Quantization: "bf16",
		MLContractVersion: "ml-1", PromptVersion: "analyze_agent_state.v2",
		ProjectionPolicyVersion: "projection-1", ProjectionPolicySHA256: digest("1"),
		ProfileRevision: "profile-1", ProfileSHA256: digest("2"),
		CalibrationRevision: "calibration-1", CalibrationSHA256: digest("3"),
		CalibrationTemperature: 1.25, IdleThreshold: 0.6, AnalysisPolicyVersion: "analysis-1",
	}
}

func (f *serviceFixture) validRegistration(t *testing.T) protocol.RegisterModelMetadataRequest {
	t.Helper()
	gate := make([]protocol.ModelGateCriterion, 6)
	reportGate := make([]eval.Criterion, 6)
	for i := range gate {
		name := fmt.Sprintf("criterion-%d", i)
		gate[i] = protocol.ModelGateCriterion{ID: i, Name: name, Verdict: "PASS"}
		reportGate[i] = eval.Criterion{ID: i, Name: name, Verdict: eval.VerdictPass, Detail: "criterion satisfied"}
	}
	run := eval.RunProvenance{
		PromptVersion: f.base.PromptVersion, BaseModelRepo: f.base.BaseModelRepo,
		BaseModelRevision: f.base.BaseModelRevision, Runtime: f.base.Runtime,
		RuntimeVersion: "runtime-1", Quantization: f.base.Quantization, GPU: "test-gpu", PredictConcurrency: 1,
	}
	baseMetrics := eval.Metrics{N: 2, Accuracy: 0.5, MacroF1: 0.5}
	loraMetrics := eval.Metrics{N: 2, Accuracy: 1, MacroF1: 1}
	validationRun := run
	validationRun.PredictConcurrency = 8
	reportValue := eval.Report{
		Bins: 15, LatencyBudgetMS: 1500,
		Inputs: []eval.Input{
			{
				Profile: eval.ProfileBase,
				Runs: map[dataset.Split]eval.RunProvenance{
					dataset.SplitValidation: validationRun, dataset.SplitTest: run,
				},
				Digest: "sha256:" + strings.Repeat("a", 64), Lines: 4,
			},
			{
				Profile: eval.ProfileLoRA,
				Runs: map[dataset.Split]eval.RunProvenance{
					dataset.SplitValidation: validationRun, dataset.SplitTest: run,
				},
				Digest: "sha256:" + strings.Repeat("b", 64), Lines: 4,
			},
		},
		Dataset: &eval.DatasetSummary{
			Digest: f.manifest.DatasetDigest, SplitFile: f.manifest.SplitFile,
			Align: f.manifest.Align, ScriptedInput: f.manifest.ScriptedInput,
		},
		Profiles: []eval.ProfileReport{
			{
				Profile: eval.ProfileBase, Role: string(eval.ProfileBase),
				Temperature: 1, TemperatureFitted: true, Theta: 0.5, ThetaFitted: true,
				Validation: eval.SplitReport{All: baseMetrics}, Test: eval.SplitReport{All: baseMetrics},
			},
			{
				Profile: eval.ProfileLoRA, Role: string(eval.ProfileLoRA),
				Temperature: f.base.CalibrationTemperature, TemperatureFitted: true,
				Theta: f.base.IdleThreshold, ThetaFitted: true,
				Validation: eval.SplitReport{All: loraMetrics}, Test: eval.SplitReport{All: loraMetrics},
			},
		},
		Gate: eval.Gate{Criteria: reportGate, Verdict: eval.VerdictPass},
		Safety: &eval.SafetySuiteReport{
			Digest: protocol.Digest("sha256:" + strings.Repeat("c", 64)), Total: 1,
			Candidate: eval.SafetyAssessment{Passed: true},
		},
	}
	report, err := json.Marshal(reportValue)
	require.NoError(t, err)
	sum := sha256.Sum256(report)
	readinessSum := sha256.Sum256(f.readinessJSON)
	return protocol.RegisterModelMetadataRequest{
		AdapterName: "analyst", AdapterRevision: "rev-1", AdapterSHA256: digest("4"),
		Contract:      protocol.ModelContractAnalyzeAgentState,
		BaseModelRepo: f.base.BaseModelRepo, BaseModelRevision: f.base.BaseModelRevision,
		Runtime: f.base.Runtime, Quantization: f.base.Quantization, MLContractVersion: f.base.MLContractVersion,
		DatasetVersion: "dataset-1", DatasetSHA256: protocol.SHA256Hex(strings.TrimPrefix(string(f.manifest.DatasetDigest), "sha256:")),
		DatasetReadinessStatus: gateStatusPassed, DatasetReadinessSHA256: protocol.SHA256Hex(hex.EncodeToString(readinessSum[:])),
		TrainingRunID: "training-1", TrainingCodeRevision: "code-1",
		TrainingReadinessStatus: gateStatusPassed, TrainingStatus: trainingStatusComplete,
		EvaluationRunID: "evaluation-1", EvaluationSHA256: protocol.SHA256Hex(hex.EncodeToString(sum[:])),
		EvaluationReport: protocol.Payload(report), Gate: gate,
		PromptVersion:           f.base.PromptVersion,
		ProjectionPolicyVersion: f.base.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  f.base.ProjectionPolicySHA256,
		ProfileRevision:         f.base.ProfileRevision, ProfileSHA256: f.base.ProfileSHA256,
		CalibrationRevision: f.base.CalibrationRevision, CalibrationSHA256: f.base.CalibrationSHA256,
		CalibrationTemperature: f.base.CalibrationTemperature, IdleThreshold: f.base.IdleThreshold,
		AnalysisPolicyVersion: f.base.AnalysisPolicyVersion,
	}
}

func (f *serviceFixture) register(
	t *testing.T,
	req protocol.RegisterModelMetadataRequest,
) (protocol.ModelMetadataRegistrationResult, error) {
	t.Helper()
	f.seedPipelineEvidence(t, req)
	return f.service.Register(t.Context(), req, operator())
}

func (f *serviceFixture) seedPipelineEvidence(t *testing.T, req protocol.RegisterModelMetadataRequest) {
	t.Helper()
	labels, harnesses := manifestCounts(f.manifest)
	labelsJSON, err := json.Marshal(labels)
	require.NoError(t, err)
	harnessesJSON, err := json.Marshal(harnesses)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT OR IGNORE INTO dataset_versions (
		dataset_version, ml_contract_version, manifest_path, dataset_sha256, window_count,
		counts_by_label_json, counts_by_harness_json, split_strategy, redaction_policy_version,
		readiness_status, readiness_json, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, 'split-v1', 'test-redaction', 'passed', ?, ?)`,
		req.DatasetVersion, f.base.MLContractVersion, f.manifestPath, f.manifest.DatasetDigest,
		f.manifest.Counts.Windows, labelsJSON, harnessesJSON, f.readinessJSON,
		eventstore.FormatTime(f.clock.now))
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT OR IGNORE INTO training_runs (
		training_run_id, dataset_version, base_model_repo, base_model_revision, method,
		hyperparameters_json, seed, training_code_revision, readiness_status,
		started_at, ended_at, status
	) VALUES (?, ?, ?, ?, 'lora', '{}', 7, ?, 'passed', ?, ?, 'completed')`,
		req.TrainingRunID, req.DatasetVersion, f.base.BaseModelRepo, f.base.BaseModelRevision,
		req.TrainingCodeRevision, eventstore.FormatTime(f.clock.now.Add(-time.Hour)),
		eventstore.FormatTime(f.clock.now))
	require.NoError(t, err)

	var report eval.Report
	require.NoError(t, json.Unmarshal(req.EvaluationReport, &report))
	base, lora, ok := reportProfiles(report)
	require.True(t, ok)
	gateJSON, err := json.Marshal(report.Gate)
	require.NoError(t, err)
	rows := []struct {
		id       string
		profile  eval.Profile
		adapter  any
		split    string
		metrics  eval.Metrics
		gateJSON any
	}{
		{string(req.EvaluationRunID) + "-base-validation", eval.ProfileBase, nil, "validation", base.Validation.All, nil},
		{string(req.EvaluationRunID) + "-base-test", eval.ProfileBase, nil, evaluationSplitTest, base.Test.All, nil},
		{string(req.EvaluationRunID) + "-lora-validation", eval.ProfileLoRA, req.AdapterRevision, "validation", lora.Validation.All, nil},
		{string(req.EvaluationRunID), eval.ProfileLoRA, req.AdapterRevision, evaluationSplitTest, lora.Test.All, gateJSON},
	}
	for _, row := range rows {
		metricsJSON, marshalErr := json.Marshal(row.metrics)
		require.NoError(t, marshalErr)
		_, err = f.store.DB().ExecContext(t.Context(), `INSERT OR IGNORE INTO evaluation_runs (
			evaluation_run_id, dataset_version, inference_profile, adapter_revision, split,
			metrics_json, gate_verdict_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, row.id, req.DatasetVersion, row.profile,
			row.adapter, row.split, metricsJSON, row.gateJSON, eventstore.FormatTime(f.clock.now))
		require.NoError(t, err)
	}
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE evaluation_runs SET
		evaluation_sha256 = ?, evaluation_report_json = ?, evidence_json = '{}'
		WHERE evaluation_run_id = ?`, req.EvaluationSHA256, req.EvaluationReport, req.EvaluationRunID)
	require.NoError(t, err)
}

func manifestCounts(manifest dataset.Manifest) (map[string]int64, map[string]int64) {
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

func setReportPrompt(t *testing.T, req *protocol.RegisterModelMetadataRequest, prompt string) {
	t.Helper()
	var report eval.Report
	require.NoError(t, json.Unmarshal(req.EvaluationReport, &report))
	for i := range report.Inputs {
		for split, run := range report.Inputs[i].Runs {
			run.PromptVersion = prompt
			report.Inputs[i].Runs[split] = run
		}
	}
	reportJSON, err := json.Marshal(report)
	require.NoError(t, err)
	req.PromptVersion = prompt
	req.EvaluationReport = reportJSON
	req.EvaluationSHA256 = digestBytes(reportJSON)
}

func digest(char string) protocol.SHA256Hex { return protocol.SHA256Hex(strings.Repeat(char, 64)) }

func operator() channel.Actor { return channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"} }

func TestRegisterActivateRollbackAndRestart(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	registered, err := fixture.register(t, req)
	require.NoError(t, err)
	require.True(t, registered.Imported)
	require.Equal(t, req.AdapterName, registered.AdapterName)

	again, err := fixture.register(t, req)
	require.NoError(t, err)
	require.False(t, again.Imported)
	require.Equal(t, registered.RegisteredAt, again.RegisteredAt)

	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.True(t, activated.Applied)
	require.Equal(t, int64(2), activated.Selection.Generation)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, activated.Selection.Profile.InferenceProfile)
	require.Equal(t, req.AdapterSHA256, activated.Selection.AdapterSHA256)
	require.Equal(t, req.DatasetSHA256, activated.Selection.DatasetSHA256)
	require.Equal(t, req.EvaluationSHA256, activated.Selection.EvaluationSHA256)
	require.Len(t, fixture.verifier.seen, 1)
	require.Equal(t, activated.Selection, fixture.publisher.CurrentModelSelection())

	var active int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Equal(t, int64(1), active)
	var events int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events
		WHERE event_type = ?`, protocol.EventTypeControlModelProfileChanged).Scan(&events))
	require.Equal(t, int64(2), events, "startup and activation are both durable changes")

	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, activated.Selection, restarted)

	noOp, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.False(t, noOp.Applied)
	require.Equal(t, int64(2), noOp.Selection.Generation)

	rolledBack, err := fixture.service.Rollback(t.Context(), protocol.RollbackToBaseRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
	}, operator())
	require.NoError(t, err)
	require.True(t, rolledBack.Applied)
	require.Equal(t, int64(3), rolledBack.Selection.Generation)
	require.Equal(t, protocol.InferenceProfileBase, rolledBack.Selection.Profile.InferenceProfile)
	require.Empty(t, rolledBack.Selection.AdapterSHA256)
	require.Empty(t, rolledBack.Selection.DatasetVersion)
	require.Empty(t, rolledBack.Selection.TrainingRunID)
	require.Empty(t, rolledBack.Selection.EvaluationRunID)
	require.Equal(t, fixture.base.BaseModelRevision, rolledBack.Selection.BaseModelRevision)
	require.Equal(t, fixture.base.PromptVersion, rolledBack.Selection.PromptVersion)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Zero(t, active)
}

func TestRegisteredMetadataIsImmutableInStorage(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_profile_registry
		SET prompt_version = 'changed' WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision)
	require.ErrorContains(t, err, "model_profile_registry is immutable")
	_, err = fixture.store.DB().ExecContext(t.Context(), `DELETE FROM model_profile_registry
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.ErrorContains(t, err, "model_profile_registry is immutable")

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_adapters SET gate_status = 'failed'
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.NoError(t, err)
	_, err = fixture.register(t, req)
	require.ErrorIs(t, err, ErrRegistryInvalid,
		"an idempotent import still verifies the mutable serving binding")
}

func TestActivateRechecksMutableAdapterBinding(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_adapters
		SET training_run_id = 'other-training' WHERE adapter_name = ? AND adapter_revision = ?`,
		req.AdapterName, req.AdapterRevision)
	require.NoError(t, err)

	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.Empty(t, fixture.verifier.seen)
	var generation int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT generation
		FROM model_profile_selections WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&generation))
	require.Equal(t, int64(1), generation)
}

func TestRegisterRejectsIncompleteOrUntrustedEvidence(t *testing.T) {
	tests := map[string]func(*protocol.RegisterModelMetadataRequest){
		"gate failure": func(req *protocol.RegisterModelMetadataRequest) { req.Gate[3].Verdict = "FAIL" },
		"missing criterion": func(req *protocol.RegisterModelMetadataRequest) {
			req.Gate[5].ID = 4
		},
		"evaluation digest":   func(req *protocol.RegisterModelMetadataRequest) { req.EvaluationSHA256 = digest("9") },
		"dataset readiness":   func(req *protocol.RegisterModelMetadataRequest) { req.DatasetReadinessStatus = "failed" },
		"underpowered":        func(req *protocol.RegisterModelMetadataRequest) { req.TrainingReadinessStatus = trainingUnderpowered },
		"training incomplete": func(req *protocol.RegisterModelMetadataRequest) { req.TrainingStatus = trainingStatusRunning },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newServiceFixture(t)
			req := fixture.validRegistration(t)
			mutate(&req)
			_, err := fixture.register(t, req)
			require.ErrorIs(t, err, ErrRegistryInvalid)
			var count int64
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT COUNT(*) FROM model_profile_registry`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestRegisterRejectsContradictoryEvaluationReport(t *testing.T) {
	for _, verdict := range []eval.Verdict{eval.VerdictFail, eval.VerdictPending} {
		t.Run(string(verdict), func(t *testing.T) {
			fixture := newServiceFixture(t)
			req := fixture.validRegistration(t)
			var report eval.Report
			require.NoError(t, json.Unmarshal(req.EvaluationReport, &report))
			report.Gate.Verdict = verdict
			for i := range report.Gate.Criteria {
				report.Gate.Criteria[i].Verdict = verdict
			}
			reportJSON, err := json.Marshal(report)
			require.NoError(t, err)
			req.EvaluationReport = reportJSON
			req.EvaluationSHA256 = digestBytes(reportJSON)

			_, err = fixture.register(t, req)
			require.ErrorIs(t, err, ErrRegistryInvalid)
			var registered, adapters int64
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT COUNT(*) FROM model_profile_registry`).Scan(&registered))
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT COUNT(*) FROM model_adapters`).Scan(&adapters))
			require.Zero(t, registered)
			require.Zero(t, adapters)
		})
	}
}

func TestRegisterRequiresCoherentPipelineEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *serviceFixture, protocol.RegisterModelMetadataRequest)
	}{
		{name: "missing rows"},
		{name: "underpowered training", mutate: func(t *testing.T, f *serviceFixture, req protocol.RegisterModelMetadataRequest) {
			f.seedPipelineEvidence(t, req)
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE training_runs
				SET readiness_status = ? WHERE training_run_id = ?`, trainingUnderpowered, req.TrainingRunID)
			require.NoError(t, err)
		}},
		{name: "evaluation adapter mismatch", mutate: func(t *testing.T, f *serviceFixture, req protocol.RegisterModelMetadataRequest) {
			f.seedPipelineEvidence(t, req)
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE evaluation_runs
				SET adapter_revision = 'other-revision' WHERE evaluation_run_id = ?`, req.EvaluationRunID)
			require.NoError(t, err)
		}},
		{name: "evaluation missing importer provenance", mutate: func(t *testing.T, f *serviceFixture, req protocol.RegisterModelMetadataRequest) {
			f.seedPipelineEvidence(t, req)
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE evaluation_runs SET
				evaluation_sha256 = NULL, evaluation_report_json = NULL, evidence_json = NULL
				WHERE evaluation_run_id = ?`, req.EvaluationRunID)
			require.NoError(t, err)
		}},
		{name: "failed dataset readiness", mutate: func(t *testing.T, f *serviceFixture, req protocol.RegisterModelMetadataRequest) {
			f.seedPipelineEvidence(t, req)
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE dataset_versions
				SET readiness_status = 'failed' WHERE dataset_version = ?`, req.DatasetVersion)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newServiceFixture(t)
			req := fixture.validRegistration(t)
			if tc.mutate != nil {
				tc.mutate(t, fixture, req)
			}
			_, err := fixture.service.Register(t.Context(), req, operator())
			require.ErrorIs(t, err, ErrRegistryInvalid)
		})
	}
}

func TestActivateRejectsMissingIncompatibleAndUnservedCandidates(t *testing.T) {
	fixture := newServiceFixture(t)
	initial := fixture.publisher.CurrentModelSelection()
	_, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: "missing", AdapterRevision: "rev-1",
	}, operator())
	require.ErrorIs(t, err, ErrAdapterNotRegistered)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())

	req := fixture.validRegistration(t)
	setReportPrompt(t, &req, "unknown-prompt")
	_, err = fixture.register(t, req)
	require.NoError(t, err)
	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.Empty(t, fixture.verifier.seen)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())

	compatible := fixture.validRegistration(t)
	compatible.AdapterRevision = "rev-2"
	compatible.AdapterSHA256 = digest("7")
	compatible.EvaluationRunID = "evaluation-2"
	_, err = fixture.register(t, compatible)
	require.NoError(t, err)
	fixture.verifier.err = errors.New("not loaded")
	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: compatible.AdapterName, AdapterRevision: compatible.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrProfileNotServed)
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())
	var generation int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT generation
		FROM model_profile_selections WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&generation))
	require.Equal(t, int64(1), generation)
}

func TestPersistenceFailureLeavesMemoryAndAdapterFlagsUnchanged(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)
	broken := fixture.options(failingEvents{err: errors.New("event write failed")})
	broken.Publisher = fixture.publisher
	broken.Verifier = fixture.verifier
	service, err := New(broken)
	require.NoError(t, err)
	initial := fixture.publisher.CurrentModelSelection()
	_, err = service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.ErrorContains(t, err, "append model selection event")
	require.Equal(t, initial, fixture.publisher.CurrentModelSelection())
	var active, generation int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT active FROM model_adapters
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision).Scan(&active))
	require.Zero(t, active)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT generation
		FROM model_profile_selections WHERE contract = ?`, protocol.ModelContractAnalyzeAgentState).Scan(&generation))
	require.Equal(t, int64(1), generation)
}

func TestAutomaticFallbackOnlyRollsBackItsCapturedGeneration(t *testing.T) {
	fixture := newServiceFixture(t)
	first := fixture.validRegistration(t)
	_, err := fixture.register(t, first)
	require.NoError(t, err)
	firstActivation, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: first.AdapterName, AdapterRevision: first.AdapterRevision,
	}, operator())
	require.NoError(t, err)

	second := fixture.validRegistration(t)
	second.AdapterRevision = "rev-2"
	second.AdapterSHA256 = digest("7")
	second.EvaluationRunID = "evaluation-2"
	_, err = fixture.register(t, second)
	require.NoError(t, err)
	secondActivation, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: second.AdapterName, AdapterRevision: second.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(3), secondActivation.Selection.Generation)

	require.NoError(t, fixture.service.FallbackToBase(
		t.Context(), firstActivation.Selection.Generation, "stale adapter failure",
	))
	require.Equal(t, secondActivation.Selection, fixture.publisher.CurrentModelSelection(),
		"an old in-flight failure cannot roll back a newer activation")

	require.NoError(t, fixture.service.FallbackToBase(
		t.Context(), secondActivation.Selection.Generation, "current adapter failure",
	))
	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, int64(4), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
}

func TestInitializeDistrustsCorruptAdapterSelectionAndFallsBackDegraded(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)
	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(2), activated.Selection.Generation)
	corrupt := activated.Selection
	corrupt.AdapterSHA256 = digest("9")
	raw, err := json.Marshal(corrupt)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_profile_selections
		SET selection_json = ? WHERE contract = ?`, raw, protocol.ModelContractAnalyzeAgentState)
	require.NoError(t, err)

	selected, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, int64(3), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
	require.Contains(t, selected.Reason, "startup selection validation failed")
}

func TestInitializeDistrustsInconsistentAdapterFlags(t *testing.T) {
	fixture := newServiceFixture(t)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)
	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.Equal(t, int64(2), activated.Selection.Generation)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE model_adapters SET active = 0
		WHERE adapter_name = ? AND adapter_revision = ?`, req.AdapterName, req.AdapterRevision)
	require.NoError(t, err)

	selected, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, int64(3), selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, "DEGRADED", selected.OperationalState)
}
