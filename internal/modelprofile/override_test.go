package modelprofile

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
)

type overrideTestBundle struct {
	request     protocol.ActivateAdapterOverrideRequest
	adapterRaw  []byte
	trainingRaw []byte
	reportRaw   []byte
}

func TestActivateOverridePersistsExactEvidenceRestartsAndRollsBack(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)

	result, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.NoError(t, err)
	require.True(t, result.Applied)
	selected := result.Selection
	require.Equal(t, int64(2), selected.Generation)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, selected.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusOverride, selected.GateStatus)
	require.Equal(t, "operator override: incident review", selected.Reason)
	require.Equal(t, "READY", selected.OperationalState)
	require.Empty(t, selected.Quantization, "empty quantization means native weights")
	require.Equal(t, "override-dataset-1", selected.DatasetVersion)
	require.Equal(t, "override-ml-1", selected.MLContractVersion)
	require.Equal(t, bundle.request.EvaluationReport.SHA256, selected.EvaluationSHA256)
	require.True(t, protocol.ULID(selected.EvaluationRunID).Valid())
	require.Len(t, fixture.verifier.seen, 1)
	require.Equal(t, selected, fixture.verifier.seen[0])
	require.Equal(t, selected, fixture.publisher.CurrentModelSelection())

	var adapterSHA, trainingSHA, reportSHA, reason, actorType, actorID string
	var adapterJSON, trainingJSON, reportJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		adapter_metadata_sha256, training_run_metadata_sha256,
		evaluation_report_sha256, reason, actor_type, actor_id,
		adapter_metadata_json, training_run_metadata_json, evaluation_report_json
		FROM model_activation_overrides WHERE evaluation_run_id = ?`, selected.EvaluationRunID).Scan(
		&adapterSHA, &trainingSHA, &reportSHA, &reason, &actorType, &actorID,
		&adapterJSON, &trainingJSON, &reportJSON,
	))
	require.Equal(t, string(bundle.request.AdapterMetadata.SHA256), adapterSHA)
	require.Equal(t, string(bundle.request.TrainingRunMetadata.SHA256), trainingSHA)
	require.Equal(t, string(bundle.request.EvaluationReport.SHA256), reportSHA)
	require.Equal(t, selected.Reason, reason, "the complete prefixed reason is durable")
	require.Equal(t, string(protocol.ActorTypeHuman), actorType)
	require.Equal(t, operator().ID, actorID)
	require.True(t, bytes.Equal(bundle.adapterRaw, []byte(adapterJSON)), "adapter evidence bytes changed")
	require.True(t, bytes.Equal(bundle.trainingRaw, []byte(trainingJSON)), "training evidence bytes changed")
	require.True(t, bytes.Equal(bundle.reportRaw, []byte(reportJSON)), "report evidence bytes changed")

	var registered, active int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*), COALESCE(SUM(active), 0) FROM model_adapters`).Scan(&registered, &active))
	require.Zero(t, registered, "an override does not register the adapter")
	require.Zero(t, active)
	_, err = fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: selected.Profile.AdapterName, AdapterRevision: selected.Profile.AdapterRevision,
	}, operator())
	require.ErrorIs(t, err, ErrAdapterNotRegistered,
		"the ordinary activation route must not treat an override as a passed registration")

	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, selected, restarted)

	rolledBack, err := fixture.service.Rollback(t.Context(), protocol.RollbackToBaseRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
	}, operator())
	require.NoError(t, err)
	require.True(t, rolledBack.Applied)
	require.Equal(t, int64(3), rolledBack.Selection.Generation)
	require.Equal(t, protocol.InferenceProfileBase, rolledBack.Selection.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusBase, rolledBack.Selection.GateStatus)
	require.Equal(t, "READY", rolledBack.Selection.OperationalState)
	require.Equal(t, int64(1), overrideRowCount(t, fixture))

	_, err = fixture.store.DB().ExecContext(t.Context(),
		`UPDATE model_activation_overrides SET reason = 'changed' WHERE evaluation_run_id = ?`,
		selected.EvaluationRunID)
	require.ErrorContains(t, err, "model_activation_overrides is immutable")
}

func TestActivateOverrideRejectsHashMismatchWithoutMutation(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	bundle.request.TrainingRunMetadata.SHA256 = digest("f")

	_, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.Equal(t, int64(1), fixture.publisher.CurrentModelSelection().Generation)
	require.Zero(t, overrideRowCount(t, fixture))
	require.Empty(t, fixture.verifier.seen)
}

func TestActivateOverrideProbeFailureDoesNotMutate(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	fixture.verifier.err = errors.New("served identity mismatch")

	_, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrProfileNotServed)
	require.Equal(t, int64(1), fixture.publisher.CurrentModelSelection().Generation)
	require.Zero(t, overrideRowCount(t, fixture))
	require.Len(t, fixture.verifier.seen, 1)
	var events int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM events WHERE event_type = ?`,
		protocol.EventTypeControlModelProfileChanged).Scan(&events))
	require.Equal(t, int64(1), events, "only startup is durable")
}

func TestActivateOverrideMissingRestartBindingFallsBackDegraded(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	result, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.NoError(t, err)

	_, err = fixture.store.DB().ExecContext(t.Context(), `DROP TRIGGER model_activation_overrides_no_delete`)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(),
		`DELETE FROM model_activation_overrides WHERE evaluation_run_id = ?`, result.Selection.EvaluationRunID)
	require.NoError(t, err)

	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, int64(3), restarted.Generation)
	require.Equal(t, protocol.InferenceProfileBase, restarted.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusBase, restarted.GateStatus)
	require.Equal(t, "DEGRADED", restarted.OperationalState)
	require.Contains(t, restarted.Reason, "startup selection validation failed")
}

func TestActivateOverrideFallbackToBase(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	result, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.NoError(t, err)

	require.NoError(t, fixture.service.FallbackToBase(
		t.Context(), result.Selection.Generation, "serving breaker opened",
	))
	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, result.Selection.Generation+1, selected.Generation)
	require.Equal(t, protocol.InferenceProfileBase, selected.Profile.InferenceProfile)
	require.Equal(t, protocol.ModelGateStatusBase, selected.GateStatus)
	require.Equal(t, "DEGRADED", selected.OperationalState)
	require.Equal(t, "serving breaker opened", selected.Reason)
	require.Equal(t, int64(1), overrideRowCount(t, fixture))
}

func writeOverrideTestBundle(t *testing.T, fixture *serviceFixture) overrideTestBundle {
	t.Helper()
	registered := fixture.validRegistration(t)
	adapter := protocol.ModelAdapterMetadata{
		AdapterName: registered.AdapterName, AdapterRevision: registered.AdapterRevision,
		AdapterSHA256: registered.AdapterSHA256, TrainingRunID: registered.TrainingRunID,
		Contract: registered.Contract, BaseModelRepo: registered.BaseModelRepo,
		BaseModelRevision: registered.BaseModelRevision, Runtime: registered.Runtime,
		Quantization:            "",
		PromptVersion:           registered.PromptVersion,
		ProjectionPolicyVersion: registered.ProjectionPolicyVersion,
		ProjectionPolicySHA256:  registered.ProjectionPolicySHA256,
		ProfileRevision:         registered.ProfileRevision, ProfileSHA256: registered.ProfileSHA256,
		CalibrationRevision:    registered.CalibrationRevision,
		CalibrationSHA256:      registered.CalibrationSHA256,
		CalibrationTemperature: registered.CalibrationTemperature,
		IdleThreshold:          registered.IdleThreshold,
		AnalysisPolicyVersion:  registered.AnalysisPolicyVersion,
	}
	ended := fixture.clock.now
	training := protocol.ModelTrainingRunMetadata{
		TrainingRunID:  registered.TrainingRunID,
		DatasetVersion: "override-dataset-1", DatasetSHA256: digest("d"),
		MLContractVersion: "override-ml-1", Contract: registered.Contract,
		BaseModelRepo: registered.BaseModelRepo, BaseModelRevision: registered.BaseModelRevision,
		Method: "lora", Hyperparameters: protocol.Payload(`{"rank":16}`), Seed: 7,
		TrainingCodeRevision: "override-code-1", ReadinessStatus: gateStatusPassed,
		StartedAt: fixture.clock.now.Add(-time.Hour), EndedAt: &ended, Status: trainingStatusComplete,
	}
	var report eval.Report
	require.NoError(t, json.Unmarshal(registered.EvaluationReport, &report))
	report.Gate = eval.Gate{}

	adapterRaw, err := json.Marshal(adapter)
	require.NoError(t, err)
	trainingRaw, err := json.Marshal(training)
	require.NoError(t, err)
	reportRaw, err := json.Marshal(report)
	require.NoError(t, err)
	dir := t.TempDir()
	request := protocol.ActivateAdapterOverrideRequest{
		AdapterMetadata:     writePipelineBytes(t, dir, "adapter.json", adapterRaw),
		TrainingRunMetadata: writePipelineBytes(t, dir, "training.json", trainingRaw),
		EvaluationReport:    writePipelineBytes(t, dir, "report.json", reportRaw),
		Reason:              "incident review",
	}
	require.NoError(t, request.Validate())
	return overrideTestBundle{
		request: request, adapterRaw: adapterRaw, trainingRaw: trainingRaw, reportRaw: reportRaw,
	}
}

func overrideRowCount(t *testing.T, fixture *serviceFixture) int64 {
	t.Helper()
	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM model_activation_overrides`).Scan(&count))
	return count
}

// A field the adapter metadata gets wrong is named in the rejection.
func TestActivateOverrideNamesTheRejectedField(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	var adapter map[string]any
	require.NoError(t, json.Unmarshal(bundle.adapterRaw, &adapter))
	adapter["calibration_temperature"] = 0
	updated, err := json.Marshal(adapter)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(bundle.request.AdapterMetadata.Path, updated, 0o600))
	bundle.request.AdapterMetadata.SHA256 = digestBytes(updated)

	_, err = fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.ErrorContains(t, err, "adapter metadata: calibration_temperature: outside (0,100]")
	require.Zero(t, overrideRowCount(t, fixture))
}

func TestActivateOverrideRejectsMismatchedReportIdentity(t *testing.T) {
	fixture := newServiceFixture(t)
	bundle := writeOverrideTestBundle(t, fixture)
	updated := strings.Replace(string(bundle.reportRaw), testAdapterRevision, "another-revision", 1)
	require.NotEqual(t, string(bundle.reportRaw), updated)
	require.NoError(t, os.WriteFile(bundle.request.EvaluationReport.Path, []byte(updated), 0o600))
	bundle.request.EvaluationReport.SHA256 = digestBytes([]byte(updated))

	_, err := fixture.service.ActivateOverride(t.Context(), bundle.request, operator())
	require.ErrorIs(t, err, ErrRegistryInvalid)
	require.Zero(t, overrideRowCount(t, fixture))
}
