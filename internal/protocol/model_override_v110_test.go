package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelOverrideGateInvariants(t *testing.T) {
	t.Parallel()

	base := modelOverrideSelection(InferenceProfileBase)
	base.GateStatus = ModelGateStatusOverride
	require.Error(t, base.Validate(), "a base selection can never be an override")

	lora := modelOverrideSelection(InferenceProfileAnalystLoRA)
	require.NoError(t, lora.Validate())

	request := ActivateAdapterOverrideRequest{
		AdapterMetadata:     modelOverrideReference("adapter.json"),
		TrainingRunMetadata: modelOverrideReference("training.json"),
		EvaluationReport:    modelOverrideReference("report.json"),
		Reason:              strings.Repeat("r", 400),
	}
	require.NoError(t, request.Validate())
	request.Reason += "x"
	require.Error(t, request.Validate(), "the prefix must still fit the frozen 500-rune selection reason")
}

func TestModelOverrideNeedsV110Schemas(t *testing.T) {
	t.Parallel()

	selection := modelOverrideSelection(InferenceProfileAnalystLoRA)
	selectionRaw, err := json.Marshal(selection)
	require.NoError(t, err)
	activationRaw, err := json.Marshal(ModelActivationResult{Applied: true, Selection: selection})
	require.NoError(t, err)
	statusRaw, err := json.Marshal(ModelStatusResult{
		Selected: &selection,
		Deployment: ModelDeploymentMetadata{
			BaseRevision:    ModelMetadata{Availability: MetadataAvailabilityUnknown},
			AdapterRevision: ModelMetadata{Availability: MetadataAvailabilityUnknown},
			DatasetVersion:  ModelMetadata{Availability: MetadataAvailabilityUnknown},
			ABVariant:       ModelMetadata{Availability: MetadataAvailabilityUnknown},
		},
	})
	require.NoError(t, err)

	for _, pair := range []struct {
		published string
		twin      string
		raw       []byte
	}{
		{published: "model_selection", twin: "model_selection_v1_10", raw: selectionRaw},
		{published: "model_activation_result", twin: "model_activation_result_v1_10", raw: activationRaw},
		{published: "model_status_result_v1_5", twin: "model_status_result_v1_10", raw: statusRaw},
	} {
		t.Run(pair.published, func(t *testing.T) {
			t.Parallel()

			published := compileSchema(t, modelOverrideRoot(t, pair.published))
			twin := compileSchema(t, modelOverrideRoot(t, pair.twin))
			document := mustDoc(t, pair.raw)
			require.Error(t, published.Validate(document), "the published root must remain frozen")
			require.NoError(t, twin.Validate(document), "the v1.10 twin must advertise override")
		})
	}

	params := ActivateAdapterOverrideRequest{
		AdapterMetadata:     modelOverrideReference("adapter.json"),
		TrainingRunMetadata: modelOverrideReference("training.json"),
		EvaluationReport:    modelOverrideReference("report.json"),
		Reason:              "operator reviewed the evidence",
	}
	paramsRaw, err := json.Marshal(params)
	require.NoError(t, err)
	rpcRaw, err := json.Marshal(RPCRequest{
		JSONRPC: JSONRPCVersion,
		ID:      1,
		Method:  ControlMethodModelActivateOverride,
		Params:  paramsRaw,
	})
	require.NoError(t, err)
	document := mustDoc(t, rpcRaw)
	require.Error(t, compileSchema(t, modelOverrideRoot(t, "rpc_request_v1_9")).Validate(document))
	require.NoError(t, compileSchema(t, modelOverrideRoot(t, "rpc_request_v1_10")).Validate(document))
}

func modelOverrideRoot(t *testing.T, name string) Root {
	t.Helper()
	for _, root := range Roots() {
		if root.Name == name {
			return root
		}
	}
	t.Fatalf("missing protocol root %q", name)
	return Root{}
}

func modelOverrideReference(path string) ModelEvidenceReference {
	return ModelEvidenceReference{Path: path, SHA256: SHA256Hex(strings.Repeat("a", 64))}
}

func modelOverrideSelection(profile InferenceProfile) ModelSelection {
	selection := ModelSelection{
		Generation: 2,
		Profile: GatewayProfile{
			Contract: ModelContractAnalyzeAgentState, InferenceProfile: profile,
		},
		BaseModelRepo: "example.invalid/glimmer", BaseModelRevision: "base-1",
		Runtime: "runtime-v1", MLContractVersion: "ml-1",
		PromptVersion:           "prompt-1",
		ProjectionPolicyVersion: "projection-1", ProjectionPolicySHA256: SHA256Hex(strings.Repeat("1", 64)),
		ProfileRevision: "profile-1", ProfileSHA256: SHA256Hex(strings.Repeat("2", 64)),
		CalibrationRevision: "calibration-1", CalibrationSHA256: SHA256Hex(strings.Repeat("3", 64)),
		CalibrationTemperature: 1.2, IdleThreshold: 0.6, AnalysisPolicyVersion: "policy-1",
		GateStatus: ModelGateStatusBase, OperationalState: "READY", Reason: "configured base",
		SelectedAt: time.Date(2026, time.November, 3, 13, 0, 0, 0, time.UTC),
		EventID:    "01JB0000000000000000000002",
	}
	if profile == InferenceProfileAnalystLoRA {
		selection.Profile.AdapterName = "analyst"
		selection.Profile.AdapterRevision = "rev-1"
		selection.AdapterSHA256 = SHA256Hex(strings.Repeat("4", 64))
		selection.DatasetVersion = "dataset-1"
		selection.DatasetSHA256 = SHA256Hex(strings.Repeat("5", 64))
		selection.TrainingRunID = "training-1"
		selection.TrainingCodeRevision = "code-1"
		selection.EvaluationRunID = "evaluation-1"
		selection.EvaluationSHA256 = SHA256Hex(strings.Repeat("6", 64))
		selection.GateStatus = ModelGateStatusOverride
		selection.Reason = "operator override: reviewed evidence"
	}
	return selection
}
