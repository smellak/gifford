package protocol

import (
	"fmt"
	"time"
)

const modelStatusPassed = "passed"

// ModelGateCriterion is one immutable evaluation-gate verdict. Activatable
// adapters carry exactly criteria 0 through 5 and every one must be PASS.
type ModelGateCriterion struct {
	ID      int    `json:"id" jsonschema:"minimum=0,maximum=5"`
	Name    string `json:"name" jsonschema:"minLength=1,maxLength=200"`
	Verdict string `json:"verdict" jsonschema:"enum=PASS,enum=FAIL,enum=PENDING"`
}

// ModelSelection is the complete ANALYZE_AGENT_STATE tuple selected by the
// control plane. It is immutable once published. Base selections leave all
// adapter, dataset, training and evaluation fields empty.
type ModelSelection struct {
	Generation              int64          `json:"generation" jsonschema:"minimum=1"`
	Profile                 GatewayProfile `json:"profile"`
	AdapterSHA256           SHA256Hex      `json:"adapter_sha256,omitempty"`
	BaseModelRepo           string         `json:"base_model_repo" jsonschema:"minLength=1,maxLength=200"`
	BaseModelRevision       string         `json:"base_model_revision" jsonschema:"minLength=1,maxLength=100"`
	Runtime                 string         `json:"runtime" jsonschema:"minLength=1,maxLength=50"`
	Quantization            string         `json:"quantization,omitempty" jsonschema:"maxLength=50"`
	MLContractVersion       string         `json:"ml_contract_version" jsonschema:"minLength=1,maxLength=100"`
	DatasetVersion          string         `json:"dataset_version,omitempty" jsonschema:"minLength=1,maxLength=100"`
	DatasetSHA256           SHA256Hex      `json:"dataset_sha256,omitempty"`
	TrainingRunID           OpaqueID       `json:"training_run_id,omitempty"`
	TrainingCodeRevision    string         `json:"training_code_revision,omitempty" jsonschema:"minLength=1,maxLength=100"`
	EvaluationRunID         OpaqueID       `json:"evaluation_run_id,omitempty"`
	EvaluationSHA256        SHA256Hex      `json:"evaluation_sha256,omitempty"`
	PromptVersion           string         `json:"prompt_version" jsonschema:"minLength=1,maxLength=64"`
	ProjectionPolicyVersion string         `json:"projection_policy_version" jsonschema:"minLength=1,maxLength=100"`
	ProjectionPolicySHA256  SHA256Hex      `json:"projection_policy_sha256"`
	ProfileRevision         string         `json:"profile_revision" jsonschema:"minLength=1,maxLength=100"`
	ProfileSHA256           SHA256Hex      `json:"profile_sha256"`
	CalibrationRevision     string         `json:"calibration_revision" jsonschema:"minLength=1,maxLength=100"`
	CalibrationSHA256       SHA256Hex      `json:"calibration_sha256"`
	CalibrationTemperature  float64        `json:"calibration_temperature" jsonschema:"exclusiveMinimum=0,maximum=100"`
	IdleThreshold           float64        `json:"idle_threshold" jsonschema:"exclusiveMinimum=0,maximum=1"`
	AnalysisPolicyVersion   string         `json:"analysis_policy_version" jsonschema:"minLength=1,maxLength=100"`
	GateStatus              string         `json:"gate_status" jsonschema:"enum=base,enum=passed"`
	OperationalState        string         `json:"operational_state" jsonschema:"enum=READY,enum=DEGRADED,enum=UNKNOWN"`
	Reason                  string         `json:"reason" jsonschema:"minLength=1,maxLength=500"`
	SelectedAt              time.Time      `json:"selected_at"`
	EventID                 ULID           `json:"event_id"`
}

// Validate checks the selected tuple and its base/adapter invariants.
func (s ModelSelection) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if err := s.Profile.Validate(); err != nil {
		return prefixPath(err, "profile")
	}
	if s.Profile.Contract != ModelContractAnalyzeAgentState {
		return &ValidationError{Path: "profile.contract", Reason: "only ANALYZE_AGENT_STATE is selectable"}
	}
	lora := s.Profile.InferenceProfile == InferenceProfileAnalystLoRA
	completeAdapterEvidence := s.AdapterSHA256 != "" && s.DatasetVersion != "" && s.DatasetSHA256 != "" &&
		s.TrainingRunID != "" && s.TrainingCodeRevision != "" && s.EvaluationRunID != "" && s.EvaluationSHA256 != ""
	anyAdapterEvidence := s.AdapterSHA256 != "" || s.DatasetVersion != "" || s.DatasetSHA256 != "" ||
		s.TrainingRunID != "" || s.TrainingCodeRevision != "" || s.EvaluationRunID != "" || s.EvaluationSHA256 != ""
	if (lora && !completeAdapterEvidence) || (!lora && anyAdapterEvidence) {
		return &ValidationError{Path: "adapter_sha256", Reason: "adapter provenance is required on analyst-lora only"}
	}
	if lora && s.GateStatus != modelStatusPassed {
		return &ValidationError{Path: "gate_status", Reason: "analyst-lora requires a passed gate"}
	}
	if !lora && s.GateStatus != "base" {
		return &ValidationError{Path: "gate_status", Reason: "base selection requires the base gate"}
	}
	return nil
}

// RegisterModelMetadataRequest imports one sealed adapter, dataset, training
// and evaluation binding. The large JSON fields contain metadata only; model
// weights remain in the serving system.
type RegisterModelMetadataRequest struct {
	AdapterName             string               `json:"adapter_name" jsonschema:"minLength=1,maxLength=100"`
	AdapterRevision         string               `json:"adapter_revision" jsonschema:"minLength=1,maxLength=100"`
	AdapterSHA256           SHA256Hex            `json:"adapter_sha256"`
	Contract                ModelContract        `json:"contract"`
	BaseModelRepo           string               `json:"base_model_repo" jsonschema:"minLength=1,maxLength=200"`
	BaseModelRevision       string               `json:"base_model_revision" jsonschema:"minLength=1,maxLength=100"`
	Runtime                 string               `json:"runtime" jsonschema:"minLength=1,maxLength=50"`
	Quantization            string               `json:"quantization,omitempty" jsonschema:"maxLength=50"`
	MLContractVersion       string               `json:"ml_contract_version" jsonschema:"minLength=1,maxLength=100"`
	DatasetVersion          string               `json:"dataset_version" jsonschema:"minLength=1,maxLength=100"`
	DatasetSHA256           SHA256Hex            `json:"dataset_sha256"`
	DatasetReadinessStatus  string               `json:"dataset_readiness_status" jsonschema:"enum=passed,enum=failed"`
	DatasetReadinessSHA256  SHA256Hex            `json:"dataset_readiness_sha256"`
	TrainingRunID           OpaqueID             `json:"training_run_id"`
	TrainingCodeRevision    string               `json:"training_code_revision" jsonschema:"minLength=1,maxLength=100"`
	TrainingReadinessStatus string               `json:"training_readiness_status" jsonschema:"enum=passed,enum=underpowered"`
	TrainingStatus          string               `json:"training_status" jsonschema:"enum=completed,enum=failed,enum=aborted,enum=running"`
	EvaluationRunID         OpaqueID             `json:"evaluation_run_id"`
	EvaluationSHA256        SHA256Hex            `json:"evaluation_sha256"`
	EvaluationReport        Payload              `json:"evaluation_report"`
	Gate                    []ModelGateCriterion `json:"gate" jsonschema:"minItems=6,maxItems=6"`
	PromptVersion           string               `json:"prompt_version" jsonschema:"minLength=1,maxLength=64"`
	ProjectionPolicyVersion string               `json:"projection_policy_version" jsonschema:"minLength=1,maxLength=100"`
	ProjectionPolicySHA256  SHA256Hex            `json:"projection_policy_sha256"`
	ProfileRevision         string               `json:"profile_revision" jsonschema:"minLength=1,maxLength=100"`
	ProfileSHA256           SHA256Hex            `json:"profile_sha256"`
	CalibrationRevision     string               `json:"calibration_revision" jsonschema:"minLength=1,maxLength=100"`
	CalibrationSHA256       SHA256Hex            `json:"calibration_sha256"`
	CalibrationTemperature  float64              `json:"calibration_temperature" jsonschema:"exclusiveMinimum=0,maximum=100"`
	IdleThreshold           float64              `json:"idle_threshold" jsonschema:"exclusiveMinimum=0,maximum=1"`
	AnalysisPolicyVersion   string               `json:"analysis_policy_version" jsonschema:"minLength=1,maxLength=100"`
}

// Validate checks the closed activation evidence. Content digests and registry
// immutability are checked again by the writer inside its transaction.
func (r RegisterModelMetadataRequest) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.Contract != ModelContractAnalyzeAgentState {
		return &ValidationError{Path: pathContract, Reason: "only ANALYZE_AGENT_STATE is activatable"}
	}
	seen := [6]bool{}
	for i, criterion := range r.Gate {
		if criterion.ID < 0 || criterion.ID > 5 || seen[criterion.ID] {
			return &ValidationError{Path: fmt.Sprintf("gate[%d].id", i), Reason: "criteria 0 through 5 are required exactly once"}
		}
		seen[criterion.ID] = true
		if criterion.Verdict != "PASS" {
			return &ValidationError{Path: fmt.Sprintf("gate[%d].verdict", i), Reason: "every activation criterion must PASS"}
		}
	}
	for _, present := range seen {
		if !present {
			return &ValidationError{Path: "gate", Reason: "criteria 0 through 5 are required exactly once"}
		}
	}
	if r.DatasetReadinessStatus != modelStatusPassed {
		return &ValidationError{Path: "dataset_readiness_status", Reason: "dataset readiness must pass"}
	}
	if r.TrainingReadinessStatus != modelStatusPassed {
		return &ValidationError{Path: "training_readiness_status", Reason: "underpowered training is not activatable"}
	}
	if r.TrainingStatus != "completed" {
		return &ValidationError{Path: "training_status", Reason: "training must be completed"}
	}
	return nil
}

// ModelMetadataRegistrationResult reports whether an immutable registry row
// was inserted or already existed byte-for-byte.
type ModelMetadataRegistrationResult struct {
	AdapterName     string    `json:"adapter_name"`
	AdapterRevision string    `json:"adapter_revision"`
	Imported        bool      `json:"imported"`
	RegisteredAt    time.Time `json:"registered_at"`
}

// Validate checks the registry result.
func (r ModelMetadataRegistrationResult) Validate() error { return checkRoot(&r) }

// ModelActivationResult is the result of activate, rollback or an automatic
// fallback. Applied=false is an explicit idempotent no-op and retains the
// durable event reference of the selected generation.
type ModelActivationResult struct {
	Applied   bool           `json:"applied"`
	Selection ModelSelection `json:"selection"`
}

// Validate checks the mutation result.
func (r ModelActivationResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	return r.Selection.Validate()
}
