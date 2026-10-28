package protocol

import (
	"fmt"
	"time"
	"unicode/utf8"
)

// This file holds the Model Gateway contracts of §5.8: the serialised
// infer, profile and adapter calls, and the call record.

// InferRequest asks the Gateway to run one model contract.
type InferRequest struct {
	Contract ModelContract `json:"contract"`
	AgentID  AgentID       `json:"agent_id,omitempty"`
	Payload  Payload       `json:"payload"`
}

// Validate checks the request, including the orchestration payload shape.
func (r InferRequest) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.Contract == ModelContractChooseOrchestrationAction {
		var orq OrchestrationRequest
		if err := decodeStrict(r.Payload, &orq); err != nil {
			return &ValidationError{Path: "payload", Reason: fmt.Sprintf("bad payload: %v", err)}
		}
		if err := orq.Validate(); err != nil {
			return prefixPath(err, "payload")
		}
	}
	return nil
}

// InferResult is one Gateway answer.
type InferResult struct {
	Contract         ModelContract      `json:"contract"`
	InferenceProfile InferenceProfile   `json:"inference_profile"`
	Label            string             `json:"label,omitempty"`
	Probabilities    map[string]float64 `json:"probabilities,omitempty"`
	Evidence         []string           `json:"evidence,omitempty" jsonschema:"maxItems=3"`
	CallRecord       GlimmerCallRecord  `json:"call_record"`
}

// Validate checks the result against its contract label set.
func (r InferResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	for i, e := range r.Evidence {
		if n := utf8.RuneCountInString(e); n < 1 || n > 120 {
			return &ValidationError{Path: fmt.Sprintf("evidence[%d]", i), Reason: "each entry is 1–120 characters"}
		}
	}
	set, ok := contractLabels(r.Contract)
	if !ok {
		return &ValidationError{Path: pathContract, Reason: "unknown contract"}
	}
	if r.CallRecord.Status == GlimmerCallStatusOK {
		if r.Label == "" {
			return &ValidationError{Path: "label", Reason: "required when the call succeeded"}
		}
		if !set[r.Label] {
			return &ValidationError{Path: "label", Reason: "outside the contract label set"}
		}
	}
	sum := 0.0
	for k, p := range r.Probabilities {
		if !set[k] {
			return &ValidationError{Path: fmt.Sprintf("probabilities[%s]", k), Reason: "outside the contract label set"}
		}
		if p < 0 || p > 1 {
			return &ValidationError{Path: fmt.Sprintf("probabilities[%s]", k), Reason: reasonRange01}
		}
		sum += p
	}
	if len(r.Probabilities) > 0 && (sum < 0.999 || sum > 1.001) {
		return &ValidationError{Path: pathProbabilities, Reason: "must sum to 1"}
	}
	if r.CallRecord.Contract != r.Contract {
		return &ValidationError{Path: "call_record", Reason: "contract must match"}
	}
	if r.CallRecord.InferenceProfile != r.InferenceProfile {
		return &ValidationError{Path: "call_record", Reason: "inference profile must match"}
	}
	return nil
}

// contractLabels returns the label set of a model contract.
func contractLabels(contract ModelContract) (map[string]bool, bool) {
	switch contract {
	case ModelContractAnalyzeAgentState:
		return map[string]bool{
			string(AgentPhaseIdleAtPrompt): true, string(AgentPhaseThinking): true,
			string(AgentPhaseRunningTool): true, string(AgentPhaseWaitingApproval): true,
			string(AgentPhaseAskingQuestion): true, string(AgentPhaseErrorOrExited): true,
			string(AgentPhaseUnknown): true,
		}, true
	case ModelContractTriageMessage:
		return map[string]bool{
			string(TriageLabelNormal): true, string(TriageLabelConflictOrUrgent): true,
			string(TriageLabelLowPriority): true, string(TriageLabelRequiresHuman): true,
		}, true
	case ModelContractChooseOrchestrationAction:
		set := make(map[string]bool)
		for _, a := range OrchestratorAction("").EnumValues() {
			set[a] = true
		}
		return set, true
	default:
		return nil, false
	}
}

// GatewayProfile describes which profile serves a contract.
type GatewayProfile struct {
	Contract         ModelContract    `json:"contract"`
	InferenceProfile InferenceProfile `json:"inference_profile"`
	AdapterName      string           `json:"adapter_name,omitempty" jsonschema:"minLength=1,maxLength=100"`
	AdapterRevision  string           `json:"adapter_revision,omitempty" jsonschema:"minLength=1,maxLength=100"`
}

// Validate checks the profile.
func (p GatewayProfile) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	hasAdapter := p.AdapterName != "" || p.AdapterRevision != ""
	if p.InferenceProfile == InferenceProfileAnalystLoRA {
		if p.AdapterName == "" || p.AdapterRevision == "" {
			return &ValidationError{Path: pathAdapterName, Reason: "analyst-lora needs an adapter"}
		}
		if p.Contract != ModelContractAnalyzeAgentState {
			return &ValidationError{Path: "contract", Reason: "analyst-lora serves ANALYZE_AGENT_STATE only"}
		}
	} else if hasAdapter {
		return &ValidationError{Path: pathAdapterName, Reason: "base has no adapter"}
	}
	return nil
}

// ActivateAdapterRequest asks the Gateway to activate an adapter revision.
type ActivateAdapterRequest struct {
	AdapterName     string `json:"adapter_name" jsonschema:"minLength=1,maxLength=100"`
	AdapterRevision string `json:"adapter_revision" jsonschema:"minLength=1,maxLength=100"`
}

// Validate checks the request.
func (r ActivateAdapterRequest) Validate() error {
	return checkRoot(&r)
}

// RollbackToBaseRequest asks the Gateway to drop the adapter.
type RollbackToBaseRequest struct {
	Contract ModelContract `json:"contract"`
}

// Validate checks the request.
func (r RollbackToBaseRequest) Validate() error {
	return checkRoot(&r)
}

// GlimmerCallRecord is one glimmer_calls row.
type GlimmerCallRecord struct {
	CallID                 OpaqueID           `json:"call_id"`
	Contract               ModelContract      `json:"contract"`
	AgentID                AgentID            `json:"agent_id,omitempty"`
	InferenceProfile       InferenceProfile   `json:"inference_profile"`
	BaseModelRepo          string             `json:"base_model_repo" jsonschema:"minLength=1,maxLength=200"`
	BaseModelRevision      string             `json:"base_model_revision" jsonschema:"minLength=1,maxLength=100"`
	AdapterName            string             `json:"adapter_name,omitempty" jsonschema:"minLength=1,maxLength=100"`
	AdapterRevision        string             `json:"adapter_revision,omitempty" jsonschema:"minLength=1,maxLength=100"`
	AdapterSHA256          SHA256Hex          `json:"adapter_sha256,omitempty"`
	DatasetVersion         string             `json:"dataset_version,omitempty" jsonschema:"minLength=1,maxLength=100"`
	DatasetSHA256          SHA256Hex          `json:"dataset_sha256,omitempty"`
	TrainingRunID          OpaqueID           `json:"training_run_id,omitempty"`
	TrainingCodeRevision   string             `json:"training_code_revision,omitempty" jsonschema:"minLength=1,maxLength=100"`
	Quantization           string             `json:"quantization,omitempty" jsonschema:"minLength=1,maxLength=50"`
	Runtime                string             `json:"runtime" jsonschema:"minLength=1,maxLength=50"`
	RuntimeVersion         string             `json:"runtime_version,omitempty" jsonschema:"minLength=1,maxLength=50"`
	GPU                    string             `json:"gpu,omitempty" jsonschema:"minLength=1,maxLength=50"`
	RequestID              OpaqueID           `json:"request_id,omitempty"`
	LatencyMS              int64              `json:"latency_ms" jsonschema:"minimum=0"`
	RedactionPolicyVersion string             `json:"redaction_policy_version" jsonschema:"minLength=1,maxLength=32"`
	RedactionsCount        int64              `json:"redactions_count" jsonschema:"minimum=0"`
	InputSHA256            SHA256Hex          `json:"input_sha256"`
	OutputSHA256           SHA256Hex          `json:"output_sha256"`
	Probabilities          map[string]float64 `json:"probabilities,omitempty"`
	Status                 GlimmerCallStatus  `json:"status"`
	CreatedAt              time.Time          `json:"created_at"`
}

// Validate checks the call record.
func (r GlimmerCallRecord) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	for k, p := range r.Probabilities {
		if p < 0 || p > 1 {
			return &ValidationError{Path: fmt.Sprintf("probabilities[%s]", k), Reason: reasonRange01}
		}
	}
	if r.InferenceProfile == InferenceProfileBase {
		if r.AdapterName != "" || r.AdapterRevision != "" || r.AdapterSHA256 != "" ||
			r.DatasetVersion != "" || r.DatasetSHA256 != "" || r.TrainingRunID != "" {
			return &ValidationError{Path: pathAdapterName, Reason: "base records carry no adapter"}
		}
		return nil
	}
	if r.Contract != ModelContractAnalyzeAgentState {
		return &ValidationError{Path: "contract", Reason: "analyst-lora serves ANALYZE_AGENT_STATE only"}
	}
	if r.AdapterName == "" || r.AdapterRevision == "" || r.AdapterSHA256 == "" {
		return &ValidationError{Path: pathAdapterName, Reason: "analyst-lora needs adapter name, revision and sha"}
	}
	return nil
}
