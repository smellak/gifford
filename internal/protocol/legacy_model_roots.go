package protocol

import "time"

// These private views preserve the byte-level contracts-v1 schemas while the
// runtime uses the additive contracts-v1.5 model provenance fields. They are
// used only by the root registry and its fixture tests.

type legacyInferResult struct {
	Contract         ModelContract           `json:"contract"`
	InferenceProfile InferenceProfile        `json:"inference_profile"`
	Label            string                  `json:"label,omitempty"`
	Probabilities    map[string]float64      `json:"probabilities,omitempty"`
	Evidence         []string                `json:"evidence,omitempty" jsonschema:"maxItems=3"`
	CallRecord       legacyGlimmerCallRecord `json:"call_record"`
}

func (r legacyInferResult) Validate() error {
	return InferResult{
		Contract:         r.Contract,
		InferenceProfile: r.InferenceProfile,
		Label:            r.Label,
		Probabilities:    r.Probabilities,
		Evidence:         r.Evidence,
		CallRecord:       r.CallRecord.current(),
	}.Validate()
}

type legacyGlimmerCallRecord struct {
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

func (r legacyGlimmerCallRecord) Validate() error { return r.current().Validate() }

func (r legacyGlimmerCallRecord) current() GlimmerCallRecord {
	return GlimmerCallRecord{
		CallID:                 r.CallID,
		Contract:               r.Contract,
		AgentID:                r.AgentID,
		InferenceProfile:       r.InferenceProfile,
		BaseModelRepo:          r.BaseModelRepo,
		BaseModelRevision:      r.BaseModelRevision,
		AdapterName:            r.AdapterName,
		AdapterRevision:        r.AdapterRevision,
		AdapterSHA256:          r.AdapterSHA256,
		DatasetVersion:         r.DatasetVersion,
		DatasetSHA256:          r.DatasetSHA256,
		TrainingRunID:          r.TrainingRunID,
		TrainingCodeRevision:   r.TrainingCodeRevision,
		Quantization:           r.Quantization,
		Runtime:                r.Runtime,
		RuntimeVersion:         r.RuntimeVersion,
		GPU:                    r.GPU,
		RequestID:              r.RequestID,
		LatencyMS:              r.LatencyMS,
		RedactionPolicyVersion: r.RedactionPolicyVersion,
		RedactionsCount:        r.RedactionsCount,
		InputSHA256:            r.InputSHA256,
		OutputSHA256:           r.OutputSHA256,
		Probabilities:          r.Probabilities,
		Status:                 r.Status,
		CreatedAt:              r.CreatedAt,
	}
}

type legacyModelStatusResult struct {
	ActiveProfile *ActiveModelProfile     `json:"active_profile" jsonschema:"nullable"`
	LastCall      *ModelLastCall          `json:"last_call" jsonschema:"nullable"`
	Deployment    ModelDeploymentMetadata `json:"deployment"`
}

func (r legacyModelStatusResult) Validate() error { return checkRoot(&r) }
