package gateway

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// SQLRecorder stores call records in the glimmer_calls table of the node
// database, one row per call. A row holds what the record holds: digests,
// counts, metadata and the distribution, never a payload, a prompt or an
// answer.
type SQLRecorder struct {
	db *sql.DB
}

// NewSQLRecorder returns a recorder over the node database.
func NewSQLRecorder(db *sql.DB) (*SQLRecorder, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: no database", ErrInvalidConfig)
	}
	return &SQLRecorder{db: db}, nil
}

// insertCallSQL writes one glimmer_calls row. An optional field the
// record leaves empty is NULL.
const insertCallSQL = `INSERT INTO glimmer_calls (
	call_id, contract, agent_id, inference_profile, base_model_repo,
	base_model_revision, adapter_name, adapter_revision, adapter_sha256,
	dataset_version, dataset_sha256, training_run_id, training_code_revision,
	selection_generation, evaluation_run_id, evaluation_sha256,
	projection_policy_version, projection_policy_sha256, profile_revision,
	profile_sha256, calibration_revision, calibration_sha256,
	calibration_temperature, idle_threshold, analysis_policy_version,
	quantization, runtime, runtime_version, gpu, request_id, latency_ms,
	redaction_policy_version, redactions_count, input_sha256, output_sha256,
	probabilities_json, status, created_at, prompt_version
) VALUES (
	?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
	?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
	?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
	?, ?, ?, ?, ?, ?, ?, ?, ?
)`

// RecordCall inserts rec, and promptVersion in the prompt_version column,
// NULL when it is empty. A record that does not validate, or a version
// the gateway would not send, is refused before it reaches the database.
func (r *SQLRecorder) RecordCall(ctx context.Context, rec protocol.GlimmerCallRecord, promptVersion string) error {
	if err := rec.Validate(); err != nil {
		return fmt.Errorf("record glimmer call: %w", err)
	}
	if promptVersion != "" && !validPromptVersion(promptVersion) {
		return errors.New("record glimmer call: invalid prompt version")
	}
	if rec.PromptVersion != "" && promptVersion != "" && rec.PromptVersion != promptVersion {
		return errors.New("record glimmer call: prompt version mismatch")
	}
	if rec.PromptVersion != "" {
		promptVersion = rec.PromptVersion
	}
	var probabilities sql.NullString
	if len(rec.Probabilities) > 0 {
		b, err := json.Marshal(rec.Probabilities)
		if err != nil {
			return fmt.Errorf("encode glimmer call probabilities: %w", err)
		}
		probabilities = sql.NullString{String: string(b), Valid: true}
	}
	_, err := r.db.ExecContext(ctx, insertCallSQL,
		string(rec.CallID), string(rec.Contract), orNull(string(rec.AgentID)), string(rec.InferenceProfile),
		rec.BaseModelRepo, rec.BaseModelRevision, orNull(rec.AdapterName), orNull(rec.AdapterRevision),
		orNull(string(rec.AdapterSHA256)), orNull(rec.DatasetVersion), orNull(string(rec.DatasetSHA256)),
		orNull(string(rec.TrainingRunID)), orNull(rec.TrainingCodeRevision), nullPositive(rec.SelectionGeneration),
		orNull(string(rec.EvaluationRunID)), orNull(string(rec.EvaluationSHA256)),
		orNull(rec.ProjectionPolicyVersion), orNull(string(rec.ProjectionPolicySHA256)),
		orNull(rec.ProfileRevision), orNull(string(rec.ProfileSHA256)),
		orNull(rec.CalibrationRevision), orNull(string(rec.CalibrationSHA256)),
		nullPositiveFloat(rec.CalibrationTemperature), nullPositiveFloat(rec.IdleThreshold),
		orNull(rec.AnalysisPolicyVersion), orNull(rec.Quantization),
		rec.Runtime, orNull(rec.RuntimeVersion), orNull(rec.GPU), orNull(string(rec.RequestID)),
		rec.LatencyMS, rec.RedactionPolicyVersion, rec.RedactionsCount,
		string(rec.InputSHA256), string(rec.OutputSHA256), probabilities, string(rec.Status),
		eventstore.FormatTime(rec.CreatedAt), orNull(promptVersion))
	if err != nil {
		return fmt.Errorf("insert glimmer call: %w", err)
	}
	return nil
}

// orNull is s, or NULL when s is empty.
func orNull(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

func nullPositive(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: v > 0} }

func nullPositiveFloat(v float64) sql.NullFloat64 {
	return sql.NullFloat64{Float64: v, Valid: v > 0}
}

// RandomCallIDs issues call ids from crypto/rand.
type RandomCallIDs struct{}

// NewCallID returns "call-" and 32 random hex digits.
func (RandomCallIDs) NewCallID() protocol.OpaqueID {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return protocol.OpaqueID("call-" + hex.EncodeToString(b[:]))
}
