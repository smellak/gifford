package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var (
	// ErrStateObservationNotFound means StateProducer has not committed a real
	// observation for this exact agent session. Legacy rows do not count.
	ErrStateObservationNotFound = errors.New("state observation not found")
	// ErrStateObservationInvalid means the latest real row is malformed or no
	// longer coherent. Readers never skip it to recover an older row.
	ErrStateObservationInvalid = errors.New("state observation invalid")
)

// StateEligibility values are stable machine-readable reasons explaining why
// the latest real snapshot is or is not automatically eligible.
const (
	StateEligibilityEligible          = "ELIGIBLE"
	StateEligibilityExpired           = "EXPIRED"
	StateEligibilityStale             = "STALE"
	StateEligibilitySessionChanged    = "SESSION_CHANGED"
	StateEligibilityGuardChanged      = "INPUT_GUARD_CHANGED"
	StateEligibilitySessionInactive   = "SESSION_INACTIVE"
	StateEligibilityUnhealthy         = "UNHEALTHY"
	StateEligibilityDeliveryUnsafe    = "DELIVERY_UNSAFE"
	StateEligibilityObservationNewer  = "NEWER_OBSERVATION_PENDING"
	StateEligibilityUnknownPhase      = "UNKNOWN_PHASE"
	StateEligibilityNotIdle           = "NOT_IDLE_AT_PROMPT"
	StateEligibilityNativeInformative = "NATIVE_INFORMATIVE_ONLY"
	StateEligibilityNoModelEvidence   = "NO_MODEL_EVIDENCE"
	StateEligibilityProfileChanged    = "PROFILE_CHANGED"
	StateEligibilityCallInvalid       = "GLIMMER_CALL_INVALID"
	StateEligibilityBelowThreshold    = "IDLE_BELOW_THRESHOLD"
	StateEligibilityHumanInput        = "HUMAN_INPUT_ACTIVE"
)

type stateQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// StateRead is the latest real StateProducer row plus its coherence evidence.
// AutomaticEligible is advisory for PR-E; stale and UNKNOWN rows remain
// available to audited human decisions.
type StateRead struct {
	Snapshot               protocol.StateSnapshot
	SessionID              protocol.OpaqueID
	ExpiresAt              time.Time
	ObservationID          protocol.OpaqueID
	ObservationGeneration  int64
	OutputGeneration       int64
	EventGeneration        int64
	ProducerVersion        string
	AnalysisRoute          string
	GlimmerCallID          protocol.OpaqueID
	InferenceProfile       protocol.InferenceProfile
	BaseModelRevision      string
	AdapterRevision        string
	AdapterSHA256          protocol.SHA256Hex
	ProfileRevision        string
	ProfileSHA256          protocol.SHA256Hex
	CalibrationRevision    string
	CalibrationSHA256      protocol.SHA256Hex
	CalibrationTemperature float64
	IdleThreshold          float64
	IdleProbability        float64
	PromptVersion          string
	AnalysisPolicyVersion  string
	AutomaticEligible      bool
	EligibilityReason      string

	sessionUpdatedAt string
	manifestSHA256   string
	modelFieldsValid bool
}

// ReadLatestState selects the newest real row first and only then validates
// freshness and evidence. It never adds WHERE clauses that could fall back to
// an older IDLE row when the latest row is stale or UNKNOWN.
func ReadLatestState(
	ctx context.Context,
	q stateQueryer,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	now time.Time,
	expected *StateAnalysisProfile,
) (StateRead, error) {
	if !agent.Valid() || !session.Valid() || q == nil {
		return StateRead{}, fmt.Errorf("read latest state: %w", ErrStateObservationInvalid)
	}
	var out StateRead
	var observedAt, expiresAt, lastHumanKey sql.NullString
	var confidenceJSON, evidenceJSON, channelJSON, loopsJSON string
	var stale, background bool
	var route, callID, inferenceProfile, baseRevision sql.NullString
	var adapterRevision, adapterSHA, profileRevision, profileSHA sql.NullString
	var calibrationRevision, calibrationSHA, promptVersion, policyVersion sql.NullString
	var temperature, threshold, idleProbability sql.NullFloat64
	err := q.QueryRowContext(ctx, `SELECT
		state_id, state_version, agent_id, observed_at, session_state,
		human_draft_state, human_activity_state, draft_generation,
		input_guard_version, last_human_key_at, agent_phase, attention_state,
		health_state, delivery_safety, background_activity,
		channel_effective_json, active_loops_json, pending_approvals,
		confidence_json, evidence_json, analyst_version, stale,
		session_id, session_updated_at, manifest_sha256, observation_id,
		observation_generation, output_generation, event_generation, expires_at,
		producer_version, analysis_route, glimmer_call_id, inference_profile,
		base_model_revision, adapter_revision, adapter_sha256, profile_revision,
		profile_sha256, calibration_revision, calibration_sha256,
		calibration_temperature, idle_threshold, idle_probability, prompt_version,
		analysis_policy_version
		FROM state_snapshots WHERE agent_id = ? AND session_id = ?
		AND producer_version IS NOT NULL
		ORDER BY observed_at DESC, state_version DESC LIMIT 1`, agent, session).Scan(
		&out.Snapshot.StateID, &out.Snapshot.StateVersion, &out.Snapshot.AgentID,
		&observedAt, &out.Snapshot.SessionState, &out.Snapshot.HumanDraftState,
		&out.Snapshot.HumanActivityState, &out.Snapshot.DraftGeneration,
		&out.Snapshot.InputGuardVersion, &lastHumanKey, &out.Snapshot.AgentPhase,
		&out.Snapshot.AttentionState, &out.Snapshot.HealthState,
		&out.Snapshot.DeliverySafety, &background, &channelJSON, &loopsJSON,
		&out.Snapshot.PendingApprovals, &confidenceJSON, &evidenceJSON,
		&out.Snapshot.AnalystVersion, &stale, &out.SessionID,
		&out.sessionUpdatedAt, &out.manifestSHA256, &out.ObservationID,
		&out.ObservationGeneration, &out.OutputGeneration, &out.EventGeneration,
		&expiresAt, &out.ProducerVersion, &route, &callID, &inferenceProfile,
		&baseRevision, &adapterRevision, &adapterSHA, &profileRevision, &profileSHA,
		&calibrationRevision, &calibrationSHA, &temperature, &threshold,
		&idleProbability, &promptVersion, &policyVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return StateRead{}, ErrStateObservationNotFound
	}
	if err != nil {
		return StateRead{}, fmt.Errorf("select latest state: %w", err)
	}
	out.Snapshot.BackgroundActivity, out.Snapshot.Stale = background, stale
	out.Snapshot.ObservedAt, err = eventstore.ParseTime(observedAt.String)
	if err != nil {
		return StateRead{}, invalidStateRow("observed_at")
	}
	out.ExpiresAt, err = eventstore.ParseTime(expiresAt.String)
	if err != nil {
		return StateRead{}, invalidStateRow("expires_at")
	}
	if !out.ExpiresAt.After(out.Snapshot.ObservedAt) ||
		out.ExpiresAt.After(out.Snapshot.ObservedAt.Add(stateTTL)) {
		return StateRead{}, invalidStateRow("expires_at")
	}
	if lastHumanKey.Valid {
		out.Snapshot.LastHumanKeyAt, err = eventstore.ParseTime(lastHumanKey.String)
		if err != nil {
			return StateRead{}, invalidStateRow("last_human_key_at")
		}
	}
	if json.Unmarshal([]byte(confidenceJSON), &out.Snapshot.Confidence) != nil ||
		json.Unmarshal([]byte(evidenceJSON), &out.Snapshot.EvidenceEventIDs) != nil ||
		json.Unmarshal([]byte(channelJSON), &out.Snapshot.ChannelEffective) != nil ||
		json.Unmarshal([]byte(loopsJSON), &out.Snapshot.ActiveLoops) != nil ||
		out.Snapshot.Validate() != nil || out.ProducerVersion != StateProducerVersion ||
		!out.SessionID.Valid() || !out.ObservationID.Valid() ||
		out.ObservationGeneration < 1 || out.OutputGeneration < 0 || out.EventGeneration < 0 {
		return StateRead{}, invalidStateRow("snapshot")
	}
	out.AnalysisRoute = route.String
	out.GlimmerCallID = protocol.OpaqueID(callID.String)
	out.InferenceProfile = protocol.InferenceProfile(inferenceProfile.String)
	out.BaseModelRevision = baseRevision.String
	out.AdapterRevision = adapterRevision.String
	out.AdapterSHA256 = protocol.SHA256Hex(adapterSHA.String)
	out.ProfileRevision = profileRevision.String
	out.ProfileSHA256 = protocol.SHA256Hex(profileSHA.String)
	out.CalibrationRevision = calibrationRevision.String
	out.CalibrationSHA256 = protocol.SHA256Hex(calibrationSHA.String)
	out.CalibrationTemperature = temperature.Float64
	out.IdleThreshold = threshold.Float64
	out.IdleProbability = idleProbability.Float64
	out.PromptVersion = promptVersion.String
	out.AnalysisPolicyVersion = policyVersion.String
	out.modelFieldsValid = callID.Valid && out.GlimmerCallID.Valid() && inferenceProfile.Valid &&
		out.InferenceProfile.Valid() && baseRevision.Valid && baseRevision.String != "" &&
		out.Snapshot.AnalystVersion == analyst.PromptVersion &&
		profileRevision.Valid && profileRevision.String != "" && out.ProfileSHA256.Valid() &&
		calibrationRevision.Valid && calibrationRevision.String != "" &&
		out.CalibrationSHA256.Valid() && temperature.Valid && threshold.Valid &&
		idleProbability.Valid && promptVersion.Valid && out.PromptVersion == analyst.PromptVersion &&
		policyVersion.Valid && out.AnalysisPolicyVersion == stateAnalysisPolicy &&
		out.CalibrationTemperature > 0 && !math.IsNaN(out.CalibrationTemperature) &&
		!math.IsInf(out.CalibrationTemperature, 0) && out.IdleThreshold > 0 &&
		out.IdleThreshold <= 1 && out.IdleProbability >= 0 && out.IdleProbability <= 1 &&
		!math.IsNaN(out.IdleProbability) && !math.IsInf(out.IdleProbability, 0)

	out.AutomaticEligible, out.EligibilityReason = evaluateStateEligibility(ctx, q, out, now, expected)
	return out, nil
}

func invalidStateRow(field string) error {
	return fmt.Errorf("%w: %s", ErrStateObservationInvalid, field)
}

func evaluateStateEligibility(
	ctx context.Context,
	q stateQueryer,
	state StateRead,
	now time.Time,
	expected *StateAnalysisProfile,
) (bool, string) {
	if state.Snapshot.Stale {
		return false, StateEligibilityStale
	}
	if !state.ExpiresAt.After(now) {
		return false, StateEligibilityExpired
	}
	var sessionState, updatedAt, manifestJSON string
	err := q.QueryRowContext(ctx, `SELECT state, updated_at, manifest_json FROM sessions
		WHERE session_id = ? AND agent_id = ?`, state.SessionID, state.Snapshot.AgentID).
		Scan(&sessionState, &updatedAt, &manifestJSON)
	if err != nil {
		return false, StateEligibilitySessionChanged
	}
	sum := sha256.Sum256([]byte(manifestJSON))
	if updatedAt != state.sessionUpdatedAt || hex.EncodeToString(sum[:]) != state.manifestSHA256 {
		return false, StateEligibilitySessionChanged
	}
	if sessionState != string(state.Snapshot.SessionState) {
		return false, StateEligibilitySessionChanged
	}
	if state.Snapshot.SessionState != protocol.SessionStateConnected {
		return false, StateEligibilitySessionInactive
	}
	var guardVersion, draftGeneration int64
	var draftState, activityState string
	var lastHumanKey sql.NullString
	var guardConfidence float64
	err = q.QueryRowContext(ctx, `SELECT version, draft_generation, draft_state,
		activity_state, last_human_key_at, confidence FROM input_guards
		WHERE session_id = ? AND agent_id = ?`, state.SessionID, state.Snapshot.AgentID).
		Scan(&guardVersion, &draftGeneration, &draftState, &activityState, &lastHumanKey, &guardConfidence)
	if err != nil || guardVersion != state.Snapshot.InputGuardVersion ||
		draftGeneration != state.Snapshot.DraftGeneration ||
		draftState != string(state.Snapshot.HumanDraftState) ||
		activityState != string(state.Snapshot.HumanActivityState) ||
		guardConfidence != state.Snapshot.Confidence.HumanDraftState {
		return false, StateEligibilityGuardChanged
	}
	if lastHumanKey.Valid {
		parsed, parseErr := eventstore.ParseTime(lastHumanKey.String)
		if parseErr != nil || !parsed.Equal(state.Snapshot.LastHumanKeyAt) {
			return false, StateEligibilityGuardChanged
		}
	} else if !state.Snapshot.LastHumanKeyAt.IsZero() {
		return false, StateEligibilityGuardChanged
	}
	if state.Snapshot.HumanDraftState != protocol.DraftStateClosed ||
		state.Snapshot.HumanActivityState == protocol.ActivityStateActive {
		return false, StateEligibilityHumanInput
	}
	if state.Snapshot.AgentPhase == protocol.AgentPhaseUnknown {
		return false, StateEligibilityUnknownPhase
	}
	if state.Snapshot.HealthState != protocol.HealthStateHealthy {
		return false, StateEligibilityUnhealthy
	}
	if state.AnalysisRoute == string(analyst.RouteNative) {
		if state.Snapshot.AnalystVersion != stateNativeAnalyst {
			return false, StateEligibilityNoModelEvidence
		}
		return false, StateEligibilityNativeInformative
	}
	if state.AnalysisRoute != string(analyst.RoutePTY) || !state.modelFieldsValid {
		return false, StateEligibilityNoModelEvidence
	}
	if state.Snapshot.AgentPhase != protocol.AgentPhaseIdleAtPrompt {
		return false, StateEligibilityNotIdle
	}
	if expected == nil || !expected.valid() ||
		state.ProfileRevision != expected.Revision || state.ProfileSHA256 != expected.SHA256 ||
		state.CalibrationRevision != expected.CalibrationRevision ||
		state.CalibrationSHA256 != expected.CalibrationSHA256 ||
		state.CalibrationTemperature != expected.Temperature ||
		state.IdleThreshold != expected.IdleThreshold {
		return false, StateEligibilityProfileChanged
	}
	var contract, agentID, profile, baseRevision, adapterRevision, adapterSHA, status string
	var probabilitiesJSON sql.NullString
	err = q.QueryRowContext(ctx, `SELECT contract, COALESCE(agent_id, ''),
		inference_profile, base_model_revision, COALESCE(adapter_revision, ''),
		COALESCE(adapter_sha256, ''), probabilities_json, status
		FROM glimmer_calls WHERE call_id = ?`,
		state.GlimmerCallID).Scan(&contract, &agentID, &profile, &baseRevision,
		&adapterRevision, &adapterSHA, &probabilitiesJSON, &status)
	if err != nil || contract != string(protocol.ModelContractAnalyzeAgentState) ||
		agentID != string(state.Snapshot.AgentID) || profile != string(state.InferenceProfile) ||
		baseRevision != state.BaseModelRevision || adapterRevision != state.AdapterRevision ||
		adapterSHA != string(state.AdapterSHA256) || status != string(protocol.GlimmerCallStatusOK) {
		return false, StateEligibilityCallInvalid
	}
	var probabilities map[string]float64
	if !probabilitiesJSON.Valid ||
		json.Unmarshal([]byte(probabilitiesJSON.String), &probabilities) != nil {
		return false, StateEligibilityCallInvalid
	}
	durableIdle, ok := calibratedProbability(
		probabilities, string(protocol.AgentPhaseIdleAtPrompt), state.CalibrationTemperature,
	)
	if !ok || math.Abs(durableIdle-state.IdleProbability) > 1e-12 {
		return false, StateEligibilityCallInvalid
	}
	if state.IdleProbability < state.IdleThreshold {
		return false, StateEligibilityBelowThreshold
	}
	if state.Snapshot.DeliverySafety != protocol.DeliverySafetySafe {
		return false, StateEligibilityDeliveryUnsafe
	}
	return true, StateEligibilityEligible
}

// LatestRealStateID returns the latest real row even when it is stale or
// UNKNOWN. Human decisions may use it for audit; automatic eligibility is a
// separate property on StateRead.
func LatestRealStateID(
	ctx context.Context,
	q stateQueryer,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	now time.Time,
) (protocol.ULID, error) {
	if !agent.Valid() || !session.Valid() {
		return "", ErrStateObservationNotFound
	}
	state, err := ReadLatestState(ctx, q, agent, session, now, nil)
	if err != nil {
		return "", err
	}
	return state.Snapshot.StateID, nil
}
