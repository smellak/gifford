package protocol

import "strings"

// AgentRunMode distinguishes a Gifford-managed run from observation only.
type AgentRunMode string

// AgentRunMode values.
const (
	AgentRunModeRun      AgentRunMode = "run"
	AgentRunModeObserved AgentRunMode = "observed"
)

// EnumValues returns the modes in table order.
func (AgentRunMode) EnumValues() []string {
	return []string{string(AgentRunModeRun), string(AgentRunModeObserved)}
}

// Valid reports whether the value is a known mode.
func (v AgentRunMode) Valid() bool {
	return v == AgentRunModeRun || v == AgentRunModeObserved
}

// AgentPhaseSource names the evidence source of a panel phase.
type AgentPhaseSource string

// AgentPhaseSource values.
const (
	AgentPhaseSourceAnalyst AgentPhaseSource = "analyst"
	AgentPhaseSourceNone    AgentPhaseSource = noneWord
)

// EnumValues returns the sources in table order.
func (AgentPhaseSource) EnumValues() []string {
	return []string{string(AgentPhaseSourceAnalyst), string(AgentPhaseSourceNone)}
}

// Valid reports whether the value is a known source.
func (v AgentPhaseSource) Valid() bool {
	return v == AgentPhaseSourceAnalyst || v == AgentPhaseSourceNone
}

// PaneLockPhase is the durable P9 phase of a cooperative tmux pane.
type PaneLockPhase string

// PaneLockPhase values mirror the on-disk pane-lock protocol.
const (
	PaneLockPhaseIdle      PaneLockPhase = "IDLE"
	PaneLockPhaseRunning   PaneLockPhase = "EN_CURSO"
	PaneLockPhaseDone      PaneLockPhase = "HECHO"
	PaneLockPhaseUncertain PaneLockPhase = "INCIERTO"
	PaneLockPhaseDeferred  PaneLockPhase = "APLAZADO"
)

// EnumValues returns the phases in protocol order.
func (PaneLockPhase) EnumValues() []string {
	return []string{
		string(PaneLockPhaseIdle),
		string(PaneLockPhaseRunning),
		string(PaneLockPhaseDone),
		string(PaneLockPhaseUncertain),
		string(PaneLockPhaseDeferred),
	}
}

// Valid reports whether the value is a known P9 phase.
func (v PaneLockPhase) Valid() bool {
	switch v {
	case PaneLockPhaseIdle, PaneLockPhaseRunning, PaneLockPhaseDone,
		PaneLockPhaseUncertain, PaneLockPhaseDeferred:
		return true
	default:
		return false
	}
}

// DeliveryQueueState names a recipient-specific delivery state.
type DeliveryQueueState string

// DeliveryQueueState values.
const (
	DeliveryQueueStateQueued     DeliveryQueueState = "QUEUED"
	DeliveryQueueStateClaimed    DeliveryQueueState = "CLAIMED"
	DeliveryQueueStateDelivering DeliveryQueueState = "DELIVERING"
	DeliveryQueueStateRetryWait  DeliveryQueueState = "RETRY_WAIT"
	DeliveryQueueStateDelivered  DeliveryQueueState = "DELIVERED"
	DeliveryQueueStateFailed     DeliveryQueueState = "FAILED"
	DeliveryQueueStateExpired    DeliveryQueueState = "EXPIRED"
	DeliveryQueueStateCancelled  DeliveryQueueState = "CANCELLED"
)

// EnumValues returns the states in lifecycle order.
func (DeliveryQueueState) EnumValues() []string {
	return []string{
		string(DeliveryQueueStateQueued),
		string(DeliveryQueueStateClaimed),
		string(DeliveryQueueStateDelivering),
		string(DeliveryQueueStateRetryWait),
		string(DeliveryQueueStateDelivered),
		string(DeliveryQueueStateFailed),
		string(DeliveryQueueStateExpired),
		string(DeliveryQueueStateCancelled),
	}
}

// Valid reports whether the value is a known state.
func (v DeliveryQueueState) Valid() bool {
	switch v {
	case DeliveryQueueStateQueued, DeliveryQueueStateClaimed, DeliveryQueueStateDelivering,
		DeliveryQueueStateRetryWait, DeliveryQueueStateDelivered, DeliveryQueueStateFailed,
		DeliveryQueueStateExpired, DeliveryQueueStateCancelled:
		return true
	default:
		return false
	}
}

// DecisionType distinguishes Channel and Orchestrator decisions.
type DecisionType string

// DecisionType values.
const (
	DecisionTypeChannel      DecisionType = "channel"
	DecisionTypeOrchestrator DecisionType = "orchestrator"
)

// EnumValues returns the decision types in table order.
func (DecisionType) EnumValues() []string {
	return []string{string(DecisionTypeChannel), string(DecisionTypeOrchestrator)}
}

// Valid reports whether the value is a known decision type.
func (v DecisionType) Valid() bool {
	return v == DecisionTypeChannel || v == DecisionTypeOrchestrator
}

// DeliveryDecisionMode names the source of an Orchestrator delivery
// decision. Fallback decisions use deterministic policy and guards without a
// Glimmer choice call.
type DeliveryDecisionMode string

// DeliveryDecisionMode values.
const (
	DeliveryDecisionModeGlimmer  DeliveryDecisionMode = "GLIMMER"
	DeliveryDecisionModeFallback DeliveryDecisionMode = "FALLBACK"
)

// EnumValues returns the decision modes in display order.
func (DeliveryDecisionMode) EnumValues() []string {
	return []string{string(DeliveryDecisionModeGlimmer), string(DeliveryDecisionModeFallback)}
}

// Valid reports whether the value is a known delivery decision mode.
func (v DeliveryDecisionMode) Valid() bool {
	return v == DeliveryDecisionModeGlimmer || v == DeliveryDecisionModeFallback
}

// DeliveryDecisionSource names what decided an Orchestrator delivery
// decision, as the judge reads it: the deterministic detector alone, the
// detector corroborating an analyst reading below its threshold, or the
// analyst's model through a recorded Glimmer call.
type DeliveryDecisionSource string

// DeliveryDecisionSource values.
const (
	DeliveryDecisionSourceDetector      DeliveryDecisionSource = "detector"
	DeliveryDecisionSourceCorroboration DeliveryDecisionSource = "detector+analyst-corroboration"
	DeliveryDecisionSourceAnalyst       DeliveryDecisionSource = "analyst"
	// DeliveryDecisionSourceAnalystUnavailable marks a decision under an
	// analyst policy that no analysis reached: the model failed or the
	// screen stayed stale, and the deterministic guards decided alone.
	DeliveryDecisionSourceAnalystUnavailable DeliveryDecisionSource = "analyst-unavailable"
)

// AnalystPolicyPrefix opens every policy_version of the analyst route.
const AnalystPolicyPrefix = "automatic-"

// EnumValues returns the sources in display order.
func (DeliveryDecisionSource) EnumValues() []string {
	return []string{
		string(DeliveryDecisionSourceDetector),
		string(DeliveryDecisionSourceCorroboration),
		string(DeliveryDecisionSourceAnalyst),
		string(DeliveryDecisionSourceAnalystUnavailable),
	}
}

// Valid reports whether the value is a known delivery decision source.
func (v DeliveryDecisionSource) Valid() bool {
	switch v {
	case DeliveryDecisionSourceDetector, DeliveryDecisionSourceCorroboration,
		DeliveryDecisionSourceAnalyst, DeliveryDecisionSourceAnalystUnavailable:
		return true
	default:
		return false
	}
}

// Policy versions the Orchestrator records on detector-route decisions.
const (
	// DeterministicPTYPolicyVersion marks a PTY delivery the detector alone
	// decided, without an orchestration model.
	DeterministicPTYPolicyVersion = "deterministic-pty-v1"
	// CorroboratedPTYPolicyVersion marks a PTY delivery the analyst found
	// idle below its threshold and the detector corroborated.
	CorroboratedPTYPolicyVersion = DeterministicPTYPolicyVersion + "+analyst"
)

// DeliveryDecisionSourceOf derives the source from what the decision row
// and its state snapshot record: the corroboration policy names itself in
// policy_version; a recorded Glimmer call means the analyst's model chose;
// an analyst policy whose state snapshot records an analysis call means the
// analyst's reading decided (a deferral on it makes no orchestration call);
// an analyst policy without either call means no analysis reached the
// decision; anything else was the detector and the deterministic guards
// alone.
func DeliveryDecisionSourceOf(policy ModelMetadataValue, call, stateCall OpaqueID) DeliveryDecisionSource {
	switch {
	case string(policy) == CorroboratedPTYPolicyVersion:
		return DeliveryDecisionSourceCorroboration
	case call != "":
		return DeliveryDecisionSourceAnalyst
	case strings.HasPrefix(string(policy), AnalystPolicyPrefix) && stateCall != "":
		// No orchestration call, but the state that decided came from the
		// analyst's own analysis call: a deferral on its reading is still
		// the analyst's.
		return DeliveryDecisionSourceAnalyst
	case strings.HasPrefix(string(policy), AnalystPolicyPrefix):
		return DeliveryDecisionSourceAnalystUnavailable
	default:
		return DeliveryDecisionSourceDetector
	}
}

// DeliveryDecisionResult is the durable state of the attempt created by an
// Orchestrator delivery decision. It deliberately includes UNCERTAIN: an
// ambiguous commit is a visible terminal result and never an implicit retry.
type DeliveryDecisionResult string

// DeliveryDecisionResult values.
const (
	DeliveryDecisionResultPreparing            DeliveryDecisionResult = "PREPARING"
	DeliveryDecisionResultPrepared             DeliveryDecisionResult = "PREPARED"
	DeliveryDecisionResultCommitSent           DeliveryDecisionResult = "COMMIT_SENT"
	DeliveryDecisionResultCommitted            DeliveryDecisionResult = "COMMITTED"
	DeliveryDecisionResultCommitNotConsumed    DeliveryDecisionResult = "COMMIT_NOT_CONSUMED"
	DeliveryDecisionResultAbortedHumanPriority DeliveryDecisionResult = "ABORTED_HUMAN_PRIORITY"
	DeliveryDecisionResultCancelled            DeliveryDecisionResult = "CANCELLED"
	DeliveryDecisionResultFailed               DeliveryDecisionResult = "FAILED"
	DeliveryDecisionResultUncertain            DeliveryDecisionResult = "UNCERTAIN"
)

// EnumValues returns attempt results in lifecycle order.
func (DeliveryDecisionResult) EnumValues() []string {
	return []string{
		string(DeliveryDecisionResultPreparing),
		string(DeliveryDecisionResultPrepared),
		string(DeliveryDecisionResultCommitSent),
		string(DeliveryDecisionResultCommitted),
		string(DeliveryDecisionResultCommitNotConsumed),
		string(DeliveryDecisionResultAbortedHumanPriority),
		string(DeliveryDecisionResultCancelled),
		string(DeliveryDecisionResultFailed),
		string(DeliveryDecisionResultUncertain),
	}
}

// Valid reports whether the value is a known durable attempt result.
func (v DeliveryDecisionResult) Valid() bool {
	switch v {
	case DeliveryDecisionResultPreparing, DeliveryDecisionResultPrepared,
		DeliveryDecisionResultCommitSent, DeliveryDecisionResultCommitted,
		DeliveryDecisionResultCommitNotConsumed, DeliveryDecisionResultAbortedHumanPriority,
		DeliveryDecisionResultCancelled, DeliveryDecisionResultFailed,
		DeliveryDecisionResultUncertain:
		return true
	default:
		return false
	}
}

// Terminal reports whether no automatic transition can follow this result.
func (v DeliveryDecisionResult) Terminal() bool {
	switch v {
	case DeliveryDecisionResultCommitted, DeliveryDecisionResultCommitNotConsumed,
		DeliveryDecisionResultAbortedHumanPriority, DeliveryDecisionResultCancelled,
		DeliveryDecisionResultFailed, DeliveryDecisionResultUncertain:
		return true
	default:
		return false
	}
}

// ModelProfileSource names where the active profile observation came from.
type ModelProfileSource string

// ModelProfileSource values.
const (
	ModelProfileSourceGatewayConfig ModelProfileSource = "gateway_config"
	ModelProfileSourceGlimmerCall   ModelProfileSource = "glimmer_call"
	ModelProfileSourceControlState  ModelProfileSource = "control_state"
)

// EnumValues returns the profile sources in table order.
func (ModelProfileSource) EnumValues() []string {
	return []string{string(ModelProfileSourceGatewayConfig), string(ModelProfileSourceGlimmerCall), string(ModelProfileSourceControlState)}
}

// Valid reports whether the value is a known source.
func (v ModelProfileSource) Valid() bool {
	return v == ModelProfileSourceGatewayConfig || v == ModelProfileSourceGlimmerCall || v == ModelProfileSourceControlState
}

// MetadataAvailability says whether a deployment identifier is trustworthy.
type MetadataAvailability string

// MetadataAvailability values.
const (
	MetadataAvailabilityKnown       MetadataAvailability = "known"
	MetadataAvailabilityUnavailable MetadataAvailability = "unavailable"
	MetadataAvailabilityUnknown     MetadataAvailability = "unknown"
)

// EnumValues returns the availability values in table order.
func (MetadataAvailability) EnumValues() []string {
	return []string{
		string(MetadataAvailabilityKnown),
		string(MetadataAvailabilityUnavailable),
		string(MetadataAvailabilityUnknown),
	}
}

// Valid reports whether the value is known.
func (v MetadataAvailability) Valid() bool {
	return v == MetadataAvailabilityKnown || v == MetadataAvailabilityUnavailable ||
		v == MetadataAvailabilityUnknown
}
