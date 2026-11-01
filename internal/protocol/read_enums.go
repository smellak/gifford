package protocol

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
