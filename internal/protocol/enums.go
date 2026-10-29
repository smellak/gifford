package protocol

// This file holds the v1 enums of §4. Every enum is a named string type
// with CamelCase constants, EnumValues in table order and Valid.

// Harness names a supported coding-agent CLI.
type Harness string

// Harness values.
const (
	HarnessClaudeCode  Harness = "claude-code"
	HarnessCodex       Harness = "codex"
	HarnessMuseCode    Harness = "muse-code"
	HarnessOpencode    Harness = "opencode"
	HarnessAntigravity Harness = "antigravity"
	HarnessGeneric     Harness = "generic"
)

// EnumValues returns the harness values in table order.
func (Harness) EnumValues() []string {
	return []string{string(HarnessClaudeCode), string(HarnessCodex), string(HarnessMuseCode), string(HarnessOpencode), string(HarnessAntigravity), string(HarnessGeneric)}
}

// Valid reports whether the value is a known harness.
func (v Harness) Valid() bool {
	switch v {
	case HarnessClaudeCode, HarnessCodex, HarnessMuseCode, HarnessOpencode, HarnessAntigravity, HarnessGeneric:
		return true
	default:
		return false
	}
}

// AdapterType names how Gifford observes an agent.
type AdapterType string

// AdapterType values.
const (
	AdapterTypeNativeMod         AdapterType = "native_mod"
	AdapterTypeNativeModThinPTY  AdapterType = "native_mod+thin_pty"
	AdapterTypeNativeAPI         AdapterType = "native_api"
	AdapterTypeNativeAPIThinPTY  AdapterType = "native_api+thin_pty"
	AdapterTypeNativeHook        AdapterType = "native_hook"
	AdapterTypeNativeHookThinPTY AdapterType = "native_hook+thin_pty"
	AdapterTypeGenericPTY        AdapterType = "generic_pty"
	AdapterTypeNone              AdapterType = noneWord
)

// EnumValues returns the adapter types in table order.
func (AdapterType) EnumValues() []string {
	return []string{string(AdapterTypeNativeMod), string(AdapterTypeNativeModThinPTY), string(AdapterTypeNativeAPI), string(AdapterTypeNativeAPIThinPTY), string(AdapterTypeNativeHook), string(AdapterTypeNativeHookThinPTY), string(AdapterTypeGenericPTY), string(AdapterTypeNone)}
}

// Valid reports whether the value is a known adapter type.
func (v AdapterType) Valid() bool {
	switch v {
	case AdapterTypeNativeMod, AdapterTypeNativeModThinPTY, AdapterTypeNativeAPI, AdapterTypeNativeAPIThinPTY,
		AdapterTypeNativeHook, AdapterTypeNativeHookThinPTY, AdapterTypeGenericPTY, AdapterTypeNone:
		return true
	default:
		return false
	}
}

// InputGuardProvider names who guards the human draft.
type InputGuardProvider string

// InputGuardProvider values.
const (
	InputGuardProviderPTYProxy      InputGuardProvider = "pty_proxy"
	InputGuardProviderNativeAdapter InputGuardProvider = "native_adapter"
	InputGuardProviderNone          InputGuardProvider = noneWord
)

// EnumValues returns the providers in table order.
func (InputGuardProvider) EnumValues() []string {
	return []string{string(InputGuardProviderPTYProxy), string(InputGuardProviderNativeAdapter), string(InputGuardProviderNone)}
}

// Valid reports whether the value is a known provider.
func (v InputGuardProvider) Valid() bool {
	switch v {
	case InputGuardProviderPTYProxy, InputGuardProviderNativeAdapter, InputGuardProviderNone:
		return true
	default:
		return false
	}
}

// CaptureMode names how an agent is captured.
type CaptureMode string

// CaptureMode values.
const (
	CaptureModePTYProxy          CaptureMode = "pty_proxy"
	CaptureModeBootstrapPipePane CaptureMode = "bootstrap_pipe_pane"
)

// EnumValues returns the modes in table order.
func (CaptureMode) EnumValues() []string {
	return []string{string(CaptureModePTYProxy), string(CaptureModeBootstrapPipePane)}
}

// Valid reports whether the value is a known capture mode.
func (v CaptureMode) Valid() bool {
	switch v {
	case CaptureModePTYProxy, CaptureModeBootstrapPipePane:
		return true
	default:
		return false
	}
}

// SourceQuality names where a label or screen came from.
type SourceQuality string

// SourceQuality values.
const (
	SourceQualityNativeStructured  SourceQuality = "native_structured"
	SourceQualityNativeHook        SourceQuality = "native_hook"
	SourceQualityPTYScreen         SourceQuality = "pty_screen"
	SourceQualityBootstrapPipePane SourceQuality = "bootstrap_pipe_pane"
	SourceQualityBootstrapImport   SourceQuality = "bootstrap_import"
	SourceQualityTranscript        SourceQuality = "transcript"
	SourceQualityProcessMetrics    SourceQuality = "process_metrics"
	SourceQualityHumanAnnotation   SourceQuality = "human_annotation"
	SourceQualitySynthetic         SourceQuality = "synthetic"
	SourceQualityInternal          SourceQuality = "internal"
)

// EnumValues returns the qualities in table order.
func (SourceQuality) EnumValues() []string {
	return []string{string(SourceQualityNativeStructured), string(SourceQualityNativeHook), string(SourceQualityPTYScreen), string(SourceQualityBootstrapPipePane), string(SourceQualityBootstrapImport), string(SourceQualityTranscript), string(SourceQualityProcessMetrics), string(SourceQualityHumanAnnotation), string(SourceQualitySynthetic), string(SourceQualityInternal)}
}

// Valid reports whether the value is a known source quality.
func (v SourceQuality) Valid() bool {
	switch v {
	case SourceQualityNativeStructured, SourceQualityNativeHook, SourceQualityPTYScreen,
		SourceQualityBootstrapPipePane, SourceQualityBootstrapImport, SourceQualityTranscript,
		SourceQualityProcessMetrics, SourceQualityHumanAnnotation, SourceQualitySynthetic,
		SourceQualityInternal:
		return true
	default:
		return false
	}
}

// Direction names which way an event flows.
type Direction string

// Direction values.
const (
	DirectionIn       Direction = "in"
	DirectionOut      Direction = "out"
	DirectionInternal Direction = "internal"
)

// EnumValues returns the directions in table order.
func (Direction) EnumValues() []string {
	return []string{string(DirectionIn), string(DirectionOut), string(DirectionInternal)}
}

// Valid reports whether the value is a known direction.
func (v Direction) Valid() bool {
	switch v {
	case DirectionIn, DirectionOut, DirectionInternal:
		return true
	default:
		return false
	}
}

// Stream names the byte stream an event came from.
type Stream string

// Stream values.
const (
	StreamPTY         Stream = "pty"
	StreamStdout      Stream = "stdout"
	StreamStderr      Stream = "stderr"
	StreamNativeEvent Stream = "native_event"
)

// EnumValues returns the streams in table order.
func (Stream) EnumValues() []string {
	return []string{string(StreamPTY), string(StreamStdout), string(StreamStderr), string(StreamNativeEvent)}
}

// Valid reports whether the value is a known stream.
func (v Stream) Valid() bool {
	switch v {
	case StreamPTY, StreamStdout, StreamStderr, StreamNativeEvent:
		return true
	default:
		return false
	}
}

// Origin names what produced an event.
type Origin string

// Origin values.
const (
	OriginHuman        Origin = "human"
	OriginHarness      Origin = "harness"
	OriginOrchestrator Origin = "orchestrator"
	OriginSystem       Origin = "system"
	OriginPeer         Origin = "peer"
)

// EnumValues returns the origins in table order.
func (Origin) EnumValues() []string {
	return []string{string(OriginHuman), string(OriginHarness), string(OriginOrchestrator), string(OriginSystem), string(OriginPeer)}
}

// Valid reports whether the value is a known origin.
func (v Origin) Valid() bool {
	switch v {
	case OriginHuman, OriginHarness, OriginOrchestrator, OriginSystem, OriginPeer:
		return true
	default:
		return false
	}
}

// Sensitivity marks events whose content must never leave the node.
type Sensitivity string

// Sensitivity values.
const (
	SensitivityNormal    Sensitivity = "normal"
	SensitivitySensitive Sensitivity = "sensitive"
)

// EnumValues returns the sensitivities in table order.
func (Sensitivity) EnumValues() []string {
	return []string{string(SensitivityNormal), string(SensitivitySensitive)}
}

// Valid reports whether the value is a known sensitivity.
func (v Sensitivity) Valid() bool {
	switch v {
	case SensitivityNormal, SensitivitySensitive:
		return true
	default:
		return false
	}
}

// SessionState is the lifecycle state of an agent session.
type SessionState string

// SessionState values.
const (
	SessionStateStarting     SessionState = "STARTING"
	SessionStateConnected    SessionState = "CONNECTED"
	SessionStateObserved     SessionState = "OBSERVED"
	SessionStateDetached     SessionState = "DETACHED"
	SessionStateDisconnected SessionState = "DISCONNECTED"
	SessionStateTerminated   SessionState = "TERMINATED"
)

// EnumValues returns the states in table order.
func (SessionState) EnumValues() []string {
	return []string{string(SessionStateStarting), string(SessionStateConnected), string(SessionStateObserved), string(SessionStateDetached), string(SessionStateDisconnected), string(SessionStateTerminated)}
}

// Valid reports whether the value is a known session state.
func (v SessionState) Valid() bool {
	switch v {
	case SessionStateStarting, SessionStateConnected, SessionStateObserved,
		SessionStateDetached, SessionStateDisconnected, SessionStateTerminated:
		return true
	default:
		return false
	}
}

// DraftState is the deterministic human-draft state from the Proxy.
type DraftState string

// DraftState values.
const (
	DraftStateClosed     DraftState = "CLOSED"
	DraftStateOpen       DraftState = "OPEN"
	DraftStatePasting    DraftState = "PASTING"
	DraftStateSubmitting DraftState = "SUBMITTING"
	DraftStateUnknown    DraftState = unknownWord
)

// EnumValues returns the states in table order.
func (DraftState) EnumValues() []string {
	return []string{string(DraftStateClosed), string(DraftStateOpen), string(DraftStatePasting), string(DraftStateSubmitting), string(DraftStateUnknown)}
}

// Valid reports whether the value is a known draft state.
func (v DraftState) Valid() bool {
	switch v {
	case DraftStateClosed, DraftStateOpen, DraftStatePasting, DraftStateSubmitting, DraftStateUnknown:
		return true
	default:
		return false
	}
}

// ActivityState is the UX-level human activity state.
type ActivityState string

// ActivityState values.
const (
	ActivityStateActive  ActivityState = "ACTIVE"
	ActivityStatePaused  ActivityState = pausedWord
	ActivityStateIdle    ActivityState = "IDLE"
	ActivityStateUnknown ActivityState = unknownWord
)

// EnumValues returns the states in table order.
func (ActivityState) EnumValues() []string {
	return []string{string(ActivityStateActive), string(ActivityStatePaused), string(ActivityStateIdle), string(ActivityStateUnknown)}
}

// Valid reports whether the value is a known activity state.
func (v ActivityState) Valid() bool {
	switch v {
	case ActivityStateActive, ActivityStatePaused, ActivityStateIdle, ActivityStateUnknown:
		return true
	default:
		return false
	}
}

// AgentPhase is the analyst label of what an agent is doing.
type AgentPhase string

// AgentPhase values.
const (
	AgentPhaseIdleAtPrompt    AgentPhase = "IDLE_AT_PROMPT"
	AgentPhaseThinking        AgentPhase = "THINKING_STREAMING"
	AgentPhaseRunningTool     AgentPhase = "RUNNING_TOOL"
	AgentPhaseBackgroundWork  AgentPhase = "BACKGROUND_WORK"
	AgentPhaseWaitingApproval AgentPhase = "WAITING_APPROVAL"
	AgentPhaseAskingQuestion  AgentPhase = "ASKING_QUESTION"
	AgentPhaseErrorOrExited   AgentPhase = "ERROR_OR_EXITED"
	AgentPhaseUnknown         AgentPhase = unknownWord
)

// EnumValues returns the phases in table order.
func (AgentPhase) EnumValues() []string {
	return []string{string(AgentPhaseIdleAtPrompt), string(AgentPhaseThinking), string(AgentPhaseRunningTool), string(AgentPhaseBackgroundWork), string(AgentPhaseWaitingApproval), string(AgentPhaseAskingQuestion), string(AgentPhaseErrorOrExited), string(AgentPhaseUnknown)}
}

// Valid reports whether the value is a known agent phase.
func (v AgentPhase) Valid() bool {
	switch v {
	case AgentPhaseIdleAtPrompt, AgentPhaseThinking, AgentPhaseRunningTool, AgentPhaseBackgroundWork,
		AgentPhaseWaitingApproval, AgentPhaseAskingQuestion, AgentPhaseErrorOrExited, AgentPhaseUnknown:
		return true
	default:
		return false
	}
}

// AttentionState names what an agent waits for.
type AttentionState string

// AttentionState values.
const (
	AttentionStateNone                 AttentionState = "NONE"
	AttentionStateWaitingHuman         AttentionState = "WAITING_HUMAN"
	AttentionStatePermissionRequired   AttentionState = "PERMISSION_REQUIRED"
	AttentionStateConfirmationRequired AttentionState = "CONFIRMATION_REQUIRED"
	AttentionStateQuestionPending      AttentionState = "QUESTION_PENDING"
)

// EnumValues returns the states in table order.
func (AttentionState) EnumValues() []string {
	return []string{string(AttentionStateNone), string(AttentionStateWaitingHuman), string(AttentionStatePermissionRequired), string(AttentionStateConfirmationRequired), string(AttentionStateQuestionPending)}
}

// Valid reports whether the value is a known attention state.
func (v AttentionState) Valid() bool {
	switch v {
	case AttentionStateNone, AttentionStateWaitingHuman, AttentionStatePermissionRequired,
		AttentionStateConfirmationRequired, AttentionStateQuestionPending:
		return true
	default:
		return false
	}
}

// HealthState names the health of an agent.
type HealthState string

// HealthState values.
const (
	HealthStateHealthy      HealthState = "HEALTHY"
	HealthStateDegraded     HealthState = "DEGRADED"
	HealthStateStalled      HealthState = "STALLED"
	HealthStateCrashed      HealthState = "CRASHED"
	HealthStateDisconnected HealthState = "DISCONNECTED"
)

// EnumValues returns the states in table order.
func (HealthState) EnumValues() []string {
	return []string{string(HealthStateHealthy), string(HealthStateDegraded), string(HealthStateStalled), string(HealthStateCrashed), string(HealthStateDisconnected)}
}

// Valid reports whether the value is a known health state.
func (v HealthState) Valid() bool {
	switch v {
	case HealthStateHealthy, HealthStateDegraded, HealthStateStalled, HealthStateCrashed, HealthStateDisconnected:
		return true
	default:
		return false
	}
}

// DeliverySafety is the analyst verdict on delivering now.
type DeliverySafety string

// DeliverySafety values.
const (
	DeliverySafetySafe      DeliverySafety = "SAFE"
	DeliverySafetyUnsafe    DeliverySafety = "UNSAFE"
	DeliverySafetyUncertain DeliverySafety = "UNCERTAIN"
)

// EnumValues returns the verdicts in table order.
func (DeliverySafety) EnumValues() []string {
	return []string{string(DeliverySafetySafe), string(DeliverySafetyUnsafe), string(DeliverySafetyUncertain)}
}

// Valid reports whether the value is a known verdict.
func (v DeliverySafety) Valid() bool {
	switch v {
	case DeliverySafetySafe, DeliverySafetyUnsafe, DeliverySafetyUncertain:
		return true
	default:
		return false
	}
}

// Priority orders messages.
type Priority string

// Priority values.
const (
	PriorityLow    Priority = "LOW"
	PriorityNormal Priority = "NORMAL"
	PriorityHigh   Priority = "HIGH"
	PriorityUrgent Priority = "URGENT"
)

// EnumValues returns the priorities in table order.
func (Priority) EnumValues() []string {
	return []string{string(PriorityLow), string(PriorityNormal), string(PriorityHigh), string(PriorityUrgent)}
}

// Valid reports whether the value is a known priority.
func (v Priority) Valid() bool {
	switch v {
	case PriorityLow, PriorityNormal, PriorityHigh, PriorityUrgent:
		return true
	default:
		return false
	}
}

// DeliveryMode selects display-only versus eventual delivery.
type DeliveryMode string

// DeliveryMode values.
const (
	DeliveryModeWhenSafe    DeliveryMode = "WHEN_SAFE"
	DeliveryModeDisplayOnly DeliveryMode = "DISPLAY_ONLY"
)

// EnumValues returns the modes in table order.
func (DeliveryMode) EnumValues() []string {
	return []string{string(DeliveryModeWhenSafe), string(DeliveryModeDisplayOnly)}
}

// Valid reports whether the value is a known delivery mode.
func (v DeliveryMode) Valid() bool {
	switch v {
	case DeliveryModeWhenSafe, DeliveryModeDisplayOnly:
		return true
	default:
		return false
	}
}

// MessageSensitivity classifies message content.
type MessageSensitivity string

// MessageSensitivity values.
const (
	MessageSensitivityPublic       MessageSensitivity = "PUBLIC"
	MessageSensitivityInternal     MessageSensitivity = "INTERNAL"
	MessageSensitivityConfidential MessageSensitivity = "CONFIDENTIAL"
)

// EnumValues returns the classes in table order.
func (MessageSensitivity) EnumValues() []string {
	return []string{string(MessageSensitivityPublic), string(MessageSensitivityInternal), string(MessageSensitivityConfidential)}
}

// Valid reports whether the value is a known class.
func (v MessageSensitivity) Valid() bool {
	switch v {
	case MessageSensitivityPublic, MessageSensitivityInternal, MessageSensitivityConfidential:
		return true
	default:
		return false
	}
}

// Autonomy names the autonomy a loop runs with or requests.
type Autonomy string

// Autonomy values.
const (
	AutonomyAuto Autonomy = "AUTO"
	AutonomyStep Autonomy = "STEP"
)

// EnumValues returns the values in table order.
func (Autonomy) EnumValues() []string { return []string{string(AutonomyAuto), string(AutonomyStep)} }

// Valid reports whether the value is a known autonomy.
func (v Autonomy) Valid() bool {
	switch v {
	case AutonomyAuto, AutonomyStep:
		return true
	default:
		return false
	}
}

// EncryptionProfile names the wire encryption profile.
type EncryptionProfile string

// EncryptionProfile values.
const (
	EncryptionProfileNone         EncryptionProfile = noneWord
	EncryptionProfileGiffordE2EV1 EncryptionProfile = "gifford-e2e-v1"
)

// EnumValues returns the profiles in table order.
func (EncryptionProfile) EnumValues() []string {
	return []string{string(EncryptionProfileNone), string(EncryptionProfileGiffordE2EV1)}
}

// Valid reports whether the value is a known profile.
func (v EncryptionProfile) Valid() bool {
	switch v {
	case EncryptionProfileNone, EncryptionProfileGiffordE2EV1:
		return true
	default:
		return false
	}
}

// ChannelMode names a global or node channel mode.
type ChannelMode string

// ChannelMode values.
const (
	ChannelModeActive   ChannelMode = "ACTIVE"
	ChannelModePaused   ChannelMode = pausedWord
	ChannelModeDraining ChannelMode = "DRAINING"
	ChannelModeOff      ChannelMode = "OFF"
)

// EnumValues returns the modes in table order.
func (ChannelMode) EnumValues() []string {
	return []string{string(ChannelModeActive), string(ChannelModePaused), string(ChannelModeDraining), string(ChannelModeOff)}
}

// Valid reports whether the value is a known channel mode.
func (v ChannelMode) Valid() bool {
	switch v {
	case ChannelModeActive, ChannelModePaused, ChannelModeDraining, ChannelModeOff:
		return true
	default:
		return false
	}
}

// LinkMode names a directional link or thread mode.
type LinkMode string

// LinkMode values.
const (
	LinkModeAllow           LinkMode = "ALLOW"
	LinkModeHold            LinkMode = "HOLD"
	LinkModeRequireApproval LinkMode = "REQUIRE_APPROVAL"
	LinkModeDeny            LinkMode = "DENY"
)

// EnumValues returns the modes in table order.
func (LinkMode) EnumValues() []string {
	return []string{string(LinkModeAllow), string(LinkModeHold), string(LinkModeRequireApproval), string(LinkModeDeny)}
}

// Valid reports whether the value is a known link mode.
func (v LinkMode) Valid() bool {
	switch v {
	case LinkModeAllow, LinkModeHold, LinkModeRequireApproval, LinkModeDeny:
		return true
	default:
		return false
	}
}

// LoopMode names a loop mode.
type LoopMode string

// LoopMode values.
const (
	LoopModeAuto      LoopMode = "AUTO"
	LoopModeStep      LoopMode = "STEP"
	LoopModePaused    LoopMode = pausedWord
	LoopModeDraining  LoopMode = "DRAINING"
	LoopModeStopped   LoopMode = "STOPPED"
	LoopModeCompleted LoopMode = "COMPLETED"
)

// EnumValues returns the modes in table order.
func (LoopMode) EnumValues() []string {
	return []string{string(LoopModeAuto), string(LoopModeStep), string(LoopModePaused), string(LoopModeDraining), string(LoopModeStopped), string(LoopModeCompleted)}
}

// Valid reports whether the value is a known loop mode.
func (v LoopMode) Valid() bool {
	switch v {
	case LoopModeAuto, LoopModeStep, LoopModePaused, LoopModeDraining, LoopModeStopped, LoopModeCompleted:
		return true
	default:
		return false
	}
}

// DecisionOutcome names what a gate decided.
type DecisionOutcome string

// DecisionOutcome values.
const (
	DecisionOutcomeAllow    DecisionOutcome = "ALLOW"
	DecisionOutcomeHold     DecisionOutcome = "HOLD"
	DecisionOutcomeApproval DecisionOutcome = "APPROVAL"
	DecisionOutcomeDeny     DecisionOutcome = "DENY"
)

// EnumValues returns the outcomes in table order.
func (DecisionOutcome) EnumValues() []string {
	return []string{string(DecisionOutcomeAllow), string(DecisionOutcomeHold), string(DecisionOutcomeApproval), string(DecisionOutcomeDeny)}
}

// Valid reports whether the value is a known outcome.
func (v DecisionOutcome) Valid() bool {
	switch v {
	case DecisionOutcomeAllow, DecisionOutcomeHold, DecisionOutcomeApproval, DecisionOutcomeDeny:
		return true
	default:
		return false
	}
}

// PatternSignal names a loop pattern signal.
type PatternSignal string

// PatternSignal values.
const (
	PatternSignalRepeatedDigest   PatternSignal = "repeated_digest"
	PatternSignalAlternation      PatternSignal = "alternation"
	PatternSignalRate             PatternSignal = "rate"
	PatternSignalUnchangedSubject PatternSignal = "unchanged_subject"
	PatternSignalRouteCycle       PatternSignal = "route_cycle"
	PatternSignalNoProgress       PatternSignal = "no_progress"
)

// EnumValues returns the signals in table order.
func (PatternSignal) EnumValues() []string {
	return []string{string(PatternSignalRepeatedDigest), string(PatternSignalAlternation), string(PatternSignalRate), string(PatternSignalUnchangedSubject), string(PatternSignalRouteCycle), string(PatternSignalNoProgress)}
}

// Valid reports whether the value is a known signal.
func (v PatternSignal) Valid() bool {
	switch v {
	case PatternSignalRepeatedDigest, PatternSignalAlternation, PatternSignalRate,
		PatternSignalUnchangedSubject, PatternSignalRouteCycle, PatternSignalNoProgress:
		return true
	default:
		return false
	}
}

// TriageLabel names a message triage verdict.
type TriageLabel string

// TriageLabel values.
const (
	TriageLabelNormal           TriageLabel = "NORMAL"
	TriageLabelConflictOrUrgent TriageLabel = "CONFLICT_OR_URGENT"
	TriageLabelLowPriority      TriageLabel = "LOW_PRIORITY"
	TriageLabelRequiresHuman    TriageLabel = "REQUIRES_HUMAN"
)

// EnumValues returns the labels in table order.
func (TriageLabel) EnumValues() []string {
	return []string{string(TriageLabelNormal), string(TriageLabelConflictOrUrgent), string(TriageLabelLowPriority), string(TriageLabelRequiresHuman)}
}

// Valid reports whether the value is a known label.
func (v TriageLabel) Valid() bool {
	switch v {
	case TriageLabelNormal, TriageLabelConflictOrUrgent, TriageLabelLowPriority, TriageLabelRequiresHuman:
		return true
	default:
		return false
	}
}

// OrchestratorAction names an orchestration action. The DELIVER_NATIVE_EVENT
// constant keeps its longer name so the delivery-call guard never matches it.
type OrchestratorAction string

// OrchestratorAction values.
const (
	OrchestratorActionShowHumanNotification OrchestratorAction = "SHOW_HUMAN_NOTIFICATION"
	OrchestratorActionDeliverNative         OrchestratorAction = "DELIVER_NATIVE_EVENT"
	OrchestratorActionInjectPTYPrompt       OrchestratorAction = "INJECT_PTY_PROMPT"
	OrchestratorActionQueue                 OrchestratorAction = "QUEUE"
	OrchestratorActionPrioritize            OrchestratorAction = "PRIORITIZE"
	OrchestratorActionBatch                 OrchestratorAction = "BATCH"
	OrchestratorActionDisplayOnly           OrchestratorAction = "DISPLAY_ONLY"
	OrchestratorActionRequestHumanDecision  OrchestratorAction = "REQUEST_HUMAN_DECISION"
	OrchestratorActionRetry                 OrchestratorAction = "RETRY"
	OrchestratorActionDefer                 OrchestratorAction = "DEFER"
	OrchestratorActionExpire                OrchestratorAction = "EXPIRE"
	OrchestratorActionDropDuplicate         OrchestratorAction = "DROP_DUPLICATE"
)

// EnumValues returns the actions in table order.
func (OrchestratorAction) EnumValues() []string {
	return []string{string(OrchestratorActionShowHumanNotification), string(OrchestratorActionDeliverNative), string(OrchestratorActionInjectPTYPrompt), string(OrchestratorActionQueue), string(OrchestratorActionPrioritize), string(OrchestratorActionBatch), string(OrchestratorActionDisplayOnly), string(OrchestratorActionRequestHumanDecision), string(OrchestratorActionRetry), string(OrchestratorActionDefer), string(OrchestratorActionExpire), string(OrchestratorActionDropDuplicate)}
}

// Valid reports whether the value is a known action.
func (v OrchestratorAction) Valid() bool {
	switch v {
	case OrchestratorActionShowHumanNotification, OrchestratorActionDeliverNative, OrchestratorActionInjectPTYPrompt,
		OrchestratorActionQueue, OrchestratorActionPrioritize, OrchestratorActionBatch,
		OrchestratorActionDisplayOnly, OrchestratorActionRequestHumanDecision, OrchestratorActionRetry,
		OrchestratorActionDefer, OrchestratorActionExpire, OrchestratorActionDropDuplicate:
		return true
	default:
		return false
	}
}

// GuardName names a deterministic guard in evaluation order.
type GuardName string

// GuardName values.
const (
	GuardNameSessionConnected             GuardName = "session_connected"
	GuardNameDeliverable                  GuardName = "deliverable"
	GuardNameHumanDraftClosed             GuardName = "human_draft_closed"
	GuardNameHumanActivityNotActive       GuardName = "human_activity_not_active"
	GuardNameInputGuardVersionUnchanged   GuardName = "input_guard_version_unchanged"
	GuardNameNoHumanKeyAfterPrepare       GuardName = "no_human_key_after_prepare"
	GuardNamePTYOutputQuiet               GuardName = "pty_output_quiet"
	GuardNameNoNewChildOrTool             GuardName = "no_new_child_or_tool"
	GuardNameNotWaitingApprovalOrQuestion GuardName = "not_waiting_approval_or_question"
	GuardNameAdapterConnected             GuardName = "adapter_connected"
	GuardNameWriterLeaseFree              GuardName = "writer_lease_free"
	GuardNameNotExpired                   GuardName = "not_expired"
	GuardNameNotDuplicate                 GuardName = "not_duplicate"
	GuardNamePeerKeyPolicyValid           GuardName = "peer_key_policy_valid"
	GuardNamePIdleThreshold               GuardName = "p_idle_threshold"
)

// EnumValues returns the guards in evaluation order.
func (GuardName) EnumValues() []string {
	return []string{string(GuardNameSessionConnected), string(GuardNameDeliverable), string(GuardNameHumanDraftClosed), string(GuardNameHumanActivityNotActive), string(GuardNameInputGuardVersionUnchanged), string(GuardNameNoHumanKeyAfterPrepare), string(GuardNamePTYOutputQuiet), string(GuardNameNoNewChildOrTool), string(GuardNameNotWaitingApprovalOrQuestion), string(GuardNameAdapterConnected), string(GuardNameWriterLeaseFree), string(GuardNameNotExpired), string(GuardNameNotDuplicate), string(GuardNamePeerKeyPolicyValid), string(GuardNamePIdleThreshold)}
}

// Valid reports whether the value is a known guard.
func (v GuardName) Valid() bool {
	switch v {
	case GuardNameSessionConnected, GuardNameDeliverable, GuardNameHumanDraftClosed,
		GuardNameHumanActivityNotActive, GuardNameInputGuardVersionUnchanged,
		GuardNameNoHumanKeyAfterPrepare, GuardNamePTYOutputQuiet, GuardNameNoNewChildOrTool,
		GuardNameNotWaitingApprovalOrQuestion, GuardNameAdapterConnected, GuardNameWriterLeaseFree,
		GuardNameNotExpired, GuardNameNotDuplicate, GuardNamePeerKeyPolicyValid, GuardNamePIdleThreshold:
		return true
	default:
		return false
	}
}

// GuardOutcome names a guard evaluation result.
type GuardOutcome string

// GuardOutcome values.
const (
	GuardOutcomePass       GuardOutcome = "pass"
	GuardOutcomeFail       GuardOutcome = "fail"
	GuardOutcomeOverridden GuardOutcome = "overridden"
)

// EnumValues returns the outcomes in table order.
func (GuardOutcome) EnumValues() []string {
	return []string{string(GuardOutcomePass), string(GuardOutcomeFail), string(GuardOutcomeOverridden)}
}

// Valid reports whether the value is a known outcome.
func (v GuardOutcome) Valid() bool {
	switch v {
	case GuardOutcomePass, GuardOutcomeFail, GuardOutcomeOverridden:
		return true
	default:
		return false
	}
}

// ExecutionStatus names the execution state of an orchestrator decision.
type ExecutionStatus string

// ExecutionStatus values.
const (
	ExecutionStatusPending  ExecutionStatus = "PENDING"
	ExecutionStatusExecuted ExecutionStatus = "EXECUTED"
	ExecutionStatusAborted  ExecutionStatus = "ABORTED"
	ExecutionStatusFailed   ExecutionStatus = "FAILED"
	ExecutionStatusStale    ExecutionStatus = "STALE"
)

// EnumValues returns the states in table order.
func (ExecutionStatus) EnumValues() []string {
	return []string{string(ExecutionStatusPending), string(ExecutionStatusExecuted), string(ExecutionStatusAborted), string(ExecutionStatusFailed), string(ExecutionStatusStale)}
}

// Valid reports whether the value is a known state.
func (v ExecutionStatus) Valid() bool {
	switch v {
	case ExecutionStatusPending, ExecutionStatusExecuted, ExecutionStatusAborted, ExecutionStatusFailed, ExecutionStatusStale:
		return true
	default:
		return false
	}
}

// FrameErrorCode names an agent-socket frame error.
type FrameErrorCode string

// FrameErrorCode values.
const (
	FrameErrorCodeInvalidFrame    FrameErrorCode = "INVALID_FRAME"
	FrameErrorCodeUnsupportedOp   FrameErrorCode = "UNSUPPORTED_OP"
	FrameErrorCodeNotRegistered   FrameErrorCode = "NOT_REGISTERED"
	FrameErrorCodeUnknownDelivery FrameErrorCode = "UNKNOWN_DELIVERY"
	FrameErrorCodeInternal        FrameErrorCode = "INTERNAL"
)

// EnumValues returns the codes in table order.
func (FrameErrorCode) EnumValues() []string {
	return []string{string(FrameErrorCodeInvalidFrame), string(FrameErrorCodeUnsupportedOp), string(FrameErrorCodeNotRegistered), string(FrameErrorCodeUnknownDelivery), string(FrameErrorCodeInternal)}
}

// Valid reports whether the value is a known code.
func (v FrameErrorCode) Valid() bool {
	switch v {
	case FrameErrorCodeInvalidFrame, FrameErrorCodeUnsupportedOp, FrameErrorCodeNotRegistered,
		FrameErrorCodeUnknownDelivery, FrameErrorCodeInternal:
		return true
	default:
		return false
	}
}

// DeliveryResultKind names a proxy delivery result.
type DeliveryResultKind string

// DeliveryResultKind values.
const (
	DeliveryResultKindPrepared             DeliveryResultKind = "PREPARED"
	DeliveryResultKindCommitted            DeliveryResultKind = "COMMITTED"
	DeliveryResultKindAbortedHumanPriority DeliveryResultKind = "ABORTED_HUMAN_PRIORITY"
	DeliveryResultKindCancelled            DeliveryResultKind = "CANCELLED"
	DeliveryResultKindCommitNotConsumed    DeliveryResultKind = "COMMIT_NOT_CONSUMED"
	DeliveryResultKindFailed               DeliveryResultKind = "FAILED"
)

// EnumValues returns the kinds in table order.
func (DeliveryResultKind) EnumValues() []string {
	return []string{string(DeliveryResultKindPrepared), string(DeliveryResultKindCommitted), string(DeliveryResultKindAbortedHumanPriority), string(DeliveryResultKindCancelled), string(DeliveryResultKindCommitNotConsumed), string(DeliveryResultKindFailed)}
}

// Valid reports whether the value is a known kind.
func (v DeliveryResultKind) Valid() bool {
	switch v {
	case DeliveryResultKindPrepared, DeliveryResultKindCommitted, DeliveryResultKindAbortedHumanPriority,
		DeliveryResultKindCancelled, DeliveryResultKindCommitNotConsumed, DeliveryResultKindFailed:
		return true
	default:
		return false
	}
}

// CancelReason names why a delivery was cancelled.
type CancelReason string

// CancelReason values.
const (
	CancelReasonHumanPriority  CancelReason = "HUMAN_PRIORITY"
	CancelReasonControlChanged CancelReason = "CONTROL_CHANGED"
	CancelReasonStateChanged   CancelReason = "STATE_CHANGED"
	CancelReasonManual         CancelReason = "MANUAL"
)

// EnumValues returns the reasons in table order.
func (CancelReason) EnumValues() []string {
	return []string{string(CancelReasonHumanPriority), string(CancelReasonControlChanged), string(CancelReasonStateChanged), string(CancelReasonManual)}
}

// Valid reports whether the value is a known reason.
func (v CancelReason) Valid() bool {
	switch v {
	case CancelReasonHumanPriority, CancelReasonControlChanged, CancelReasonStateChanged, CancelReasonManual:
		return true
	default:
		return false
	}
}

// InjectMode names how bytes are injected into a harness.
type InjectMode string

// InjectMode values.
const (
	InjectModeTyped          InjectMode = "typed"
	InjectModeBracketedPaste InjectMode = "bracketed_paste"
)

// EnumValues returns the modes in table order.
func (InjectMode) EnumValues() []string {
	return []string{string(InjectModeTyped), string(InjectModeBracketedPaste)}
}

// Valid reports whether the value is a known mode.
func (v InjectMode) Valid() bool {
	switch v {
	case InjectModeTyped, InjectModeBracketedPaste:
		return true
	default:
		return false
	}
}

// AckStatus names the three message states an ack can report: the two
// lifecycle states, plus the metadata-mismatch state the Hub records.
type AckStatus string

// AckStatus values.
const (
	AckStatusProcessing   AckStatus = "PROCESSING"
	AckStatusAcknowledged AckStatus = "ACKNOWLEDGED"
	AckStatusMismatch     AckStatus = "MISMATCH"
)

// EnumValues returns the states in table order.
func (AckStatus) EnumValues() []string {
	return []string{string(AckStatusProcessing), string(AckStatusAcknowledged), string(AckStatusMismatch)}
}

// Valid reports whether the value is a known state.
func (v AckStatus) Valid() bool {
	switch v {
	case AckStatusProcessing, AckStatusAcknowledged, AckStatusMismatch:
		return true
	default:
		return false
	}
}

// ArtifactState names an artifact lifecycle state.
type ArtifactState string

// ArtifactState values.
const (
	ArtifactStateReserved        ArtifactState = "RESERVED"
	ArtifactStateUploading       ArtifactState = "UPLOADING"
	ArtifactStateStoredOrigin    ArtifactState = "STORED_ORIGIN"
	ArtifactStateRouting         ArtifactState = "ROUTING"
	ArtifactStateLocalAvailable  ArtifactState = "LOCAL_AVAILABLE"
	ArtifactStateQueuedRemote    ArtifactState = "QUEUED_REMOTE"
	ArtifactStateReplicating     ArtifactState = "REPLICATING"
	ArtifactStateStoredRemote    ArtifactState = "STORED_REMOTE"
	ArtifactStateAvailable       ArtifactState = "AVAILABLE"
	ArtifactStateDownloading     ArtifactState = "DOWNLOADING"
	ArtifactStateVerifying       ArtifactState = "VERIFYING"
	ArtifactStateDecrypted       ArtifactState = "DECRYPTED"
	ArtifactStateDelivered       ArtifactState = "DELIVERED"
	ArtifactStateAcknowledged    ArtifactState = "ACKNOWLEDGED"
	ArtifactStateFailedRetryable ArtifactState = "FAILED_RETRYABLE"
	ArtifactStateFailedPermanent ArtifactState = "FAILED_PERMANENT"
	ArtifactStateExpired         ArtifactState = "EXPIRED"
	ArtifactStateCancelled       ArtifactState = "CANCELLED"
	ArtifactStateRevoked         ArtifactState = "REVOKED"
)

// EnumValues returns the states in table order.
func (ArtifactState) EnumValues() []string {
	return []string{string(ArtifactStateReserved), string(ArtifactStateUploading), string(ArtifactStateStoredOrigin), string(ArtifactStateRouting), string(ArtifactStateLocalAvailable), string(ArtifactStateQueuedRemote), string(ArtifactStateReplicating), string(ArtifactStateStoredRemote), string(ArtifactStateAvailable), string(ArtifactStateDownloading), string(ArtifactStateVerifying), string(ArtifactStateDecrypted), string(ArtifactStateDelivered), string(ArtifactStateAcknowledged), string(ArtifactStateFailedRetryable), string(ArtifactStateFailedPermanent), string(ArtifactStateExpired), string(ArtifactStateCancelled), string(ArtifactStateRevoked)}
}

// Valid reports whether the value is a known state.
func (v ArtifactState) Valid() bool {
	switch v {
	case ArtifactStateReserved, ArtifactStateUploading, ArtifactStateStoredOrigin, ArtifactStateRouting,
		ArtifactStateLocalAvailable, ArtifactStateQueuedRemote, ArtifactStateReplicating,
		ArtifactStateStoredRemote, ArtifactStateAvailable, ArtifactStateDownloading, ArtifactStateVerifying,
		ArtifactStateDecrypted, ArtifactStateDelivered, ArtifactStateAcknowledged,
		ArtifactStateFailedRetryable, ArtifactStateFailedPermanent, ArtifactStateExpired,
		ArtifactStateCancelled, ArtifactStateRevoked:
		return true
	default:
		return false
	}
}

// ModelContract names a Glimmer contract.
type ModelContract string

// ModelContract values.
const (
	ModelContractAnalyzeAgentState         ModelContract = "ANALYZE_AGENT_STATE"
	ModelContractTriageMessage             ModelContract = "TRIAGE_MESSAGE"
	ModelContractChooseOrchestrationAction ModelContract = "CHOOSE_ORCHESTRATION_ACTION"
)

// EnumValues returns the contracts in table order.
func (ModelContract) EnumValues() []string {
	return []string{string(ModelContractAnalyzeAgentState), string(ModelContractTriageMessage), string(ModelContractChooseOrchestrationAction)}
}

// Valid reports whether the value is a known contract.
func (v ModelContract) Valid() bool {
	switch v {
	case ModelContractAnalyzeAgentState, ModelContractTriageMessage, ModelContractChooseOrchestrationAction:
		return true
	default:
		return false
	}
}

// InferenceProfile names a model inference profile.
type InferenceProfile string

// InferenceProfile values.
const (
	InferenceProfileBase        InferenceProfile = "base"
	InferenceProfileAnalystLoRA InferenceProfile = "analyst-lora"
)

// EnumValues returns the profiles in table order.
func (InferenceProfile) EnumValues() []string {
	return []string{string(InferenceProfileBase), string(InferenceProfileAnalystLoRA)}
}

// Valid reports whether the value is a known profile.
func (v InferenceProfile) Valid() bool {
	switch v {
	case InferenceProfileBase, InferenceProfileAnalystLoRA:
		return true
	default:
		return false
	}
}

// GlimmerCallStatus names a Gateway call outcome.
type GlimmerCallStatus string

// GlimmerCallStatus values.
const (
	GlimmerCallStatusOK              GlimmerCallStatus = "ok"
	GlimmerCallStatusTimeout         GlimmerCallStatus = "timeout"
	GlimmerCallStatusInvalid         GlimmerCallStatus = "invalid"
	GlimmerCallStatusError           GlimmerCallStatus = "error"
	GlimmerCallStatusRedactionFailed GlimmerCallStatus = "redaction_failed"
)

// EnumValues returns the outcomes in table order.
func (GlimmerCallStatus) EnumValues() []string {
	return []string{string(GlimmerCallStatusOK), string(GlimmerCallStatusTimeout), string(GlimmerCallStatusInvalid), string(GlimmerCallStatusError), string(GlimmerCallStatusRedactionFailed)}
}

// Valid reports whether the value is a known outcome.
func (v GlimmerCallStatus) Valid() bool {
	switch v {
	case GlimmerCallStatusOK, GlimmerCallStatusTimeout, GlimmerCallStatusInvalid,
		GlimmerCallStatusError, GlimmerCallStatusRedactionFailed:
		return true
	default:
		return false
	}
}

// CaptureGapCause names why a capture has a gap.
type CaptureGapCause string

// CaptureGapCause values.
const (
	CaptureGapCausePipeClosed CaptureGapCause = "pipe_closed"
	CaptureGapCauseIngestDied CaptureGapCause = "ingest_died"
	CaptureGapCauseQuota      CaptureGapCause = "quota"
)

// EnumValues returns the causes in table order.
func (CaptureGapCause) EnumValues() []string {
	return []string{string(CaptureGapCausePipeClosed), string(CaptureGapCauseIngestDied), string(CaptureGapCauseQuota)}
}

// Valid reports whether the value is a known cause.
func (v CaptureGapCause) Valid() bool {
	switch v {
	case CaptureGapCausePipeClosed, CaptureGapCauseIngestDied, CaptureGapCauseQuota:
		return true
	default:
		return false
	}
}

// CaptureStopReason names why a capture stopped.
type CaptureStopReason string

// CaptureStopReason values.
const (
	CaptureStopReasonManual            CaptureStopReason = "manual"
	CaptureStopReasonRelaunchedWithRun CaptureStopReason = "relaunched_with_run"
	CaptureStopReasonQuota             CaptureStopReason = "quota"
	CaptureStopReasonError             CaptureStopReason = "error"
)

// EnumValues returns the reasons in table order.
func (CaptureStopReason) EnumValues() []string {
	return []string{string(CaptureStopReasonManual), string(CaptureStopReasonRelaunchedWithRun), string(CaptureStopReasonQuota), string(CaptureStopReasonError)}
}

// Valid reports whether the value is a known reason.
func (v CaptureStopReason) Valid() bool {
	switch v {
	case CaptureStopReasonManual, CaptureStopReasonRelaunchedWithRun, CaptureStopReasonQuota, CaptureStopReasonError:
		return true
	default:
		return false
	}
}

// QuotaScope names what a capture quota applies to.
type QuotaScope string

// QuotaScope values.
const (
	QuotaScopeNode  QuotaScope = "node"
	QuotaScopeAgent QuotaScope = "agent"
)

// EnumValues returns the scopes in table order.
func (QuotaScope) EnumValues() []string {
	return []string{string(QuotaScopeNode), string(QuotaScopeAgent)}
}

// Valid reports whether the value is a known scope.
func (v QuotaScope) Valid() bool {
	switch v {
	case QuotaScopeNode, QuotaScopeAgent:
		return true
	default:
		return false
	}
}
