package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// enumCase binds one enum's zero value to its table values.
type enumCase struct {
	zero   interface{ EnumValues() []string }
	check  func(string) bool
	values []string
}

func v1Enums() []enumCase {
	return []enumCase{
		{zero: Harness(""), check: func(s string) bool { return Harness(s).Valid() }, values: []string{string(HarnessClaudeCode), string(HarnessCodex), string(HarnessMuseCode), string(HarnessOpencode), string(HarnessAntigravity), string(HarnessGeneric)}},
		{zero: AdapterType(""), check: func(s string) bool { return AdapterType(s).Valid() }, values: []string{string(AdapterTypeNativeMod), string(AdapterTypeNativeModThinPTY), string(AdapterTypeNativeAPI), string(AdapterTypeNativeAPIThinPTY), string(AdapterTypeNativeHook), string(AdapterTypeNativeHookThinPTY), string(AdapterTypeGenericPTY), string(AdapterTypeNone)}},
		{zero: InputGuardProvider(""), check: func(s string) bool { return InputGuardProvider(s).Valid() }, values: []string{string(InputGuardProviderPTYProxy), string(InputGuardProviderNativeAdapter), string(InputGuardProviderNone)}},
		{zero: CaptureMode(""), check: func(s string) bool { return CaptureMode(s).Valid() }, values: []string{string(CaptureModePTYProxy), string(CaptureModeBootstrapPipePane)}},
		{zero: SourceQuality(""), check: func(s string) bool { return SourceQuality(s).Valid() }, values: []string{string(SourceQualityNativeStructured), string(SourceQualityNativeHook), string(SourceQualityPTYScreen), string(SourceQualityBootstrapPipePane), string(SourceQualityBootstrapImport), string(SourceQualityTranscript), string(SourceQualityProcessMetrics), string(SourceQualityHumanAnnotation), string(SourceQualitySynthetic), string(SourceQualityInternal)}},
		{zero: Direction(""), check: func(s string) bool { return Direction(s).Valid() }, values: []string{string(DirectionIn), string(DirectionOut), string(DirectionInternal)}},
		{zero: Stream(""), check: func(s string) bool { return Stream(s).Valid() }, values: []string{string(StreamPTY), string(StreamStdout), string(StreamStderr), string(StreamNativeEvent)}},
		{zero: Origin(""), check: func(s string) bool { return Origin(s).Valid() }, values: []string{string(OriginHuman), string(OriginHarness), string(OriginOrchestrator), string(OriginSystem), string(OriginPeer)}},
		{zero: Sensitivity(""), check: func(s string) bool { return Sensitivity(s).Valid() }, values: []string{string(SensitivityNormal), string(SensitivitySensitive)}},
		{zero: SessionState(""), check: func(s string) bool { return SessionState(s).Valid() }, values: []string{string(SessionStateStarting), string(SessionStateConnected), string(SessionStateObserved), string(SessionStateDetached), string(SessionStateDisconnected), string(SessionStateTerminated)}},
		{zero: DraftState(""), check: func(s string) bool { return DraftState(s).Valid() }, values: []string{string(DraftStateClosed), string(DraftStateOpen), string(DraftStatePasting), string(DraftStateSubmitting), string(DraftStateUnknown)}},
		{zero: ActivityState(""), check: func(s string) bool { return ActivityState(s).Valid() }, values: []string{string(ActivityStateActive), string(ActivityStatePaused), string(ActivityStateIdle), string(ActivityStateUnknown)}},
		{zero: AgentPhase(""), check: func(s string) bool { return AgentPhase(s).Valid() }, values: []string{string(AgentPhaseIdleAtPrompt), string(AgentPhaseThinking), string(AgentPhaseRunningTool), string(AgentPhaseBackgroundWork), string(AgentPhaseWaitingApproval), string(AgentPhaseAskingQuestion), string(AgentPhaseErrorOrExited), string(AgentPhaseUnknown)}},
		{zero: AttentionState(""), check: func(s string) bool { return AttentionState(s).Valid() }, values: []string{string(AttentionStateNone), string(AttentionStateWaitingHuman), string(AttentionStatePermissionRequired), string(AttentionStateConfirmationRequired), string(AttentionStateQuestionPending)}},
		{zero: HealthState(""), check: func(s string) bool { return HealthState(s).Valid() }, values: []string{string(HealthStateHealthy), string(HealthStateDegraded), string(HealthStateStalled), string(HealthStateCrashed), string(HealthStateDisconnected)}},
		{zero: DeliverySafety(""), check: func(s string) bool { return DeliverySafety(s).Valid() }, values: []string{string(DeliverySafetySafe), string(DeliverySafetyUnsafe), string(DeliverySafetyUncertain)}},
		{zero: Priority(""), check: func(s string) bool { return Priority(s).Valid() }, values: []string{string(PriorityLow), string(PriorityNormal), string(PriorityHigh), string(PriorityUrgent)}},
		{zero: DeliveryMode(""), check: func(s string) bool { return DeliveryMode(s).Valid() }, values: []string{string(DeliveryModeWhenSafe), string(DeliveryModeDisplayOnly)}},
		{zero: MessageSensitivity(""), check: func(s string) bool { return MessageSensitivity(s).Valid() }, values: []string{string(MessageSensitivityPublic), string(MessageSensitivityInternal), string(MessageSensitivityConfidential)}},
		{zero: Autonomy(""), check: func(s string) bool { return Autonomy(s).Valid() }, values: []string{string(AutonomyAuto), string(AutonomyStep)}},
		{zero: EncryptionProfile(""), check: func(s string) bool { return EncryptionProfile(s).Valid() }, values: []string{string(EncryptionProfileNone), string(EncryptionProfileGiffordE2EV1)}},
		{zero: ChannelMode(""), check: func(s string) bool { return ChannelMode(s).Valid() }, values: []string{string(ChannelModeActive), string(ChannelModePaused), string(ChannelModeDraining), string(ChannelModeOff)}},
		{zero: LinkMode(""), check: func(s string) bool { return LinkMode(s).Valid() }, values: []string{string(LinkModeAllow), string(LinkModeHold), string(LinkModeRequireApproval), string(LinkModeDeny)}},
		{zero: LoopMode(""), check: func(s string) bool { return LoopMode(s).Valid() }, values: []string{string(LoopModeAuto), string(LoopModeStep), string(LoopModePaused), string(LoopModeDraining), string(LoopModeStopped), string(LoopModeCompleted)}},
		{zero: DecisionOutcome(""), check: func(s string) bool { return DecisionOutcome(s).Valid() }, values: []string{string(DecisionOutcomeAllow), string(DecisionOutcomeHold), string(DecisionOutcomeApproval), string(DecisionOutcomeDeny)}},
		{zero: PatternSignal(""), check: func(s string) bool { return PatternSignal(s).Valid() }, values: []string{string(PatternSignalRepeatedDigest), string(PatternSignalAlternation), string(PatternSignalRate), string(PatternSignalUnchangedSubject), string(PatternSignalRouteCycle), string(PatternSignalNoProgress)}},
		{zero: TriageLabel(""), check: func(s string) bool { return TriageLabel(s).Valid() }, values: []string{string(TriageLabelNormal), string(TriageLabelConflictOrUrgent), string(TriageLabelLowPriority), string(TriageLabelRequiresHuman)}},
		{zero: OrchestratorAction(""), check: func(s string) bool { return OrchestratorAction(s).Valid() }, values: []string{string(OrchestratorActionShowHumanNotification), string(OrchestratorActionDeliverNative), string(OrchestratorActionInjectPTYPrompt), string(OrchestratorActionQueue), string(OrchestratorActionPrioritize), string(OrchestratorActionBatch), string(OrchestratorActionDisplayOnly), string(OrchestratorActionRequestHumanDecision), string(OrchestratorActionRetry), string(OrchestratorActionDefer), string(OrchestratorActionExpire), string(OrchestratorActionDropDuplicate)}},
		{zero: GuardName(""), check: func(s string) bool { return GuardName(s).Valid() }, values: []string{string(GuardNameSessionConnected), string(GuardNameDeliverable), string(GuardNameHumanDraftClosed), string(GuardNameHumanActivityNotActive), string(GuardNameInputGuardVersionUnchanged), string(GuardNameNoHumanKeyAfterPrepare), string(GuardNamePTYOutputQuiet), string(GuardNameNoNewChildOrTool), string(GuardNameNotWaitingApprovalOrQuestion), string(GuardNameAdapterConnected), string(GuardNameWriterLeaseFree), string(GuardNameNotExpired), string(GuardNameNotDuplicate), string(GuardNamePeerKeyPolicyValid), string(GuardNamePIdleThreshold)}},
		{zero: GuardOutcome(""), check: func(s string) bool { return GuardOutcome(s).Valid() }, values: []string{string(GuardOutcomePass), string(GuardOutcomeFail), string(GuardOutcomeOverridden)}},
		{zero: ExecutionStatus(""), check: func(s string) bool { return ExecutionStatus(s).Valid() }, values: []string{string(ExecutionStatusPending), string(ExecutionStatusExecuted), string(ExecutionStatusAborted), string(ExecutionStatusFailed), string(ExecutionStatusStale)}},
		{zero: FrameErrorCode(""), check: func(s string) bool { return FrameErrorCode(s).Valid() }, values: []string{string(FrameErrorCodeInvalidFrame), string(FrameErrorCodeUnsupportedOp), string(FrameErrorCodeNotRegistered), string(FrameErrorCodeUnknownDelivery), string(FrameErrorCodeInternal)}},
		{zero: DeliveryResultKind(""), check: func(s string) bool { return DeliveryResultKind(s).Valid() }, values: []string{string(DeliveryResultKindPrepared), string(DeliveryResultKindCommitted), string(DeliveryResultKindAbortedHumanPriority), string(DeliveryResultKindCancelled), string(DeliveryResultKindCommitNotConsumed), string(DeliveryResultKindFailed)}},
		{zero: CancelReason(""), check: func(s string) bool { return CancelReason(s).Valid() }, values: []string{string(CancelReasonHumanPriority), string(CancelReasonControlChanged), string(CancelReasonStateChanged), string(CancelReasonManual)}},
		{zero: InjectMode(""), check: func(s string) bool { return InjectMode(s).Valid() }, values: []string{string(InjectModeTyped), string(InjectModeBracketedPaste)}},
		{zero: AckStatus(""), check: func(s string) bool { return AckStatus(s).Valid() }, values: []string{string(AckStatusProcessing), string(AckStatusAcknowledged)}},
		{zero: ArtifactState(""), check: func(s string) bool { return ArtifactState(s).Valid() }, values: []string{string(ArtifactStateReserved), string(ArtifactStateUploading), string(ArtifactStateStoredOrigin), string(ArtifactStateRouting), string(ArtifactStateLocalAvailable), string(ArtifactStateQueuedRemote), string(ArtifactStateReplicating), string(ArtifactStateStoredRemote), string(ArtifactStateAvailable), string(ArtifactStateDownloading), string(ArtifactStateVerifying), string(ArtifactStateDecrypted), string(ArtifactStateDelivered), string(ArtifactStateAcknowledged), string(ArtifactStateFailedRetryable), string(ArtifactStateFailedPermanent), string(ArtifactStateExpired), string(ArtifactStateCancelled), string(ArtifactStateRevoked)}},
		{zero: ModelContract(""), check: func(s string) bool { return ModelContract(s).Valid() }, values: []string{string(ModelContractAnalyzeAgentState), string(ModelContractTriageMessage), string(ModelContractChooseOrchestrationAction)}},
		{zero: InferenceProfile(""), check: func(s string) bool { return InferenceProfile(s).Valid() }, values: []string{string(InferenceProfileBase), string(InferenceProfileAnalystLoRA)}},
		{zero: GlimmerCallStatus(""), check: func(s string) bool { return GlimmerCallStatus(s).Valid() }, values: []string{string(GlimmerCallStatusOK), string(GlimmerCallStatusTimeout), string(GlimmerCallStatusInvalid), string(GlimmerCallStatusError), string(GlimmerCallStatusRedactionFailed)}},
		{zero: CaptureGapCause(""), check: func(s string) bool { return CaptureGapCause(s).Valid() }, values: []string{string(CaptureGapCausePipeClosed), string(CaptureGapCauseIngestDied), string(CaptureGapCauseQuota)}},
		{zero: CaptureStopReason(""), check: func(s string) bool { return CaptureStopReason(s).Valid() }, values: []string{string(CaptureStopReasonManual), string(CaptureStopReasonRelaunchedWithRun), string(CaptureStopReasonQuota), string(CaptureStopReasonError)}},
		{zero: QuotaScope(""), check: func(s string) bool { return QuotaScope(s).Valid() }, values: []string{string(QuotaScopeNode), string(QuotaScopeAgent)}},
	}
}

// TestEnums checks every v1 enum: values unique, Valid accepts each and
// rejects the empty string and a case variant.
func TestEnums(t *testing.T) {
	t.Parallel()

	for _, ec := range v1Enums() {
		values := ec.zero.EnumValues()
		require.Equal(t, ec.values, values, "enum table order")
		seen := make(map[string]bool, len(values))
		for _, v := range values {
			require.False(t, seen[v], "duplicate value")
			seen[v] = true
			require.True(t, ec.check(v), "Valid rejects listed value")
		}
		require.False(t, ec.check(""), "Valid accepts empty")
		if len(values) > 0 {
			first := values[0][0]
			var variant string
			switch {
			case first >= 'a' && first <= 'z':
				variant = string(rune(first-32)) + values[0][1:]
			case first >= 'A' && first <= 'Z':
				variant = string(rune(first+32)) + values[0][1:]
			default:
				variant = values[0] + "x"
			}
			if !seen[variant] {
				require.False(t, ec.check(variant), "Valid accepts case variant")
			}
		}
	}
}
