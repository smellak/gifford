package protocol

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testReadAgentID = "worker-1"

func TestPanelReadMethodsDoNotMutate(t *testing.T) {
	t.Parallel()

	want := map[ControlMethod]bool{
		ControlMethodAgentList:        true,
		ControlMethodMessageList:      true,
		ControlMethodNotificationList: true,
		ControlMethodDecisionList:     true,
		ControlMethodDecisionGet:      true,
		ControlMethodModelStatus:      true,
	}
	for _, spec := range ControlMethodSpecs() {
		if want[spec.Method] {
			require.False(t, spec.Mutates, "%s is read-only", spec.Method)
			delete(want, spec.Method)
		}
	}
	require.Empty(t, want, "every panel read has a method spec")
}

func TestReadListParams(t *testing.T) {
	t.Parallel()

	one, twoHundred := int64(1), int64(200)
	for name, params := range map[string]interface {
		Validate() error
		EffectiveLimit() int64
	}{
		"agents default":   AgentListParams{},
		"agents minimum":   AgentListParams{Limit: &one},
		"messages maximum": MessageListParams{Limit: &twoHundred, AgentID: testReadAgentID, State: DeliveryQueueStateQueued},
		"notifications":    NotificationListParams{Limit: &one, State: NotificationStatePending},
		"decisions filter": DecisionListParams{AgentID: testReadAgentID, MessageID: testMsgID, Type: DecisionTypeChannel},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, params.Validate())
		})
	}
	require.Equal(t, DefaultListLimit, AgentListParams{}.EffectiveLimit())
	require.Equal(t, int64(1), AgentListParams{Limit: &one}.EffectiveLimit())
	require.Equal(t, MaxListLimit, MessageListParams{Limit: &twoHundred}.EffectiveLimit())
}

func TestReadListParamsRejectInvalidValues(t *testing.T) {
	t.Parallel()

	zero, tooLarge := int64(0), MaxListLimit+1
	tests := map[string]interface{ Validate() error }{
		"zero limit":             AgentListParams{Limit: &zero},
		"large limit":            MessageListParams{Limit: &tooLarge},
		"bad cursor":             DecisionListParams{Cursor: "not.a.cursor"},
		"bad recipient":          MessageListParams{AgentID: "Bad Agent"},
		"bad delivery state":     MessageListParams{State: "SENDING"},
		"bad notification state": NotificationListParams{State: "SENDING"},
		"bad decision type":      DecisionListParams{Type: "policy"},
		"bad message filter":     DecisionListParams{MessageID: "not-a-ulid"},
		"bad decision lookup":    DecisionGetParams{DecisionID: "not-a-decision"},
	}
	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, params.Validate())
		})
	}
}

func TestNotificationListItemRules(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 10, 30, 18, 0, 0, 0, time.UTC)
	item := NotificationListItem{
		NotificationID: "notification-1", Class: NotificationClassMessage,
		ObjectID: testMsgID, SenderAgent: testReadAgentID,
		RecipientAgents: []AgentID{"worker-2"}, Priority: PriorityHigh,
		Reason: NotificationReasonMessageAvailable, State: NotificationStatePending,
		Adapter: NotificationAdapterTMUX, CreatedAt: when, UpdatedAt: when,
	}
	require.NoError(t, item.Validate())

	service := item
	service.Class = "SERVICE"
	require.Error(t, service.Validate(), "SERVICE rows are not public")

	wrongObject := item
	wrongObject.Class = NotificationClassApproval
	wrongObject.Reason = NotificationReasonApprovalPending
	require.Error(t, wrongObject.Validate())

	rendered := item
	rendered.State = NotificationStateRendered
	require.Error(t, rendered.Validate())
	rendered.RenderedAt = &when
	require.NoError(t, rendered.Validate())
}

func TestReadScalars(t *testing.T) {
	t.Parallel()

	require.True(t, PageCursor("eyJpZCI6IjAxIn0").Valid())
	require.False(t, PageCursor("has.dot").Valid())
	require.True(t, DecisionID(testMsgID).Valid())
	require.True(t, DecisionID("chd_"+testMsgID).Valid())
	require.False(t, DecisionID("chd_bad").Valid())
	require.True(t, ReasonCode("CONTROL_CHANGED").Valid())
	require.False(t, ReasonCode("operator explanation").Valid())
	metadataPattern := regexp.MustCompile(ModelMetadataValue("").JSONPattern())
	for _, value := range []string{
		"repo/rev:v1+cuda",
		"base-rev-1",
		"/private/model",
		".model",
		":revision",
		"free text",
		"",
	} {
		require.Equal(t, metadataPattern.MatchString(value), ModelMetadataValue(value).Valid(), value)
	}
}

func TestReadAgentEvidenceRules(t *testing.T) {
	t.Parallel()

	require.NoError(t, AgentPhaseStatus{Value: AgentPhaseUnknown, Source: AgentPhaseSourceNone}.Validate())
	require.NoError(t, AgentPhaseStatus{Value: AgentPhaseUnknown, Source: AgentPhaseSourceAnalyst}.Validate())
	require.Error(t, AgentPhaseStatus{Value: AgentPhaseIdleAtPrompt, Source: AgentPhaseSourceNone}.Validate())

	item := validAgentListItem()
	require.Nil(t, item.Keyboard)
	require.NoError(t, item.Validate(), "required nullable keyboard accepts explicit null")
	item.Keyboard = &AgentKeyboardState{
		DraftState:        DraftStateClosed,
		ActivityState:     ActivityStateUnknown,
		InputGuardVersion: 7,
		ObservedAt:        time.Date(2026, 10, 30, 18, 0, 0, 0, time.UTC),
	}
	require.NoError(t, item.Validate(), "CLOSED draft does not infer an IDLE phase")
	item.Phase = AgentPhaseStatus{Value: AgentPhaseIdleAtPrompt, Source: AgentPhaseSourceAnalyst}
	require.NoError(t, item.Validate())
}

func TestReadDecisionFamilies(t *testing.T) {
	t.Parallel()

	channel := validDecisionGetResult()
	require.NoError(t, channel.Validate())

	orchestrator := channel
	orchestrator.DecisionID = DecisionID(testMsgID)
	orchestrator.Type = DecisionTypeOrchestrator
	orchestrator.Stage = ""
	orchestrator.Outcome = ""
	orchestrator.SelectedAction = OrchestratorActionQueue
	orchestrator.ReasonCodes = []ReasonCode{"human_draft_closed"}
	orchestrator.ChannelEvidence = nil
	orchestrator.OrchestratorEvidence = &OrchestratorDecisionEvidence{
		AllowedActions:    []OrchestratorAction{OrchestratorActionQueue, OrchestratorActionDefer},
		Guards:            []DecisionGuardEvidence{{Guard: GuardNameHumanDraftClosed, Outcome: GuardOutcomeFail}},
		SelectedAction:    OrchestratorActionQueue,
		PolicyVersion:     "policy-v1",
		ExecutionStatus:   ExecutionStatusPending,
		ChannelDecisionID: ChannelDecisionID("chd_" + testMsgID),
	}
	require.NoError(t, orchestrator.Validate())

	wrongEvidence := channel
	wrongEvidence.ChannelEvidence = nil
	require.Error(t, wrongEvidence.Validate())

	for _, stage := range []DecisionStage{DecisionStageAcceptance, DecisionStageContinuation} {
		withStage := channel
		withStage.Stage = stage
		require.NoError(t, withStage.Validate(), stage)
	}
	missingStage := channel
	missingStage.Stage = ""
	require.Error(t, missingStage.Validate())

	wrongAction := orchestrator
	wrongAction.OrchestratorEvidence.SelectedAction = OrchestratorActionDefer
	require.Error(t, wrongAction.Validate())
}

func TestModelStatusUnknownMetadata(t *testing.T) {
	t.Parallel()

	status := ModelStatusResult{
		Deployment: ModelDeploymentMetadata{
			BaseRevision:    ModelMetadata{Availability: MetadataAvailabilityUnknown},
			AdapterRevision: ModelMetadata{Availability: MetadataAvailabilityUnavailable},
			DatasetVersion:  ModelMetadata{Availability: MetadataAvailabilityUnknown},
			ABVariant:       ModelMetadata{Availability: MetadataAvailabilityUnavailable},
		},
	}
	require.NoError(t, status.Validate(), "null profile and last call are explicit and valid")

	status.Deployment.BaseRevision.Value = "invented-revision"
	require.Error(t, status.Validate(), "unknown metadata cannot carry a fabricated value")
	status.Deployment.BaseRevision.Availability = MetadataAvailabilityKnown
	require.NoError(t, status.Validate())
}

func validAgentListItem() AgentListItem {
	return AgentListItem{
		AgentID:        testReadAgentID,
		NodeID:         "node-1",
		Harness:        HarnessCodex,
		HarnessVersion: "1.2.3",
		Mode:           AgentRunModeObserved,
		SessionState:   SessionStateObserved,
		Phase:          AgentPhaseStatus{Value: AgentPhaseUnknown, Source: AgentPhaseSourceNone},
		Channel:        AgentEffectiveChannel{TX: true, RX: true, AutoDelivery: true, AutoContinue: true, Mode: AgentModeFull},
		Queue:          AgentQueueCounts{},
	}
}

func validDecisionGetResult() DecisionGetResult {
	return DecisionGetResult{
		DecisionID:  DecisionID("chd_" + testMsgID),
		Type:        DecisionTypeChannel,
		Stage:       DecisionStageDelivery,
		Outcome:     DecisionOutcomeHold,
		AgentID:     testReadAgentID,
		MessageID:   testMsgID,
		CreatedAt:   time.Date(2026, 10, 30, 18, 0, 0, 0, time.UTC),
		ReasonCodes: []ReasonCode{"CONTROL_CHANGED"},
		ChannelEvidence: &ChannelDecisionEvidence{
			ControlVersions: ControlVersions{"global:*": 3},
			DecidingScope:   ExplanationScopeGlobal,
		},
	}
}
