package protocol

// This file holds the Control API method enum and specs of §6.8 (v1.1):
// JSON-RPC 2.0 over daemon.sock. The actor is the authenticated socket
// peer, never a parameter. No method is available through MCP.

// ControlMethod names one Control API method, in spec table order.
type ControlMethod string

// ControlMethod values.
const (
	ControlMethodChannelGetGlobal      ControlMethod = "channel.get_global"
	ControlMethodChannelSetGlobal      ControlMethod = "channel.set_global"
	ControlMethodChannelGetNode        ControlMethod = "channel.get_node"
	ControlMethodChannelSetNode        ControlMethod = "channel.set_node"
	ControlMethodChannelGetEffective   ControlMethod = "channel.get_effective"
	ControlMethodChannelExplain        ControlMethod = "channel.explain"
	ControlMethodAgentChannelGet       ControlMethod = "agent_channel.get"
	ControlMethodAgentChannelSet       ControlMethod = "agent_channel.set"
	ControlMethodAgentChannelSetMode   ControlMethod = "agent_channel.set_mode"
	ControlMethodLinkGet               ControlMethod = "link.get"
	ControlMethodLinkList              ControlMethod = "link.list"
	ControlMethodLinkSet               ControlMethod = "link.set"
	ControlMethodLinkDeleteOverride    ControlMethod = "link.delete_override"
	ControlMethodLoopCreate            ControlMethod = "loop.create"
	ControlMethodLoopGet               ControlMethod = "loop.get"
	ControlMethodLoopList              ControlMethod = "loop.list"
	ControlMethodLoopSetMode           ControlMethod = "loop.set_mode"
	ControlMethodLoopAdvance           ControlMethod = "loop.advance"
	ControlMethodLoopExtendBudget      ControlMethod = "loop.extend_budget"
	ControlMethodLoopStop              ControlMethod = "loop.stop"
	ControlMethodLoopComplete          ControlMethod = "loop.complete"
	ControlMethodApprovalList          ControlMethod = "approval.list"
	ControlMethodApprovalGet           ControlMethod = "approval.get"
	ControlMethodApprovalResolve       ControlMethod = "approval.resolve"
	ControlMethodMessageApprove        ControlMethod = "message.approve"
	ControlMethodMessageReject         ControlMethod = "message.reject"
	ControlMethodMessageReleaseHold    ControlMethod = "message.release_hold"
	ControlMethodNodeEnrollToken       ControlMethod = "node.enroll_token" //nolint:gosec // RPC method name, not a credential.
	ControlMethodNodeJoin              ControlMethod = "node.join"
	ControlMethodNodePeers             ControlMethod = "node.peers"
	ControlMethodNodeRevoke            ControlMethod = "node.revoke"
	ControlMethodAgentList             ControlMethod = "agent.list"
	ControlMethodMessageList           ControlMethod = "message.list"
	ControlMethodDeliveryList          ControlMethod = "delivery.list"
	ControlMethodNotificationList      ControlMethod = "notification.list"
	ControlMethodDecisionList          ControlMethod = "decision.list"
	ControlMethodDecisionGet           ControlMethod = "decision.get"
	ControlMethodModelStatus           ControlMethod = "model.status"
	ControlMethodModelRegister         ControlMethod = "model.register"
	ControlMethodModelRegisterPipeline ControlMethod = "model.register_pipeline"
	ControlMethodModelActivate         ControlMethod = "model.activate"
	ControlMethodModelRollback         ControlMethod = "model.rollback"
)

// EnumValues returns the methods in table order.
func (ControlMethod) EnumValues() []string {
	return []string{
		string(ControlMethodChannelGetGlobal),
		string(ControlMethodChannelSetGlobal),
		string(ControlMethodChannelGetNode),
		string(ControlMethodChannelSetNode),
		string(ControlMethodChannelGetEffective),
		string(ControlMethodChannelExplain),
		string(ControlMethodAgentChannelGet),
		string(ControlMethodAgentChannelSet),
		string(ControlMethodAgentChannelSetMode),
		string(ControlMethodLinkGet),
		string(ControlMethodLinkList),
		string(ControlMethodLinkSet),
		string(ControlMethodLinkDeleteOverride),
		string(ControlMethodLoopCreate),
		string(ControlMethodLoopGet),
		string(ControlMethodLoopList),
		string(ControlMethodLoopSetMode),
		string(ControlMethodLoopAdvance),
		string(ControlMethodLoopExtendBudget),
		string(ControlMethodLoopStop),
		string(ControlMethodLoopComplete),
		string(ControlMethodApprovalList),
		string(ControlMethodApprovalGet),
		string(ControlMethodApprovalResolve),
		string(ControlMethodMessageApprove),
		string(ControlMethodMessageReject),
		string(ControlMethodMessageReleaseHold),
		string(ControlMethodNodeEnrollToken),
		string(ControlMethodNodeJoin),
		string(ControlMethodNodePeers),
		string(ControlMethodNodeRevoke),
		string(ControlMethodAgentList),
		string(ControlMethodMessageList),
		string(ControlMethodDeliveryList),
		string(ControlMethodNotificationList),
		string(ControlMethodDecisionList),
		string(ControlMethodDecisionGet),
		string(ControlMethodModelStatus),
		string(ControlMethodModelRegister),
		string(ControlMethodModelRegisterPipeline),
		string(ControlMethodModelActivate),
		string(ControlMethodModelRollback),
	}
}

// Valid reports whether the value is a known method.
func (v ControlMethod) Valid() bool {
	switch v {
	case ControlMethodChannelGetGlobal, ControlMethodChannelSetGlobal,
		ControlMethodChannelGetNode, ControlMethodChannelSetNode,
		ControlMethodChannelGetEffective, ControlMethodChannelExplain,
		ControlMethodAgentChannelGet, ControlMethodAgentChannelSet,
		ControlMethodAgentChannelSetMode, ControlMethodLinkGet,
		ControlMethodLinkList, ControlMethodLinkSet,
		ControlMethodLinkDeleteOverride, ControlMethodLoopCreate,
		ControlMethodLoopGet, ControlMethodLoopList,
		ControlMethodLoopSetMode, ControlMethodLoopAdvance,
		ControlMethodLoopExtendBudget, ControlMethodLoopStop,
		ControlMethodLoopComplete, ControlMethodApprovalList,
		ControlMethodApprovalGet, ControlMethodApprovalResolve,
		ControlMethodMessageApprove, ControlMethodMessageReject,
		ControlMethodMessageReleaseHold, ControlMethodNodeEnrollToken,
		ControlMethodNodeJoin, ControlMethodNodePeers, ControlMethodNodeRevoke,
		ControlMethodAgentList, ControlMethodMessageList, ControlMethodDeliveryList,
		ControlMethodNotificationList,
		ControlMethodDecisionList,
		ControlMethodDecisionGet, ControlMethodModelStatus,
		ControlMethodModelRegister, ControlMethodModelRegisterPipeline,
		ControlMethodModelActivate, ControlMethodModelRollback:
		return true
	default:
		return false
	}
}

// ControlMethodSpec binds one method to its params and result roots.
type ControlMethodSpec struct {
	// Method is the method name.
	Method ControlMethod
	// NewParams returns a pointer to a zero value of the params type.
	NewParams func() any
	// NewResult returns a pointer to a zero value of the result type.
	NewResult func() any
	// Mutates reports whether the method writes control state.
	Mutates bool
}

// ControlMethodSpecs returns one spec per method, in ControlMethod order.
// The slice is fresh.
func ControlMethodSpecs() []ControlMethodSpec {
	return []ControlMethodSpec{
		{Method: ControlMethodChannelGetGlobal, NewParams: func() any { return &EmptyParams{} }, NewResult: func() any { return &ChannelState{} }},
		{Method: ControlMethodChannelSetGlobal, NewParams: func() any { return &SetGlobalParams{} }, NewResult: func() any { return &ChannelState{} }, Mutates: true},
		{Method: ControlMethodChannelGetNode, NewParams: func() any { return &NodeParams{} }, NewResult: func() any { return &ChannelState{} }},
		{Method: ControlMethodChannelSetNode, NewParams: func() any { return &SetNodeParams{} }, NewResult: func() any { return &ChannelState{} }, Mutates: true},
		{Method: ControlMethodChannelGetEffective, NewParams: func() any { return &EffectiveParams{} }, NewResult: func() any { return &EffectiveView{} }},
		{Method: ControlMethodChannelExplain, NewParams: func() any { return &ExplainParams{} }, NewResult: func() any { return &Explanation{} }},
		{Method: ControlMethodAgentChannelGet, NewParams: func() any { return &AgentParams{} }, NewResult: func() any { return &AgentChannel{} }},
		{Method: ControlMethodAgentChannelSet, NewParams: func() any { return &SetAgentChannelParams{} }, NewResult: func() any { return &AgentChannel{} }, Mutates: true},
		{Method: ControlMethodAgentChannelSetMode, NewParams: func() any { return &SetAgentModeParams{} }, NewResult: func() any { return &AgentChannel{} }, Mutates: true},
		{Method: ControlMethodLinkGet, NewParams: func() any { return &LinkParams{} }, NewResult: func() any { return &LinkState{} }},
		{Method: ControlMethodLinkList, NewParams: func() any { return &ListLinksParams{} }, NewResult: func() any { return &LinkList{} }},
		{Method: ControlMethodLinkSet, NewParams: func() any { return &SetLinkParams{} }, NewResult: func() any { return &LinkList{} }, Mutates: true},
		{Method: ControlMethodLinkDeleteOverride, NewParams: func() any { return &DeleteLinkOverrideParams{} }, NewResult: func() any { return &LinkState{} }, Mutates: true},
		{Method: ControlMethodLoopCreate, NewParams: func() any { return &CreateLoopParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodLoopGet, NewParams: func() any { return &LoopParams{} }, NewResult: func() any { return &Loop{} }},
		{Method: ControlMethodLoopList, NewParams: func() any { return &ListLoopsParams{} }, NewResult: func() any { return &LoopList{} }},
		{Method: ControlMethodLoopSetMode, NewParams: func() any { return &SetLoopModeParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodLoopAdvance, NewParams: func() any { return &AdvanceLoopParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodLoopExtendBudget, NewParams: func() any { return &ExtendLoopBudgetParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodLoopStop, NewParams: func() any { return &StopLoopParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodLoopComplete, NewParams: func() any { return &CompleteLoopParams{} }, NewResult: func() any { return &Loop{} }, Mutates: true},
		{Method: ControlMethodApprovalList, NewParams: func() any { return &ListApprovalsParams{} }, NewResult: func() any { return &ApprovalList{} }},
		{Method: ControlMethodApprovalGet, NewParams: func() any { return &ApprovalParams{} }, NewResult: func() any { return &ApprovalRequest{} }},
		{Method: ControlMethodApprovalResolve, NewParams: func() any { return &ResolveApprovalParams{} }, NewResult: func() any { return &ApprovalRequest{} }, Mutates: true},
		{Method: ControlMethodMessageApprove, NewParams: func() any { return &ApproveMessageParams{} }, NewResult: func() any { return &ObjectControlState{} }, Mutates: true},
		{Method: ControlMethodMessageReject, NewParams: func() any { return &RejectMessageParams{} }, NewResult: func() any { return &ObjectControlState{} }, Mutates: true},
		{Method: ControlMethodMessageReleaseHold, NewParams: func() any { return &ReleaseHoldParams{} }, NewResult: func() any { return &ObjectControlState{} }, Mutates: true},
		{Method: ControlMethodNodeEnrollToken, NewParams: func() any { return &NodeEnrollTokenParams{} }, NewResult: func() any { return &NodeEnrollTokenResult{} }, Mutates: true},
		{Method: ControlMethodNodeJoin, NewParams: func() any { return &NodeJoinParams{} }, NewResult: func() any { return &NodeJoinResult{} }, Mutates: true},
		{Method: ControlMethodNodePeers, NewParams: func() any { return &EmptyParams{} }, NewResult: func() any { return &NodePeersResult{} }},
		{Method: ControlMethodNodeRevoke, NewParams: func() any { return &NodeRevokeParams{} }, NewResult: func() any { return &NodeRevokeResult{} }, Mutates: true},
		{Method: ControlMethodAgentList, NewParams: func() any { return &AgentListParams{} }, NewResult: func() any { return &AgentListResult{} }},
		{Method: ControlMethodMessageList, NewParams: func() any { return &MessageListParams{} }, NewResult: func() any { return &MessageListResult{} }},
		{Method: ControlMethodDeliveryList, NewParams: func() any { return &MessageListParams{} }, NewResult: func() any { return &DeliveryListResult{} }},
		{Method: ControlMethodNotificationList, NewParams: func() any { return &NotificationListParams{} }, NewResult: func() any { return &NotificationListResult{} }},
		{Method: ControlMethodDecisionList, NewParams: func() any { return &DecisionListParams{} }, NewResult: func() any { return &DecisionListResult{} }},
		{Method: ControlMethodDecisionGet, NewParams: func() any { return &DecisionGetParams{} }, NewResult: func() any { return &DecisionGetResult{} }},
		{Method: ControlMethodModelStatus, NewParams: func() any { return &EmptyParams{} }, NewResult: func() any { return &ModelStatusResult{} }},
		{Method: ControlMethodModelRegister, NewParams: func() any { return &RegisterModelMetadataRequest{} }, NewResult: func() any { return &ModelMetadataRegistrationResult{} }, Mutates: true},
		{Method: ControlMethodModelRegisterPipeline, NewParams: func() any { return &RegisterModelPipelineRequest{} }, NewResult: func() any { return &ModelMetadataRegistrationResult{} }, Mutates: true},
		{Method: ControlMethodModelActivate, NewParams: func() any { return &ActivateAdapterRequest{} }, NewResult: func() any { return &ModelActivationResult{} }, Mutates: true},
		{Method: ControlMethodModelRollback, NewParams: func() any { return &RollbackToBaseRequest{} }, NewResult: func() any { return &ModelActivationResult{} }, Mutates: true},
	}
}
