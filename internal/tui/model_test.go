package tui

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
)

var updatePanelGoldens = flag.Bool("update", false, "rewrite panel golden files")

type recordingCaller struct {
	mu                  sync.Mutex
	calls               []protocol.ControlMethod
	global              protocol.ChannelState
	node                protocol.ChannelState
	agents              protocol.AgentListResult
	messages            protocol.DeliveryListResult
	approvals           protocol.ApprovalList
	notifications       protocol.NotificationListResult
	decisions           protocol.DecisionListResult
	detail              protocol.DecisionGetResult
	modelStatus         protocol.ModelStatusResult
	resolvedApproval    protocol.ApprovalRequest
	err                 error
	approvalResolveErr  error
	notificationErr     error
	approvalListParams  []protocol.ListApprovalsParams
	approvalResolutions []protocol.ResolveApprovalParams
	notificationParams  []protocol.NotificationListParams
}

func (c *recordingCaller) Call(
	_ context.Context,
	method protocol.ControlMethod,
	params, result any,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, method)
	if c.err != nil {
		return c.err
	}
	if method == protocol.ControlMethodNotificationList {
		if value, ok := params.(protocol.NotificationListParams); ok {
			c.notificationParams = append(c.notificationParams, value)
		}
		if c.notificationErr != nil {
			return c.notificationErr
		}
	}
	if method == protocol.ControlMethodApprovalList {
		if value, ok := params.(protocol.ListApprovalsParams); ok {
			c.approvalListParams = append(c.approvalListParams, value)
		}
	}
	if method == protocol.ControlMethodApprovalResolve {
		if value, ok := params.(protocol.ResolveApprovalParams); ok {
			c.approvalResolutions = append(c.approvalResolutions, value)
		}
		if c.approvalResolveErr != nil {
			return c.approvalResolveErr
		}
	}
	var source any
	switch method {
	case protocol.ControlMethodChannelGetGlobal:
		source = c.global
	case protocol.ControlMethodChannelGetNode:
		source = c.node
	case protocol.ControlMethodAgentList:
		source = c.agents
	case protocol.ControlMethodDeliveryList:
		source = c.messages
	case protocol.ControlMethodApprovalList:
		source = c.approvals
	case protocol.ControlMethodApprovalResolve:
		source = c.resolvedApproval
	case protocol.ControlMethodNotificationList:
		source = c.notifications
	case protocol.ControlMethodDecisionList:
		source = c.decisions
	case protocol.ControlMethodDecisionGet:
		source = c.detail
	case protocol.ControlMethodModelStatus:
		source = c.modelStatus
	default:
		return ErrMethodNotAllowed
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, result)
}

func (c *recordingCaller) approvalListRequests() []protocol.ListApprovalsParams {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.ListApprovalsParams(nil), c.approvalListParams...)
}

func (c *recordingCaller) approvalResolveRequests() []protocol.ResolveApprovalParams {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.ResolveApprovalParams(nil), c.approvalResolutions...)
}

func (c *recordingCaller) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *recordingCaller) methods() []protocol.ControlMethod {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.ControlMethod(nil), c.calls...)
}

func (c *recordingCaller) notificationRequests() []protocol.NotificationListParams {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.NotificationListParams(nil), c.notificationParams...)
}

func TestUpdateReturnsIOAsCommand(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{
		global: channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive),
		node:   channelState(protocol.ScopeTypeNode, string(panelTestNodeID), protocol.ChannelModeActive),
	}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller

	updated, command := model.Update(refreshChannelMsg{})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount(), "Update must not perform the read")
	message := command()
	require.Equal(t, 2, caller.callCount())
	updated, command = updated.Update(message)
	require.NotNil(t, command, "Update schedules the next refresh through tea.Tick")
	result := updated.(Model)
	require.True(t, result.connected)
	require.Equal(t, protocol.ChannelModeActive, result.global.Mode)
}

func TestActivePanelReadRunsOnlyInCommand(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{agents: protocol.AgentListResult{Agents: []protocol.AgentListItem{
		panelAgent(),
	}}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.connected = true
	model.global = channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive)
	updated, command := model.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount(), "Update must not perform a panel read")
	message := command()
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodAgentList}, caller.methods())
	updated, refresh := updated.Update(message)
	require.NotNil(t, refresh)
	result := updated.(Model)
	require.True(t, result.panelReady)
	require.Equal(t, readPanelAgentID, result.agents.Agents[0].AgentID)
	require.Contains(t, ansi.Strip(result.View().Content), string(readPanelAgentID))
}

func TestApprovalPanelReadsPendingMetadataOnly(t *testing.T) {
	t.Parallel()

	approval := panelApproval()
	caller := &recordingCaller{approvals: protocol.ApprovalList{
		Approvals: []protocol.ApprovalRequest{approval},
	}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.clock = panelClock{now: panelTestTime}
	model.connected = true
	model.global = channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive)
	model.activeTab = approvalsTab

	updated, command := model.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount(), "Update must not read approvals")
	message := command().(panelSnapshotMsg)
	require.NoError(t, message.err)
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodApprovalList}, caller.methods())
	requests := caller.approvalListRequests()
	require.Len(t, requests, 1)
	require.Equal(t, protocol.ApprovalStatePending, requests[0].State)

	updated, refresh := updated.Update(message)
	require.NotNil(t, refresh)
	view := ansi.Strip(updated.(Model).View().Content)
	require.Contains(t, view, string(approval.ApprovalID))
	require.Contains(t, view, "agente="+string(approval.AgentID))
	require.Contains(t, view, "objeto=message:"+approval.ObjectID)
	require.Contains(t, view, "nota="+approval.RequestNote)
	require.Contains(t, view, "estado=PENDING")
	require.Contains(t, view, "edad=4m")
}

func TestApprovalActionsRunOnlyInCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        rune
		option     protocol.ApprovalOption
		reason     string
		finalState protocol.ApprovalState
	}{
		{
			name: "approve and deliver now", key: 'a', option: approvalDeliverNowOption,
			finalState: protocol.ApprovalStateApproved,
		},
		{
			name: "reject", key: 'r', option: approvalRejectOption,
			reason: panelRejectReason, finalState: protocol.ApprovalStateRejected,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			approval := panelApproval()
			resolved := approval
			resolved.State = test.finalState
			caller := &recordingCaller{resolvedApproval: resolved}
			model := NewModel(t.Context(), nil, panelTestNodeID)
			model.client = caller
			model.width, model.height = 120, 40
			model.connected, model.panelReady = true, true
			model.activeTab = approvalsTab
			model.approvals = protocol.ApprovalList{
				Approvals: []protocol.ApprovalRequest{approval},
			}

			updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: test.key, Text: string(test.key)}))
			require.NotNil(t, command)
			require.Zero(t, caller.callCount(), "Update must not resolve an approval")
			busy := updated.(Model)
			require.True(t, busy.approvalActionBusy)
			message := command().(approvalActionMsg)
			require.NoError(t, message.err)
			require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodApprovalResolve}, caller.methods())
			requests := caller.approvalResolveRequests()
			require.Equal(t, []protocol.ResolveApprovalParams{{
				ApprovalID: approval.ApprovalID, Option: test.option, Reason: test.reason,
			}}, requests)

			updated, refresh := updated.Update(message)
			require.NotNil(t, refresh)
			result := updated.(Model)
			require.False(t, result.approvalActionBusy)
			require.Contains(t, ansi.Strip(result.View().Content), "resuelta: "+string(test.finalState))
		})
	}
}

func TestDeferredApprovalStaysVisibleWithOneLineReason(t *testing.T) {
	t.Parallel()

	approval := panelApproval()
	caller := &recordingCaller{approvalResolveErr: errors.New(
		"deliver_now deferred; the request is still pending",
	)}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height = 120, 40
	model.connected, model.panelReady = true, true
	model.activeTab = approvalsTab
	model.approvals = protocol.ApprovalList{Approvals: []protocol.ApprovalRequest{approval}}

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'a', Text: "a"}))
	require.NotNil(t, command)
	message := command().(approvalActionMsg)
	require.Error(t, message.err)
	updated, refresh := updated.Update(message)
	require.NotNil(t, refresh)
	result := updated.(Model)
	require.Len(t, result.approvals.Approvals, 1, "deferred delivery must leave the approval pending")
	view := ansi.Strip(result.View().Content)
	require.Contains(t, view, string(approval.ApprovalID))
	require.Contains(t, view, "deliver_now deferred; the request is still pending")
}

func TestApprovalActionTargetsSelectedRow(t *testing.T) {
	t.Parallel()

	first := panelApproval()
	second := panelApproval()
	second.ApprovalID = "apr_01JB0000000000000000000056"
	caller := &recordingCaller{resolvedApproval: second}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height = 120, 40
	model.connected, model.panelReady = true, true
	model.activeTab = approvalsTab
	model.approvals = protocol.ApprovalList{
		Approvals: []protocol.ApprovalRequest{first, second},
	}

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	require.Nil(t, command)
	selected := updated.(Model)
	require.Equal(t, 1, selected.approvalIndex)
	updated, command = selected.Update(tea.KeyPressMsg(tea.Key{Code: 'a', Text: "a"}))
	require.NotNil(t, command)
	_ = command()
	require.Equal(t, second.ApprovalID, caller.approvalResolveRequests()[0].ApprovalID)
	require.True(t, updated.(Model).approvalActionBusy)
}

func TestLivePanelRefreshesBelowOneSecond(t *testing.T) {
	t.Parallel()

	require.Less(t, panelRefreshInterval, time.Second)
	require.Less(t, approvalHeaderRefreshInterval, time.Second)
}

func TestFirstSmallWindowStillStartsPanelRead(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	_, command := model.Update(tea.WindowSizeMsg{Width: 40, Height: 10})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount())
	_ = command()
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodAgentList}, caller.methods())
}

func TestDecisionEnterFetchesStructuredDetail(t *testing.T) {
	t.Parallel()

	decision := panelDecision()
	caller := &recordingCaller{detail: protocol.DecisionGetResult{
		DecisionID: decision.DecisionID, Type: decision.Type, Stage: decision.Stage,
		Outcome: decision.Outcome, AgentID: decision.AgentID, MessageID: decision.MessageID,
		CreatedAt: decision.CreatedAt, ReasonCodes: decision.ReasonCodes,
		ChannelEvidence: &protocol.ChannelDecisionEvidence{
			ControlVersions: protocol.ControlVersions{"global:*": 3},
			DecidingScope:   protocol.ExplanationScopeReceiver,
		},
	}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height, model.activeTab = 120, 40, decisionsTab
	model.connected, model.panelReady = true, true
	model.decisions = protocol.DecisionListResult{Decisions: []protocol.DecisionListItem{decision}}
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	require.NotNil(t, command)
	require.Zero(t, caller.callCount())
	message := command()
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodDecisionGet}, caller.methods())
	updated, _ = updated.Update(message)
	view := ansi.Strip(updated.(Model).View().Content)
	require.Contains(t, view, "scope decisivo: receiver")
	require.Contains(t, view, "global:*=3")
	require.NotContains(t, view, "explanation")
}

func TestPanelReadFailureClearsPreviouslyRenderedData(t *testing.T) {
	t.Parallel()

	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.width, model.height = 120, 40
	model.connected, model.panelReady = true, true
	model.agents = protocol.AgentListResult{Agents: []protocol.AgentListItem{panelAgent()}}
	updated, refresh := model.Update(panelSnapshotMsg{
		tab: agentsTab, limit: model.visibleRows(), at: panelTestTime,
		err: context.DeadlineExceeded,
	})
	require.NotNil(t, refresh)
	result := updated.(Model)
	require.ErrorIs(t, result.panelErr, context.DeadlineExceeded)
	require.Empty(t, result.agents.Agents)
	view := ansi.Strip(result.View().Content)
	require.Contains(t, view, "lectura no disponible")
	require.NotContains(t, view, string(readPanelAgentID))
}

func TestPanelEpochRejectsResponseFromPreviousTabVisit(t *testing.T) {
	t.Parallel()

	setupReturn := func(t *testing.T) (Model, panelSnapshotMsg) {
		t.Helper()
		model := NewModel(t.Context(), nil, panelTestNodeID)
		model.width, model.height = 120, 40
		model.connected = true
		model.panelEpoch = 1
		oldAgent := panelAgent()
		oldAgent.AgentID = "agent-old"
		old := panelSnapshotMsg{
			tab: agentsTab, epoch: model.panelEpoch, limit: model.visibleRows(),
			agents: protocol.AgentListResult{Agents: []protocol.AgentListItem{oldAgent}},
		}

		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyRight}))
		require.NotNil(t, command)
		updated, command = updated.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyLeft}))
		require.NotNil(t, command)
		returned := updated.(Model)
		require.Equal(t, agentsTab, returned.activeTab)
		require.Equal(t, uint64(3), returned.panelEpoch)
		return returned, old
	}

	t.Run("old data cannot replace fresh data", func(t *testing.T) {
		t.Parallel()
		model, old := setupReturn(t)
		freshAgent := panelAgent()
		freshAgent.AgentID = "agent-fresh"
		fresh := panelSnapshotMsg{
			tab: agentsTab, epoch: model.panelEpoch, limit: model.visibleRows(),
			agents: protocol.AgentListResult{Agents: []protocol.AgentListItem{freshAgent}},
		}
		updated, timer := model.Update(fresh)
		require.NotNil(t, timer)
		updated, duplicate := updated.Update(old)
		require.Nil(t, duplicate, "an obsolete response must not start another polling chain")
		result := updated.(Model)
		require.True(t, result.panelReady)
		require.NoError(t, result.panelErr)
		require.Equal(t, protocol.AgentID("agent-fresh"), result.agents.Agents[0].AgentID)
	})

	t.Run("old data cannot erase a recent error", func(t *testing.T) {
		t.Parallel()
		model, old := setupReturn(t)
		current := panelSnapshotMsg{
			tab: agentsTab, epoch: model.panelEpoch, limit: model.visibleRows(),
			err: context.DeadlineExceeded,
		}
		updated, timer := model.Update(current)
		require.NotNil(t, timer)
		updated, duplicate := updated.Update(old)
		require.Nil(t, duplicate)
		result := updated.(Model)
		require.ErrorIs(t, result.panelErr, context.DeadlineExceeded)
		require.Empty(t, result.agents.Agents)
	})
}

func TestObsoletePanelTimerDoesNotReadOrRestart(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height = 120, 40
	model.panelEpoch = 7

	updated, command := model.Update(refreshPanelMsg{tab: agentsTab, epoch: 6})
	require.Nil(t, command)
	require.Equal(t, model.panelEpoch, updated.(Model).panelEpoch)
	require.Zero(t, caller.callCount(), "Update must not perform a panel read")

	_, command = model.Update(refreshPanelMsg{tab: agentsTab, epoch: model.panelEpoch})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount(), "a current timer still defers I/O to its command")
	message := command().(panelSnapshotMsg)
	require.Equal(t, model.panelEpoch, message.epoch)
	require.Equal(t, 1, caller.callCount())
}

func TestMissingNodeIDSkipsNodeRead(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{
		global: channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive),
	}
	message := fetchChannelCmd(t.Context(), caller, "")().(channelSnapshotMsg)
	require.NoError(t, message.err)
	require.False(t, message.hasNode)
	require.Equal(t, 1, caller.callCount())

	model := NewModel(t.Context(), nil, "")
	model.width = 80
	model.height = 24
	updated, _ := model.Update(message)
	require.Contains(t, ansi.Strip(updated.(Model).View().Content), "node=—")
}

func TestStoppedServerReturnsDisconnectedWithoutBlocking(t *testing.T) {
	t.Parallel()

	client, err := NewClient(ClientOptions{
		SocketPath: filepath.Join(t.TempDir(), "daemon.sock"),
		Timeout:    40 * time.Millisecond,
	})
	require.NoError(t, err)
	model := NewModel(t.Context(), client, panelTestNodeID)
	model.width = 80
	model.height = 24
	started := time.Now()
	message := model.Init()().(channelSnapshotMsg)
	require.Error(t, message.err)
	require.Less(t, time.Since(started), time.Second)
	updated, retry := model.Update(message)
	require.NotNil(t, retry)
	result := updated.(Model)
	require.False(t, result.connected)
	require.Contains(t, ansi.Strip(result.View().Content), "sin conexión con el nodo")
}

func TestFailedReadShowsDisconnectedAndBacksOff(t *testing.T) {
	t.Parallel()

	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.width = 80
	model.height = 24
	updated, command := model.Update(channelSnapshotMsg{err: context.DeadlineExceeded})
	require.NotNil(t, command)
	result := updated.(Model)
	require.False(t, result.connected)
	require.Equal(t, 2*initialRetryDelay, result.retryDelay)
	require.Contains(t, ansi.Strip(result.View().Content), "sin conexión con el nodo")
	for range 10 {
		updated, _ = result.Update(channelSnapshotMsg{err: context.DeadlineExceeded})
		result = updated.(Model)
	}
	require.Equal(t, maximumRetryDelay, result.retryDelay)
}

// TestModelStatusDisconnectLateResponseAndReconnect covers one polling
// generation end to end: disconnect clears the MODEL header, a late response
// and timer cannot revive it, and reconnect starts exactly one fresh read.
func TestModelStatusDisconnectLateResponseAndReconnect(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{modelStatus: knownPanelModel()}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height = 120, 40
	model.activeTab = modelTab
	model.connected, model.panelReady = true, true
	model.modelStatus = knownPanelModel()
	staleEpoch := model.panelEpoch
	require.Contains(t, model.modelHeader(), string(protocol.InferenceProfileAnalystLoRA))

	updated, retry := model.Update(channelSnapshotMsg{err: context.DeadlineExceeded})
	require.NotNil(t, retry)
	disconnected := updated.(Model)
	require.False(t, disconnected.connected)
	require.Equal(t, protocol.ModelStatusResult{}, disconnected.modelStatus)
	require.Equal(t, "MODEL —", disconnected.modelHeader())
	require.True(t, disconnected.panelAfterReconnect)

	late := panelSnapshotMsg{
		tab: modelTab, epoch: staleEpoch, limit: disconnected.visibleRows(),
		modelStatus: knownPanelModel(),
	}
	updated, duplicate := disconnected.Update(late)
	require.Nil(t, duplicate, "a disconnected response must not restart panel polling")
	disconnected = updated.(Model)
	require.Equal(t, protocol.ModelStatusResult{}, disconnected.modelStatus)
	require.Equal(t, "MODEL —", disconnected.modelHeader())

	updated, duplicate = disconnected.Update(refreshPanelMsg{tab: modelTab, epoch: staleEpoch})
	require.Nil(t, duplicate, "a disconnected timer must not restart panel polling")
	disconnected = updated.(Model)

	updated, reconnect := disconnected.Update(channelSnapshotMsg{
		global: channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive),
	})
	require.NotNil(t, reconnect)
	connected := updated.(Model)
	require.True(t, connected.connected)
	require.False(t, connected.panelAfterReconnect)
	require.Greater(t, connected.panelEpoch, staleEpoch)
	require.Equal(t, "MODEL —", connected.modelHeader(), "stale MODEL must stay hidden until the fresh read")

	batch, ok := reconnect().(tea.BatchMsg)
	require.True(t, ok, "reconnect must schedule channel, panel and header reads")
	require.Len(t, batch, 4)
	var fresh panelSnapshotMsg
	var notification notificationSnapshotMsg
	var approvalHeader approvalHeaderSnapshotMsg
	for _, command := range batch {
		switch message := command().(type) {
		case refreshChannelMsg:
		case panelSnapshotMsg:
			fresh = message
		case notificationSnapshotMsg:
			notification = message
		case approvalHeaderSnapshotMsg:
			approvalHeader = message
		default:
			t.Fatalf("unexpected reconnect command message %T", message)
		}
	}
	require.Equal(t, []protocol.ControlMethod{
		protocol.ControlMethodModelStatus, protocol.ControlMethodNotificationList,
		protocol.ControlMethodApprovalList,
	}, caller.methods())
	require.Equal(t, connected.panelEpoch, fresh.epoch)
	require.Equal(t, connected.notificationEpoch, notification.epoch)
	require.Equal(t, connected.approvalHeaderEpoch, approvalHeader.epoch)

	updated, panelRefresh := connected.Update(fresh)
	require.NotNil(t, panelRefresh)
	connected = updated.(Model)
	require.Contains(t, connected.modelHeader(), string(protocol.InferenceProfileAnalystLoRA))

	updated, channelOnly := connected.Update(channelSnapshotMsg{
		global: channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive),
	})
	require.NotNil(t, channelOnly)
	require.IsType(t, refreshChannelMsg{}, channelOnly())
	require.Equal(t, []protocol.ControlMethod{
		protocol.ControlMethodModelStatus, protocol.ControlMethodNotificationList,
		protocol.ControlMethodApprovalList,
	}, caller.methods(),
		"a second connected snapshot must not start another panel read")
	require.Contains(t, updated.(Model).modelHeader(), string(protocol.InferenceProfileAnalystLoRA))
}

func TestNavigationAndHelpUseSyntheticMessages(t *testing.T) {
	t.Parallel()

	model := NewModel(t.Context(), nil, "")
	model.width = 80
	model.height = 24
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	require.NotNil(t, command, "switching to a readable tab starts its read in a command")
	result := updated.(Model)
	require.Equal(t, 1, result.activeTab)

	updated, command = result.Update(tea.KeyPressMsg(tea.Key{Code: '?', Text: "?"}))
	require.Nil(t, command)
	result = updated.(Model)
	require.True(t, result.help)
	require.Contains(t, ansi.Strip(result.View().Content), "Ayuda")

	updated, _ = result.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyLeft}))
	require.Zero(t, updated.(Model).activeTab)
}

func TestEveryTabHasAnHonestEmptyState(t *testing.T) {
	t.Parallel()

	for index, tab := range panelTabs {
		model := NewModel(t.Context(), nil, "")
		model.width = 120
		model.height = 40
		model.activeTab = index
		view := ansi.Strip(model.View().Content)
		require.Contains(t, view, tab.name)
		if tab.readable {
			require.Contains(t, view, "sin conexión con el nodo")
		} else {
			require.Contains(t, view, "llega en una fase posterior")
		}
	}
}

func TestRefreshObservesServerStateChange(t *testing.T) {
	t.Parallel()

	client, controller := startControlServer(t)
	model := NewModel(t.Context(), client, "")
	first := model.Init()().(channelSnapshotMsg)
	require.NoError(t, first.err)
	updated, tick := model.Update(first)
	result := updated.(Model)
	require.Equal(t, protocol.ChannelModeActive, result.global.Mode)

	_, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModePaused,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"})
	require.NoError(t, err)
	started := time.Now()
	batch, ok := tick().(tea.BatchMsg)
	require.True(t, ok)
	var refresh refreshChannelMsg
	for _, command := range batch {
		switch message := command().(type) {
		case refreshChannelMsg:
			refresh = message
		case notificationSnapshotMsg:
		case approvalHeaderSnapshotMsg:
		default:
			t.Fatalf("unexpected initial command message %T", message)
		}
	}
	updated, read := result.Update(refresh)
	snapshot := read().(channelSnapshotMsg)
	require.NoError(t, snapshot.err)
	updated, _ = updated.Update(snapshot)
	require.Equal(t, protocol.ChannelModePaused, updated.(Model).global.Mode)
	require.Less(t, time.Since(started), time.Second)
}

func TestNotificationHeaderUsesOneBoundedRead(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{notifications: protocol.NotificationListResult{
		Notifications: []protocol.NotificationListItem{panelNotification()},
		PendingCount:  2,
	}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.clock = panelClock{now: panelTestTime}
	model.width, model.height = 120, 40
	model.connected = true
	model.global = channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive)
	model.node = channelState(protocol.ScopeTypeNode, string(panelTestNodeID), protocol.ChannelModeActive)
	model.hasNode = true
	model.panelReady = true

	command := model.fetchNotificationHeader()
	require.Zero(t, caller.callCount(), "model state access must not perform I/O")
	message := command().(notificationSnapshotMsg)
	require.NoError(t, message.err)
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodNotificationList}, caller.methods())
	requests := caller.notificationRequests()
	require.Len(t, requests, 1)
	require.NotNil(t, requests[0].Limit)
	require.Equal(t, notificationHeaderLimit, *requests[0].Limit)
	require.Empty(t, requests[0].Cursor)
	require.Empty(t, requests[0].State)

	updated, refresh := model.Update(message)
	require.NotNil(t, refresh)
	result := updated.(Model)
	view := ansi.Strip(result.View().Content)
	require.Contains(t, view, "NOTIFICACIONES p=2")
	require.Contains(t, view, "MESSAGE/notifica…00099")
	require.Contains(t, view, "agent-sender→agent-panel")
	require.Contains(t, view, "MESSAGE_AVAILABLE")
	require.Contains(t, view, "PENDING")
	require.NotContains(t, view, string(panelNotification().NotificationID),
		"notification ids are deterministically shortened")
	require.NotContains(t, view, "body")
	require.NotContains(t, view, "request_note")

	before := caller.callCount()
	updated, next := result.Update(refreshNotificationMsg{epoch: result.notificationEpoch})
	require.NotNil(t, next)
	require.Equal(t, before, caller.callCount(), "Update must not perform a header read")
	_ = next()
	require.Equal(t, before+1, caller.callCount(), "one refresh performs exactly one notification.list")
	_, obsolete := updated.Update(refreshNotificationMsg{epoch: result.notificationEpoch + 1})
	require.Nil(t, obsolete)
}

func TestApprovalHeaderShowsPendingCount(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{approvals: protocol.ApprovalList{
		Approvals: []protocol.ApprovalRequest{panelApproval()},
	}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.width, model.height = 120, 40
	model.connected = true
	model.global = channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive)
	model.panelReady = true

	command := model.fetchApprovalHeader()
	require.Zero(t, caller.callCount(), "model state access must not perform I/O")
	message := command().(approvalHeaderSnapshotMsg)
	require.NoError(t, message.err)
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodApprovalList}, caller.methods())
	requests := caller.approvalListRequests()
	require.Equal(t, []protocol.ListApprovalsParams{{State: protocol.ApprovalStatePending}}, requests)

	updated, refresh := model.Update(message)
	require.NotNil(t, refresh)
	result := updated.(Model)
	require.Contains(t, ansi.Strip(result.View().Content), "APPROVALS p=1")

	before := caller.callCount()
	updated, next := result.Update(refreshApprovalHeaderMsg{epoch: result.approvalHeaderEpoch})
	require.NotNil(t, next)
	require.Equal(t, before, caller.callCount(), "Update must not read the approval header")
	_ = next()
	require.Equal(t, before+1, caller.callCount())
	_, obsolete := updated.Update(refreshApprovalHeaderMsg{epoch: result.approvalHeaderEpoch + 1})
	require.Nil(t, obsolete)
}

func TestMessagePanelShowsRouteAndAcknowledgementMetadata(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{messages: protocol.DeliveryListResult{
		Deliveries: []protocol.DeliveryListItem{panelMessage()},
	}}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.clock = panelClock{now: panelTestTime}
	model.connected = true
	model.activeTab = messagesTab

	updated, command := model.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	require.NotNil(t, command)
	require.Zero(t, caller.callCount())
	message := command().(panelSnapshotMsg)
	require.NoError(t, message.err)
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodDeliveryList}, caller.methods())
	updated, refresh := updated.Update(message)
	require.NotNil(t, refresh)
	view := ansi.Strip(updated.(Model).View().Content)
	require.Contains(t, view, "thread="+string(panelMessage().ThreadRef))
	require.Contains(t, view, "ACK=ACKNOWLEDGED")
	require.NotContains(t, view, "subject")
	require.NotContains(t, view, "body")
}

func TestNotificationHeaderSanitizesMetadata(t *testing.T) {
	t.Parallel()

	item := panelNotification()
	item.SenderAgent = protocol.AgentID("agent-\x1b[31mred")
	item.RecipientAgents = []protocol.AgentID{protocol.AgentID("agent-\x1b[2Jrecipient")}
	model := Model{
		notificationReady: true,
		notificationAt:    panelTestTime,
		notifications: protocol.NotificationListResult{
			Notifications: []protocol.NotificationListItem{item}, PendingCount: 1,
		},
	}
	header := model.notificationHeader()
	require.NotContains(t, header, "\x1b")
	require.Contains(t, header, "agent-red→agent-recipient")
}

func TestOldDaemonLeavesOtherPanelViewsAvailable(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{notificationErr: errors.New("control api notification.list: Method not found")}
	model := NewModel(t.Context(), nil, panelTestNodeID)
	model.client = caller
	model.clock = panelClock{now: panelTestTime}
	model.width, model.height = 120, 40
	model.connected, model.panelReady = true, true
	model.activeTab = messagesTab
	model.global = channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive)
	model.messages = protocol.DeliveryListResult{Deliveries: []protocol.DeliveryListItem{panelMessage()}}

	message := model.fetchNotificationHeader()().(notificationSnapshotMsg)
	require.Error(t, message.err)
	updated, refresh := model.Update(message)
	require.NotNil(t, refresh, "an old daemon is retried without stopping the panel")
	view := ansi.Strip(updated.(Model).View().Content)
	require.Contains(t, view, "NOTIFICACIONES no disponible")
	require.Contains(t, view, string(panelMessage().MessageID), "the active tab remains usable")
	require.NotContains(t, view, "Method not found", "daemon errors are not rendered as untrusted text")
	require.Equal(t, []protocol.ControlMethod{protocol.ControlMethodNotificationList}, caller.methods())
}

func TestSanitizeRemovesControlsAndPreservesGraphemes(t *testing.T) {
	t.Parallel()

	raw := "\x1b[31mred\x1b[0m\x00\x07\u0085界e\u0301extra"
	got := Sanitize(raw, 5)
	require.Equal(t, "red界", got)
	require.LessOrEqual(t, ansi.StringWidth(got), 5)
	for _, r := range got {
		require.False(t, unicode.IsControl(r))
	}

	combined := Sanitize("e\u0301x", 1)
	require.Equal(t, "e\u0301", combined, "the combining mark stays with its base grapheme")
}

func TestPanelViewsMatchGoldens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		width     int
		height    int
		connected bool
		mode      protocol.ChannelMode
		tab       int
	}{
		{name: "disconnected_80x24", width: 80, height: 24},
		{name: "disconnected_messages_120x40", width: 120, height: 40, tab: messagesTab},
		{name: "disconnected_decisions_80x24", width: 80, height: 24, tab: decisionsTab},
		{name: "disconnected_model_120x40", width: 120, height: 40, tab: modelTab},
		{name: "active_agents_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive},
		{name: "empty_agents_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive},
		{name: "paused_messages_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModePaused, tab: 1},
		{name: "empty_messages_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive, tab: 1},
		{name: "draining_links_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeDraining, tab: 3},
		{name: "approvals_data_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive, tab: approvalsTab},
		{name: "empty_approvals_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive, tab: approvalsTab},
		{name: "decisions_data_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive, tab: 7},
		{name: "empty_decisions_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive, tab: 7},
		{name: "off_model_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeOff, tab: 10},
		{name: "empty_model_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive, tab: modelTab},
		{name: "model_data_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive, tab: 10},
		{name: "notifications_data_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive},
		{name: "notifications_empty_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive},
		{name: "notifications_old_daemon_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeActive, tab: messagesTab},
		{name: "narrow_72x20", width: 72, height: 20},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			model := NewModel(t.Context(), nil, panelTestNodeID)
			model.width = test.width
			model.height = test.height
			model.activeTab = test.tab
			model.connected = test.connected
			if test.connected {
				model.global = channelState(protocol.ScopeTypeGlobal, "*", test.mode)
				model.node = channelState(protocol.ScopeTypeNode, string(panelTestNodeID), test.mode)
				model.hasNode = true
				model.approvalHeaderReady = true
				model.approvalHeader = protocol.ApprovalList{Approvals: []protocol.ApprovalRequest{}}
				if isReadableTab(model.activeTab) {
					model.panelReady = true
					model.panelAt = panelTestTime
				}
				switch test.name {
				case "active_agents_80x24":
					model.agents = protocol.AgentListResult{Agents: []protocol.AgentListItem{panelAgent()}}
				case "paused_messages_120x40":
					model.messages = protocol.DeliveryListResult{Deliveries: []protocol.DeliveryListItem{panelMessage()}}
				case "approvals_data_120x40":
					model.approvals = protocol.ApprovalList{Approvals: []protocol.ApprovalRequest{panelApproval()}}
					model.approvalHeader = model.approvals
				case "decisions_data_120x40":
					model.decisions = protocol.DecisionListResult{Decisions: []protocol.DecisionListItem{panelDecision()}}
				case "off_model_120x40", "empty_model_80x24":
					model.modelStatus = unavailablePanelModel()
				case "model_data_120x40":
					model.modelStatus = knownPanelModel()
				case "notifications_data_120x40":
					model.notificationReady = true
					model.notificationAt = panelTestTime
					model.notifications = protocol.NotificationListResult{
						Notifications: []protocol.NotificationListItem{panelNotification()},
						PendingCount:  2,
					}
				case "notifications_empty_120x40":
					model.notificationReady = true
					model.notificationAt = panelTestTime
					model.notifications = protocol.NotificationListResult{
						Notifications: []protocol.NotificationListItem{},
					}
				case "notifications_old_daemon_120x40":
					model.notificationReady = true
					model.notificationErr = errors.New("control api notification.list: Method not found")
					model.messages = protocol.DeliveryListResult{Deliveries: []protocol.DeliveryListItem{panelMessage()}}
				}
			}
			got := ansi.Strip(model.View().Content) + "\n"
			requirePanelGolden(t, test.name+".golden", got)
		})
	}
}

const (
	readPanelAgentID protocol.AgentID    = "agent-panel"
	panelTestNodeID  protocol.NodeID     = "node-a"
	panelApprovalID  protocol.ApprovalID = "apr_01JB0000000000000000000055"
)

var panelTestTime = time.Date(2026, time.October, 30, 19, 0, 0, 0, time.UTC)

type panelClock struct{ now time.Time }

func (c panelClock) Now() time.Time { return c.now }

func panelAgent() protocol.AgentListItem {
	return protocol.AgentListItem{
		AgentID: readPanelAgentID, NodeID: panelTestNodeID, Harness: protocol.HarnessCodex,
		HarnessVersion: "1.2.3", Mode: protocol.AgentRunModeRun,
		SessionState: protocol.SessionStateConnected, Deliverable: true,
		Keyboard: &protocol.AgentKeyboardState{
			DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
			InputGuardVersion: 7, ObservedAt: panelTestTime,
		},
		Phase: protocol.AgentPhaseStatus{Value: protocol.AgentPhaseUnknown, Source: protocol.AgentPhaseSourceNone},
		Channel: protocol.AgentEffectiveChannel{
			TX: true, RX: true, AutoDelivery: true, AutoContinue: true, Mode: protocol.AgentModeFull,
		},
		Queue: protocol.AgentQueueCounts{Queued: 2, Delivering: 1, Uncertain: 1},
	}
}

func panelMessage() protocol.DeliveryListItem {
	return protocol.DeliveryListItem{
		MessageID: "01JB0000000000000000000011", FromAgent: "agent-sender",
		ToAgent: readPanelAgentID, ThreadRef: "thr_01JB0000000000000000000000",
		State: protocol.DeliveryQueueStateRetryWait, Priority: protocol.PriorityHigh,
		CreatedAt: panelTestTime.Add(-3 * time.Minute), ExpiresAt: panelTestTime.Add(time.Hour),
		Attempts: 2, LastReason: "DISCONNECTED", AckStatus: protocol.AckStatusAcknowledged,
	}
}

func panelApproval() protocol.ApprovalRequest {
	return protocol.ApprovalRequest{
		ApprovalID: panelApprovalID,
		Source:     protocol.ApprovalSourceOrchestrator,
		ObjectType: protocol.ApprovalObjectTypeMessage,
		ObjectID:   string(panelMessage().MessageID),
		AgentID:    readPanelAgentID,
		Options: []protocol.ApprovalOption{
			approvalDeliverNowOption, approvalRejectOption,
		},
		RequestNote: "recipient not deliverable",
		State:       protocol.ApprovalStatePending,
		CreatedAt:   panelTestTime.Add(-4 * time.Minute),
	}
}

func panelNotification() protocol.NotificationListItem {
	return protocol.NotificationListItem{
		NotificationID: "notification-01JB0000000000000000000099",
		Class:          protocol.NotificationClassMessage,
		ObjectID:       "01JB0000000000000000000099",
		SenderAgent:    "agent-sender",
		RecipientAgents: []protocol.AgentID{
			readPanelAgentID,
		},
		Priority:      protocol.PriorityHigh,
		Reason:        protocol.NotificationReasonMessageAvailable,
		State:         protocol.NotificationStatePending,
		Adapter:       protocol.NotificationAdapterTMUX,
		CreatedAt:     panelTestTime.Add(-2 * time.Minute),
		UpdatedAt:     panelTestTime.Add(-2 * time.Minute),
		LastAttemptAt: nil,
		NextAttemptAt: nil,
		RenderedAt:    nil,
	}
}

func panelDecision() protocol.DecisionListItem {
	return protocol.DecisionListItem{
		DecisionID: "chd_01JB0000000000000000000022", Type: protocol.DecisionTypeChannel,
		Stage: protocol.DecisionStageDelivery, Outcome: protocol.DecisionOutcomeHold,
		AgentID: readPanelAgentID, MessageID: "01JB0000000000000000000011",
		CreatedAt: panelTestTime, ReasonCodes: []protocol.ReasonCode{"CONTROL_CHANGED"},
	}
}

func unavailablePanelModel() protocol.ModelStatusResult {
	unavailable := protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityUnavailable}
	return protocol.ModelStatusResult{Deployment: protocol.ModelDeploymentMetadata{
		BaseRevision: unavailable, AdapterRevision: unavailable,
		DatasetVersion: unavailable, ABVariant: unavailable,
	}}
}

func knownPanelModel() protocol.ModelStatusResult {
	known := func(value string) protocol.ModelMetadata {
		return protocol.ModelMetadata{
			Availability: protocol.MetadataAvailabilityKnown,
			Value:        protocol.ModelMetadataValue(value),
		}
	}
	return protocol.ModelStatusResult{
		ActiveProfile: &protocol.ActiveModelProfile{
			Profile:    protocol.InferenceProfileAnalystLoRA,
			Source:     protocol.ModelProfileSourceGlimmerCall,
			ObservedAt: panelTestTime,
		},
		LastCall: &protocol.ModelLastCall{
			CallID: "call-panel", Status: protocol.GlimmerCallStatusOK,
			LatencyMS: 87, ObservedAt: panelTestTime.Add(-time.Minute),
		},
		Deployment: protocol.ModelDeploymentMetadata{
			BaseRevision: known("base-rev-1"), AdapterRevision: known("adapter-rev-2"),
			DatasetVersion: known("dataset-v3"),
			ABVariant:      protocol.ModelMetadata{Availability: protocol.MetadataAvailabilityUnavailable},
		},
	}
}

func channelState(scope protocol.ScopeType, key string, mode protocol.ChannelMode) protocol.ChannelState {
	return protocol.ChannelState{
		ScopeType: scope, ScopeKey: key, Mode: mode, EffectiveMode: mode,
	}
}

func requirePanelGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updatePanelGoldens {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), got)
}
