package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	channelRefreshInterval            = 100 * time.Millisecond
	panelRefreshInterval              = time.Second
	notificationRefreshInterval       = time.Second
	notificationHeaderLimit     int64 = 1
	initialRetryDelay                 = 100 * time.Millisecond
	maximumRetryDelay                 = time.Second

	agentsTab    = 0
	messagesTab  = 1
	decisionsTab = 7
	modelTab     = 10
)

type controlCaller interface {
	Call(context.Context, protocol.ControlMethod, any, any) error
}

// Model is the single root model for gifford panel. Every data field is a
// presentation snapshot returned by the daemon, never independent control
// state.
type Model struct {
	ctx        context.Context
	client     controlCaller
	clock      clock.Clock
	nodeID     protocol.NodeID
	width      int
	height     int
	activeTab  int
	help       bool
	connected  bool
	global     protocol.ChannelState
	node       protocol.ChannelState
	hasNode    bool
	retryDelay time.Duration

	panelReady bool
	panelErr   error
	panelAt    time.Time
	panelEpoch uint64
	// panelAfterReconnect says every outstanding panel response and timer
	// belongs to the disconnected generation. The next channel connection
	// retires that generation and starts one fresh active-panel read.
	panelAfterReconnect bool

	agents      protocol.AgentListResult
	agentCursor protocol.PageCursor

	messages      protocol.MessageListResult
	messageCursor protocol.PageCursor

	decisions      protocol.DecisionListResult
	decisionCursor protocol.PageCursor
	decisionIndex  int
	detailID       protocol.DecisionID
	detail         *protocol.DecisionGetResult

	modelStatus protocol.ModelStatusResult

	notifications     protocol.NotificationListResult
	notificationReady bool
	notificationErr   error
	notificationAt    time.Time
	notificationEpoch uint64
}

// NewModel returns the panel's root model. The client and context are immutable
// dependencies; all presentation state changes happen in Update.
func NewModel(ctx context.Context, client *Client, nodeID protocol.NodeID) Model {
	if ctx == nil {
		ctx = context.Background()
	}
	return Model{
		ctx: ctx, client: client, clock: clock.System{}, nodeID: nodeID,
		retryDelay: initialRetryDelay,
	}
}

// Init starts the channel read. The first panel read starts after Bubble Tea
// reports the window size, so its page limit matches the visible rows.
func (m Model) Init() tea.Cmd {
	return fetchChannelCmd(m.ctx, m.client, m.nodeID)
}

// Update applies messages and returns commands for all I/O and timers.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		firstSize := m.width == 0 && m.height == 0
		oldLimit := m.visibleRows()
		m.width = msg.Width
		m.height = msg.Height
		if (firstSize || m.visibleRows() != oldLimit) && isReadableTab(m.activeTab) {
			m.resetActiveRead()
			return m, m.fetchActivePanel()
		}
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "tab", "right":
			m.activeTab = (m.activeTab + 1) % len(panelTabs)
			m.resetActiveRead()
			return m, m.fetchActivePanel()
		case "shift+tab", "left":
			m.activeTab = (m.activeTab - 1 + len(panelTabs)) % len(panelTabs)
			m.resetActiveRead()
			return m, m.fetchActivePanel()
		case "?":
			m.help = !m.help
		case "n", "pgdown":
			if next := m.activeNextCursor(); next != "" {
				m.setActiveCursor(next)
				m.invalidatePanelView()
				m.panelReady = false
				m.panelErr = nil
				m.detail = nil
				m.detailID = ""
				return m, m.fetchActivePanel()
			}
		case "up":
			if m.activeTab == decisionsTab && m.detailID == "" && m.decisionIndex > 0 {
				m.decisionIndex--
			}
		case "down":
			if m.activeTab == decisionsTab && m.detailID == "" &&
				m.decisionIndex+1 < len(m.decisions.Decisions) {
				m.decisionIndex++
			}
		case "enter":
			if m.activeTab == decisionsTab && m.detailID == "" &&
				m.decisionIndex < len(m.decisions.Decisions) {
				m.detailID = m.decisions.Decisions[m.decisionIndex].DecisionID
				m.invalidatePanelView()
				m.detail = nil
				m.panelReady = false
				m.panelErr = nil
				return m, m.fetchActivePanel()
			}
		case "esc":
			if m.activeTab == decisionsTab && m.detailID != "" {
				m.detailID = ""
				m.invalidatePanelView()
				m.detail = nil
				m.panelReady = false
				m.panelErr = nil
				return m, m.fetchActivePanel()
			}
		}
	case refreshChannelMsg:
		return m, fetchChannelCmd(m.ctx, m.client, m.nodeID)
	case channelSnapshotMsg:
		if msg.err != nil {
			m.connected = false
			m.global = protocol.ChannelState{}
			m.node = protocol.ChannelState{}
			m.hasNode = false
			m.modelStatus = protocol.ModelStatusResult{}
			m.panelReady = false
			m.panelErr = nil
			m.panelAt = time.Time{}
			m.panelAfterReconnect = true
			m.notifications = protocol.NotificationListResult{}
			m.notificationReady = false
			m.notificationErr = nil
			m.notificationAt = time.Time{}
			m.notificationEpoch++
			m.invalidatePanelView()
			delay := m.retryDelay
			if delay <= 0 {
				delay = initialRetryDelay
			}
			m.retryDelay = min(delay*2, maximumRetryDelay)
			return m, scheduleChannelRefresh(delay)
		}
		wasConnected := m.connected
		refreshPanel := m.panelAfterReconnect
		m.panelAfterReconnect = false
		m.connected = true
		m.global = msg.global
		m.node = msg.node
		m.hasNode = msg.hasNode
		m.retryDelay = initialRetryDelay
		commands := []tea.Cmd{scheduleChannelRefresh(channelRefreshInterval)}
		if refreshPanel && m.width > 0 && m.height > 0 {
			m.modelStatus = protocol.ModelStatusResult{}
			m.invalidatePanelView()
			commands = append(commands, m.fetchActivePanel())
		}
		if !wasConnected {
			commands = append(commands, m.fetchNotificationHeader())
		}
		return m, batchCommands(commands...)
	case refreshPanelMsg:
		if msg.epoch == m.panelEpoch && msg.tab == m.activeTab && isReadableTab(m.activeTab) {
			return m, m.fetchActivePanel()
		}
	case panelSnapshotMsg:
		if msg.epoch != m.panelEpoch || msg.tab != m.activeTab || msg.cursor != m.activeCursor() ||
			msg.limit != m.visibleRows() || msg.detailID != m.detailID {
			return m, nil
		}
		m.clearActiveData()
		m.panelReady = true
		m.panelErr = msg.err
		m.panelAt = msg.at
		if msg.err == nil {
			switch msg.tab {
			case agentsTab:
				m.agents = msg.agents
			case messagesTab:
				m.messages = msg.messages
			case decisionsTab:
				if msg.detail != nil {
					m.detail = msg.detail
				} else {
					m.decisions = msg.decisions
					if m.decisionIndex >= len(m.decisions.Decisions) {
						m.decisionIndex = max(len(m.decisions.Decisions)-1, 0)
					}
				}
			case modelTab:
				m.modelStatus = msg.modelStatus
			}
		}
		return m, schedulePanelRefresh(msg.tab, msg.epoch, panelRefreshInterval)
	case refreshNotificationMsg:
		if msg.epoch == m.notificationEpoch && m.connected {
			return m, m.fetchNotificationHeader()
		}
	case notificationSnapshotMsg:
		if msg.epoch != m.notificationEpoch || !m.connected {
			return m, nil
		}
		m.notifications = protocol.NotificationListResult{}
		m.notificationReady = true
		m.notificationErr = msg.err
		m.notificationAt = msg.at
		if msg.err == nil {
			m.notifications = msg.notifications
		}
		return m, scheduleNotificationRefresh(msg.epoch, notificationRefreshInterval)
	}
	return m, nil
}

// View renders a full-window panel. It is pure and performs no reads.
func (m Model) View() tea.View {
	view := tea.NewView(m.render())
	view.AltScreen = true
	view.WindowTitle = "gifford panel"
	return view
}

func (m Model) fetchActivePanel() tea.Cmd {
	if !isReadableTab(m.activeTab) {
		return nil
	}
	return fetchPanelCmd(
		m.ctx, m.client, m.clock, m.activeTab, m.activeCursor(),
		m.visibleRows(), m.detailID, m.panelEpoch,
	)
}

func (m Model) visibleRows() int64 {
	rows := m.height - 11
	switch m.activeTab {
	case agentsTab:
		rows /= 2
	case decisionsTab:
		rows /= 3
	}
	if rows < 1 {
		rows = 1
	}
	if rows > int(protocol.MaxListLimit) {
		rows = int(protocol.MaxListLimit)
	}
	return int64(rows)
}

func (m Model) fetchNotificationHeader() tea.Cmd {
	return fetchNotificationHeaderCmd(
		m.ctx, m.client, m.clock, m.notificationEpoch,
	)
}

func isReadableTab(tab int) bool {
	return tab == agentsTab || tab == messagesTab || tab == decisionsTab || tab == modelTab
}

func (m Model) activeCursor() protocol.PageCursor {
	switch m.activeTab {
	case agentsTab:
		return m.agentCursor
	case messagesTab:
		return m.messageCursor
	case decisionsTab:
		return m.decisionCursor
	default:
		return ""
	}
}

func (m *Model) setActiveCursor(cursor protocol.PageCursor) {
	switch m.activeTab {
	case agentsTab:
		m.agentCursor = cursor
	case messagesTab:
		m.messageCursor = cursor
	case decisionsTab:
		m.decisionCursor = cursor
	}
}

func (m Model) activeNextCursor() protocol.PageCursor {
	if !m.panelReady || m.panelErr != nil || m.detailID != "" {
		return ""
	}
	switch m.activeTab {
	case agentsTab:
		return m.agents.NextCursor
	case messagesTab:
		return m.messages.NextCursor
	case decisionsTab:
		return m.decisions.NextCursor
	default:
		return ""
	}
}

func (m *Model) resetActiveRead() {
	m.invalidatePanelView()
	m.panelReady = false
	m.panelErr = nil
	m.panelAt = time.Time{}
	m.setActiveCursor("")
	m.decisionIndex = 0
	m.detailID = ""
	m.detail = nil
	m.clearActiveData()
}

// invalidatePanelView retires every in-flight response and timer for the
// previous query, including an earlier visit with the same tab and cursor.
func (m *Model) invalidatePanelView() {
	m.panelEpoch++
}

func (m *Model) clearActiveData() {
	switch m.activeTab {
	case agentsTab:
		m.agents = protocol.AgentListResult{}
	case messagesTab:
		m.messages = protocol.MessageListResult{}
	case decisionsTab:
		if m.detailID != "" {
			m.detail = nil
		} else {
			m.decisions = protocol.DecisionListResult{}
		}
	case modelTab:
		m.modelStatus = protocol.ModelStatusResult{}
	}
}

type refreshChannelMsg struct{}

type channelSnapshotMsg struct {
	global  protocol.ChannelState
	node    protocol.ChannelState
	hasNode bool
	err     error
}

func fetchChannelCmd(ctx context.Context, client controlCaller, nodeID protocol.NodeID) tea.Cmd {
	return func() tea.Msg {
		if client == nil {
			return channelSnapshotMsg{err: context.Canceled}
		}
		var global protocol.ChannelState
		if err := client.Call(ctx, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &global); err != nil {
			return channelSnapshotMsg{err: err}
		}
		msg := channelSnapshotMsg{global: global}
		if nodeID == "" {
			return msg
		}
		if err := client.Call(ctx, protocol.ControlMethodChannelGetNode, protocol.NodeParams{NodeID: nodeID}, &msg.node); err != nil {
			return channelSnapshotMsg{err: err}
		}
		msg.hasNode = true
		return msg
	}
}

type refreshPanelMsg struct {
	tab   int
	epoch uint64
}

type panelSnapshotMsg struct {
	tab      int
	epoch    uint64
	cursor   protocol.PageCursor
	limit    int64
	detailID protocol.DecisionID
	at       time.Time
	err      error

	agents      protocol.AgentListResult
	messages    protocol.MessageListResult
	decisions   protocol.DecisionListResult
	detail      *protocol.DecisionGetResult
	modelStatus protocol.ModelStatusResult
}

type refreshNotificationMsg struct{ epoch uint64 }

type notificationSnapshotMsg struct {
	epoch         uint64
	at            time.Time
	err           error
	notifications protocol.NotificationListResult
}

func fetchPanelCmd(
	ctx context.Context,
	client controlCaller,
	clk clock.Clock,
	tab int,
	cursor protocol.PageCursor,
	limit int64,
	detailID protocol.DecisionID,
	epoch uint64,
) tea.Cmd {
	if !isReadableTab(tab) {
		return nil
	}
	return func() tea.Msg {
		msg := panelSnapshotMsg{
			tab: tab, epoch: epoch, cursor: cursor, limit: limit, detailID: detailID,
		}
		if client == nil {
			msg.err = context.Canceled
			return msg
		}
		switch tab {
		case agentsTab:
			msg.err = client.Call(ctx, protocol.ControlMethodAgentList,
				protocol.AgentListParams{Cursor: cursor, Limit: &limit}, &msg.agents)
		case messagesTab:
			msg.err = client.Call(ctx, protocol.ControlMethodMessageList,
				protocol.MessageListParams{Cursor: cursor, Limit: &limit}, &msg.messages)
		case decisionsTab:
			if detailID != "" {
				var detail protocol.DecisionGetResult
				msg.err = client.Call(ctx, protocol.ControlMethodDecisionGet,
					protocol.DecisionGetParams{DecisionID: detailID}, &detail)
				if msg.err == nil {
					msg.detail = &detail
				}
			} else {
				msg.err = client.Call(ctx, protocol.ControlMethodDecisionList,
					protocol.DecisionListParams{Cursor: cursor, Limit: &limit}, &msg.decisions)
			}
		case modelTab:
			msg.err = client.Call(ctx, protocol.ControlMethodModelStatus,
				protocol.EmptyParams{}, &msg.modelStatus)
		}
		if clk != nil {
			msg.at = clk.Now().UTC()
		}
		return msg
	}
}

func scheduleChannelRefresh(delay time.Duration) tea.Cmd {
	return tea.Tick(delay, func(time.Time) tea.Msg { return refreshChannelMsg{} })
}

func schedulePanelRefresh(tab int, epoch uint64, delay time.Duration) tea.Cmd {
	return tea.Tick(delay, func(time.Time) tea.Msg { return refreshPanelMsg{tab: tab, epoch: epoch} })
}

func fetchNotificationHeaderCmd(
	ctx context.Context,
	client controlCaller,
	clk clock.Clock,
	epoch uint64,
) tea.Cmd {
	return func() tea.Msg {
		msg := notificationSnapshotMsg{epoch: epoch}
		if client == nil {
			msg.err = context.Canceled
			return msg
		}
		limit := notificationHeaderLimit
		msg.err = client.Call(ctx, protocol.ControlMethodNotificationList,
			protocol.NotificationListParams{Limit: &limit}, &msg.notifications)
		if clk != nil {
			msg.at = clk.Now().UTC()
		}
		return msg
	}
}

func scheduleNotificationRefresh(epoch uint64, delay time.Duration) tea.Cmd {
	return tea.Tick(delay, func(time.Time) tea.Msg { return refreshNotificationMsg{epoch: epoch} })
}

func batchCommands(commands ...tea.Cmd) tea.Cmd {
	filtered := commands[:0]
	for _, command := range commands {
		if command != nil {
			filtered = append(filtered, command)
		}
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	return tea.Batch(filtered...)
}
