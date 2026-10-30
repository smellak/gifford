package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	channelRefreshInterval = 100 * time.Millisecond
	initialRetryDelay      = 100 * time.Millisecond
	maximumRetryDelay      = time.Second
)

type controlCaller interface {
	Call(context.Context, protocol.ControlMethod, any, any) error
}

// Model is the single root model for gifford panel. Its channel fields are a
// presentation snapshot returned by the daemon, never an independent control
// state.
type Model struct {
	ctx        context.Context
	client     controlCaller
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
}

// NewModel returns the panel's root model. The client and context are immutable
// dependencies; all presentation state changes happen in Update.
func NewModel(ctx context.Context, client *Client, nodeID protocol.NodeID) Model {
	if ctx == nil {
		ctx = context.Background()
	}
	return Model{
		ctx: ctx, client: client, nodeID: nodeID, retryDelay: initialRetryDelay,
	}
}

// Init starts the first bounded read in a command, outside Update.
func (m Model) Init() tea.Cmd {
	return fetchChannelCmd(m.ctx, m.client, m.nodeID)
}

// Update applies messages and returns commands for all I/O and timers.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "tab", "right":
			m.activeTab = (m.activeTab + 1) % len(panelTabs)
		case "shift+tab", "left":
			m.activeTab = (m.activeTab - 1 + len(panelTabs)) % len(panelTabs)
		case "?":
			m.help = !m.help
		}
	case refreshChannelMsg:
		return m, fetchChannelCmd(m.ctx, m.client, m.nodeID)
	case channelSnapshotMsg:
		if msg.err != nil {
			m.connected = false
			m.global = protocol.ChannelState{}
			m.node = protocol.ChannelState{}
			m.hasNode = false
			delay := m.retryDelay
			if delay <= 0 {
				delay = initialRetryDelay
			}
			m.retryDelay = min(delay*2, maximumRetryDelay)
			return m, scheduleChannelRefresh(delay)
		}
		m.connected = true
		m.global = msg.global
		m.node = msg.node
		m.hasNode = msg.hasNode
		m.retryDelay = initialRetryDelay
		return m, scheduleChannelRefresh(channelRefreshInterval)
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

func scheduleChannelRefresh(delay time.Duration) tea.Cmd {
	return tea.Tick(delay, func(time.Time) tea.Msg { return refreshChannelMsg{} })
}
