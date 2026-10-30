package tui

import (
	"context"
	"encoding/json"
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
	mu     sync.Mutex
	calls  []protocol.ControlMethod
	global protocol.ChannelState
	node   protocol.ChannelState
	err    error
}

func (c *recordingCaller) Call(
	_ context.Context,
	method protocol.ControlMethod,
	_, result any,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, method)
	if c.err != nil {
		return c.err
	}
	var source any
	switch method {
	case protocol.ControlMethodChannelGetGlobal:
		source = c.global
	case protocol.ControlMethodChannelGetNode:
		source = c.node
	default:
		return ErrMethodNotAllowed
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, result)
}

func (c *recordingCaller) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func TestUpdateReturnsIOAsCommand(t *testing.T) {
	t.Parallel()

	caller := &recordingCaller{
		global: channelState(protocol.ScopeTypeGlobal, "*", protocol.ChannelModeActive),
		node:   channelState(protocol.ScopeTypeNode, "node-a", protocol.ChannelModeActive),
	}
	model := NewModel(t.Context(), nil, "node-a")
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
	model := NewModel(t.Context(), client, "node-a")
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

	model := NewModel(t.Context(), nil, "node-a")
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

func TestNavigationAndHelpUseSyntheticMessages(t *testing.T) {
	t.Parallel()

	model := NewModel(t.Context(), nil, "")
	model.width = 80
	model.height = 24
	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	require.Nil(t, command)
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
		if tab.missing {
			require.Contains(t, view, "falta el método de lectura (8.4b)")
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
	refresh := tick()
	updated, read := result.Update(refresh)
	snapshot := read().(channelSnapshotMsg)
	require.NoError(t, snapshot.err)
	updated, _ = updated.Update(snapshot)
	require.Equal(t, protocol.ChannelModePaused, updated.(Model).global.Mode)
	require.Less(t, time.Since(started), time.Second)
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
		{name: "active_agents_80x24", width: 80, height: 24, connected: true, mode: protocol.ChannelModeActive},
		{name: "paused_messages_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModePaused, tab: 1},
		{name: "draining_links_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeDraining, tab: 3},
		{name: "off_model_120x40", width: 120, height: 40, connected: true, mode: protocol.ChannelModeOff, tab: 10},
		{name: "narrow_72x20", width: 72, height: 20},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			model := NewModel(t.Context(), nil, "node-a")
			model.width = test.width
			model.height = test.height
			model.activeTab = test.tab
			model.connected = test.connected
			if test.connected {
				model.global = channelState(protocol.ScopeTypeGlobal, "*", test.mode)
				model.node = channelState(protocol.ScopeTypeNode, "node-a", test.mode)
				model.hasNode = true
			}
			got := ansi.Strip(model.View().Content) + "\n"
			requirePanelGolden(t, test.name+".golden", got)
		})
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
