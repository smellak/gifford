package hub

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

type paneTransportStub struct {
	server tmuxattach.Server
	rows   []tmuxattach.PaneObservation
}

func (s *paneTransportStub) ListPanes(
	_ context.Context,
	server tmuxattach.Server,
) ([]tmuxattach.PaneObservation, error) {
	s.server = server
	return append([]tmuxattach.PaneObservation(nil), s.rows...), nil
}

func TestTmuxPanelListerResolvesOwnerAndPreservesCanonicalPaneRows(t *testing.T) {
	t.Parallel()
	transport := &paneTransportStub{rows: []tmuxattach.PaneObservation{
		{PaneID: "%0", PanePID: 100}, {PaneID: "%12", PanePID: 200},
	}}
	lister := tmuxPanelLister{
		transport: transport,
		lookupUID: func(owner string) (uint32, error) {
			require.Equal(t, "agent_owner", owner)
			return 1001, nil
		},
	}
	rows, err := lister.ListPanes(t.Context(), "agent_owner", "/run/user/1001/tmux.sock")
	require.NoError(t, err)
	require.Equal(t, tmuxattach.Server{
		OwnerUser: "agent_owner", OwnerUID: 1001, Socket: "/run/user/1001/tmux.sock",
	}, transport.server)
	require.Equal(t, []protocol.TmuxPaneID{"%0", "%12"},
		[]protocol.TmuxPaneID{rows[0].PaneID, rows[1].PaneID})
	require.Equal(t, []int64{100, 200}, []int64{rows[0].PanePID, rows[1].PanePID})
}

func TestTmuxPanelListerFailsClosedWhenOwnerCannotBeResolved(t *testing.T) {
	t.Parallel()
	transport := &paneTransportStub{}
	lister := tmuxPanelLister{
		transport: transport,
		lookupUID: func(string) (uint32, error) { return 0, errors.New("missing owner") },
	}
	rows, err := lister.ListPanes(t.Context(), "missing", "/run/tmux.sock")
	require.Nil(t, rows)
	require.ErrorContains(t, err, "lookup tmux owner")
	require.Empty(t, transport.server)
}

func TestTmuxReservedUIDsIncludeNodeAndOptionalBridge(t *testing.T) {
	t.Parallel()
	require.Equal(t, []uint32{1001}, tmuxReservedUIDs(1001, nil))
	require.Equal(t, []uint32{1001, 998}, tmuxReservedUIDs(1001,
		&config.NodeSocket{BridgeUID: 998}))
	require.Equal(t, []uint32{1001}, tmuxReservedUIDs(1001,
		&config.NodeSocket{BridgeUID: 1001}), "an identical bridge uid is reserved once")
}

func TestLogTmuxReconcileReportUsesOnlyRefusalIdentityAndCode(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logTmuxReconcileReport(&output, tmuxattach.ReconcileReport{
		StatementError: errors.New("invalid statements"),
		Refused: []tmuxinstance.Refusal{{
			AgentID: "agent-one", Code: tmuxinstance.RefusalPaneMissing,
		}},
	})
	require.Equal(t, "gifford hub: load tmux instance statements: invalid statements\n"+
		"gifford hub: tmux instance statement refused: agent=agent-one code=pane_missing\n",
		output.String())
}
