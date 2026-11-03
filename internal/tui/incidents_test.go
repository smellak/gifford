package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	incidentTestOwnerKind  = "operator"
	incidentEscalatedLabel = "ESCALATED"
)

func TestIncidentViewShowsOwnerRoutingAndLifecycle(t *testing.T) {
	t.Parallel()

	list := panelIncidentList()

	view := strings.Join(incidentViewLines(list, panelTestTime), "\n")
	require.Contains(t, view, "DETECTOR_UNCERTAIN")
	require.Contains(t, view, "dueño=operator/operator-node")
	require.Contains(t, view, "fix_area=detector")
	require.Contains(t, view, "agente="+string(readPanelAgentID))
	require.Contains(t, view, "motivo=STATE_UNKNOWN")
	require.Contains(t, view, "próximo_aviso=en 15m")
	require.Contains(t, view, "DELIVERY_UNCERTAIN")
	require.Contains(t, view, "mensaje="+string(panelMessage().MessageID))
	require.Contains(t, view, "fix_area=delivery")
	require.Contains(t, view, "operador=mantener pendiente y mirar panel")
	require.Contains(t, view, "rechazar=cierra sin reinyectar")
	require.Contains(t, view, "aprobar=reinyecta; sólo con panel limpio y mensaje sin leer")
	require.Contains(t, view, "APPROVAL_PENDING")
	require.Contains(t, view, "estado=PENDING")
	require.Contains(t, view, "TMUX_INSTANCE_INVALID")
	require.Contains(t, view, "fix_area=instances")
	require.Contains(t, view, incidentEscalatedLabel)
	require.Contains(t, view, "dueño=operator/—")
	require.Contains(t, view, "fix_area=unknown")
	require.Contains(t, view, "escalada=30m")
	require.Contains(t, view, "resuelta=—")
	require.Contains(t, view, "n / avpág carga la página siguiente")
	require.NotContains(t, view, "subject")
	require.NotContains(t, view, "body")
}

func TestIncidentViewHasHonestEmptyState(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"no hay incidencias registradas"},
		incidentViewLines(protocol.IncidentListResult{}, panelTestTime))
}

func panelIncidentList() protocol.IncidentListResult {
	escalated := panelTestTime.Add(-30 * time.Minute)
	nextNotice := panelTestTime.Add(15 * time.Minute)
	ownerID := protocol.AgentID("operator-node")
	return protocol.IncidentListResult{Incidents: []protocol.IncidentListItem{
		{
			IncidentID: "incident-detector", Kind: "DETECTOR_UNCERTAIN", AgentID: readPanelAgentID,
			OwnerKind: incidentTestOwnerKind, OwnerID: &ownerID, FixArea: "detector",
			State: "UNCERTAIN", Reason: "STATE_UNKNOWN", NextNoticeAt: &nextNotice,
			OpenedAt: panelTestTime.Add(-10 * time.Minute), UpdatedAt: panelTestTime.Add(-2 * time.Minute),
		},
		{
			IncidentID: "incident-delivery", Kind: "DELIVERY_UNCERTAIN", AgentID: readPanelAgentID,
			MessageID: panelMessage().MessageID, OwnerKind: incidentTestOwnerKind, OwnerID: &ownerID,
			FixArea: "delivery", State: "UNCERTAIN", Reason: "PROCESS_RESTART",
			OpenedAt: panelTestTime.Add(-time.Hour), UpdatedAt: panelTestTime.Add(-time.Minute),
		},
		{
			IncidentID: "incident-approval", Kind: "APPROVAL_PENDING", AgentID: readPanelAgentID,
			OwnerKind: incidentTestOwnerKind, OwnerID: &ownerID, FixArea: "delivery",
			State: "PENDING", Reason: "AWAITING_OPERATOR",
			OpenedAt: panelTestTime.Add(-8 * time.Minute), UpdatedAt: panelTestTime.Add(-8 * time.Minute),
		},
		{
			IncidentID: "incident-instance", Kind: "TMUX_INSTANCE_INVALID", AgentID: readPanelAgentID,
			OwnerKind: incidentTestOwnerKind, OwnerID: &ownerID, FixArea: "instances",
			State: "INVALID", Reason: "PROGRAM_CHANGED",
			OpenedAt: panelTestTime.Add(-7 * time.Minute), UpdatedAt: panelTestTime.Add(-6 * time.Minute),
		},
		{
			IncidentID: "incident-escalated", Kind: incidentEscalatedLabel, AgentID: readPanelAgentID,
			OwnerKind: incidentTestOwnerKind, FixArea: "unknown",
			State: incidentEscalatedLabel, Reason: "FALLBACK_EXHAUSTED", EscalatedAt: &escalated,
			OpenedAt: escalated, UpdatedAt: escalated,
		},
	}, NextCursor: protocol.PageCursor("next-page")}
}
