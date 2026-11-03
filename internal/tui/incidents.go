package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

func (m Model) incidentLines() []string {
	return incidentViewLines(m.incidents, m.panelAt)
}

func incidentViewLines(list protocol.IncidentListResult, now time.Time) []string {
	if len(list.Incidents) == 0 {
		return []string{"no hay incidencias registradas"}
	}
	lines := make([]string, 0, len(list.Incidents)*5+1)
	for _, incident := range list.Incidents {
		ownerID := "—"
		if incident.OwnerID != nil {
			ownerID = Sanitize(string(*incident.OwnerID), 24)
		}
		owner := emptyLabel(Sanitize(string(incident.OwnerKind), 12)) + "/" + ownerID
		lines = append(lines,
			fmt.Sprintf("%s  %s  estado=%s  dueño=%s  fix_area=%s",
				Sanitize(string(incident.Kind), 28), shortID(incident.IncidentID),
				Sanitize(string(incident.State), 16), owner,
				Sanitize(string(incident.FixArea), 16)),
			fmt.Sprintf("  agente=%s  mensaje=%s  motivo=%s",
				emptyLabel(Sanitize(string(incident.AgentID), 20)),
				emptyLabel(Sanitize(string(incident.MessageID), 30)),
				emptyLabel(Sanitize(incident.Reason, 36))),
			"  "+strings.Join([]string{
				"abierta=" + incidentAge(now, &incident.OpenedAt),
				"actualizada=" + incidentAge(now, &incident.UpdatedAt),
				"escalada=" + incidentAge(now, incident.EscalatedAt),
				"próximo_aviso=" + incidentAge(now, incident.NextNoticeAt),
				"resuelta=" + incidentAge(now, incident.ResolvedAt),
			}, "  "),
		)
		lines = append(lines, incidentOperatorGuidance(incident)...)
	}
	return appendPageHint(lines, list.NextCursor)
}

func incidentOperatorGuidance(incident protocol.IncidentListItem) []string {
	if string(incident.Kind) != "DELIVERY_UNCERTAIN" {
		return nil
	}
	return []string{
		"  operador=mantener pendiente y mirar panel; rechazar=cierra sin reinyectar",
		"  aprobar=reinyecta; sólo con panel limpio y mensaje sin leer",
	}
}

func incidentAge(now time.Time, value *time.Time) string {
	if value == nil {
		return "—"
	}
	if value.After(now) {
		return "en " + formatAge(*value, now)
	}
	return formatAge(now, *value)
}
