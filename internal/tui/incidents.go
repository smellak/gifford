package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// incidentViewItem is the presentation-only shape expected from the pending
// read contract. It keeps the renderer independent from storage and is
// replaced by the protocol projection when incident.list lands.
type incidentViewItem struct {
	Kind         string
	AgentID      protocol.AgentID
	MessageID    protocol.ULID
	OwnerKind    string
	OwnerID      protocol.AgentID
	FixArea      string
	State        string
	Reason       string
	EscalatedAt  *time.Time
	NextNoticeAt *time.Time
	OpenedAt     time.Time
	UpdatedAt    time.Time
	ResolvedAt   *time.Time
}

type incidentViewList struct {
	Incidents  []incidentViewItem
	NextCursor protocol.PageCursor
}

func incidentViewLines(list incidentViewList, now time.Time) []string {
	if len(list.Incidents) == 0 {
		return []string{"no hay incidencias registradas"}
	}
	lines := make([]string, 0, len(list.Incidents)*3+1)
	for _, incident := range list.Incidents {
		owner := emptyLabel(Sanitize(incident.OwnerKind, 12)) + "/" +
			emptyLabel(Sanitize(string(incident.OwnerID), 24))
		lines = append(lines,
			fmt.Sprintf("%s  estado=%s  dueño=%s  fix_area=%s",
				Sanitize(incident.Kind, 28), Sanitize(incident.State, 16), owner,
				Sanitize(incident.FixArea, 16)),
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
	}
	return appendPageHint(lines, list.NextCursor)
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
