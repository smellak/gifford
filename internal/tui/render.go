//nolint:misspell // User-facing panel copy is Spanish.
package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/x/ansi"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tui/styles"
)

const (
	minimumWidth  = 80
	minimumHeight = 24
)

type panelTab struct {
	name     string
	readable bool
}

var panelTabs = []panelTab{
	{name: "Agents", readable: true},
	{name: "Messages", readable: true},
	{name: "Artifacts"},
	{name: "Links"},
	{name: "Loops"},
	{name: "Approvals"},
	{name: "Nodes"},
	{name: "Decisions", readable: true},
	{name: "Events"},
	{name: "Capture"},
	{name: "Model", readable: true},
	{name: "Settings"},
}

// Sanitize removes terminal escapes and control characters, then truncates by
// displayed width without splitting wide runes or grapheme clusters.
func Sanitize(raw string, width int) string {
	if width <= 0 {
		return ""
	}
	stripped := ansi.Strip(raw)
	clean := strings.Map(func(r rune) rune {
		if r <= 0x1f || r >= 0x7f && r <= 0x9f {
			return -1
		}
		return r
	}, stripped)
	return ansi.Truncate(clean, width, "")
}

func (m Model) render() string {
	if m.width < minimumWidth || m.height < minimumHeight {
		return renderNarrow(m.width, m.height)
	}

	lines := m.headerLines()
	lines = append(lines, strings.Repeat("─", m.width))
	lines = append(lines, m.tabLines()...)
	lines = append(lines, strings.Repeat("─", m.width), "")
	if m.help {
		lines = append(lines,
			styles.Heading("Ayuda"),
			"tab / →        siguiente pestaña",
			"shift+tab / ←  pestaña anterior",
			"n / avpág       página siguiente",
			"↑ / ↓ / enter  seleccionar y abrir una decisión",
			"esc            volver a la lista de decisiones",
			"?              cerrar ayuda",
			"q / ctrl+c     salir",
		)
	} else {
		tab := panelTabs[m.activeTab]
		lines = append(lines, styles.Heading(tab.name), "")
		if tab.readable {
			lines = append(lines, m.activePanelLines()...)
		} else {
			lines = append(lines, "llega en una fase posterior")
		}
	}
	lines = append(lines, "", styles.Muted("tab/→ siguiente · shift+tab/← anterior · ? ayuda · q salir"))
	return fitView(lines, m.width, m.height)
}

func (m Model) headerLines() []string {
	nodeID := "—"
	if m.nodeID != "" {
		nodeID = Sanitize(string(m.nodeID), 40)
	}
	if !m.connected {
		return []string{
			styles.Header("GIFFORD CHANNEL") + " [—]    " + styles.Warning("sin conexión con el nodo") + "    node=" + nodeID,
			"APPROVALS —    " + m.modelHeader() + "    CAPTURE —",
		}
	}

	global := modeLabel(string(m.global.Mode))
	node := "node=" + nodeID
	if m.nodeID != "" && m.hasNode {
		node += " " + modeLabel(string(m.node.EffectiveMode))
	}
	return []string{
		styles.Header("GIFFORD CHANNEL") + " " + global + "    " + node,
		"APPROVALS —    " + m.modelHeader() + "    CAPTURE —",
	}
}

func (m Model) modelHeader() string {
	if m.modelStatus.ActiveProfile != nil {
		return "MODEL " + Sanitize(string(m.modelStatus.ActiveProfile.Profile), 24)
	}
	if m.modelStatus.Deployment.BaseRevision.Availability.Valid() {
		return "MODEL sin perfil"
	}
	return "MODEL —"
}

func (m Model) activePanelLines() []string {
	if !m.connected {
		return []string{"sin conexión con el nodo"}
	}
	if !m.panelReady {
		return []string{"cargando…"}
	}
	if m.panelErr != nil {
		return []string{"lectura no disponible: " + Sanitize(m.panelErr.Error(), max(m.width-24, 1))}
	}
	switch m.activeTab {
	case agentsTab:
		return m.agentLines()
	case messagesTab:
		return m.messageLines()
	case decisionsTab:
		return m.decisionLines()
	case modelTab:
		return m.modelLines()
	default:
		return nil
	}
}

func (m Model) agentLines() []string {
	if len(m.agents.Agents) == 0 {
		return []string{"no hay agentes registrados"}
	}
	lines := make([]string, 0, len(m.agents.Agents)*2+1)
	for _, agent := range m.agents.Agents {
		mode := "RUN"
		if agent.Mode == protocol.AgentRunModeObserved {
			mode = "OBS"
		}
		draft := "sin report"
		if agent.Keyboard != nil {
			draft = string(agent.Keyboard.DraftState)
		}
		lines = append(lines,
			fmt.Sprintf("%s  %s@%s  %s %s  draft=%s",
				Sanitize(string(agent.AgentID), 24), Sanitize(string(agent.Harness), 12),
				Sanitize(agent.HarnessVersion, 12), mode,
				Sanitize(string(agent.SessionState), 14), Sanitize(draft, 12)),
			fmt.Sprintf("  TX=%s RX=%s AUTO=%s  Q=%d D=%d U=%d  phase=%s·%s",
				onOffLabel(agent.Channel.TX), onOffLabel(agent.Channel.RX),
				onOffLabel(agent.Channel.AutoDelivery), agent.Queue.Queued,
				agent.Queue.Delivering, agent.Queue.Uncertain,
				Sanitize(string(agent.Phase.Value), 20), Sanitize(string(agent.Phase.Source), 12)),
		)
	}
	return appendPageHint(lines, m.agents.NextCursor)
}

func (m Model) messageLines() []string {
	if len(m.messages.Messages) == 0 {
		return []string{"no hay entregas de mensajes"}
	}
	lines := make([]string, 0, len(m.messages.Messages)+1)
	for _, message := range m.messages.Messages {
		reason := "—"
		if message.LastReason != "" {
			reason = string(message.LastReason)
		}
		lines = append(lines, fmt.Sprintf(
			"%s  %s → %s  %s %s  intentos=%d motivo=%s edad=%s",
			Sanitize(string(message.MessageID), 26), Sanitize(string(message.FromAgent), 20),
			Sanitize(string(message.ToAgent), 20), Sanitize(string(message.State), 14),
			Sanitize(string(message.Priority), 8), message.Attempts,
			Sanitize(reason, 24), formatAge(m.panelAt, message.CreatedAt),
		))
	}
	return appendPageHint(lines, m.messages.NextCursor)
}

func (m Model) decisionLines() []string {
	if m.detailID != "" {
		if m.detail == nil {
			return []string{"cargando detalle…"}
		}
		return decisionDetailLines(*m.detail)
	}
	if len(m.decisions.Decisions) == 0 {
		return []string{"no hay decisiones registradas"}
	}
	lines := make([]string, 0, len(m.decisions.Decisions)*3+1)
	for index, decision := range m.decisions.Decisions {
		marker := " "
		if index == m.decisionIndex {
			marker = ">"
		}
		verdict := string(decision.SelectedAction)
		if decision.Type == protocol.DecisionTypeChannel {
			verdict = string(decision.Stage) + "/" + string(decision.Outcome)
		}
		lines = append(lines,
			fmt.Sprintf("%s %s  %s %s", marker, Sanitize(string(decision.DecisionID), 30),
				Sanitize(string(decision.Type), 12), Sanitize(verdict, 24)),
			fmt.Sprintf("  agent=%s  message=%s", Sanitize(string(decision.AgentID), 20),
				Sanitize(string(decision.MessageID), 26)),
			"  motivos="+Sanitize(joinReasons(decision.ReasonCodes), 64),
		)
	}
	return appendPageHint(lines, m.decisions.NextCursor)
}

func decisionDetailLines(decision protocol.DecisionGetResult) []string {
	lines := []string{
		"id: " + Sanitize(string(decision.DecisionID), 40),
		"tipo: " + Sanitize(string(decision.Type), 16) +
			"  agente: " + Sanitize(string(decision.AgentID), 24) +
			"  mensaje: " + Sanitize(string(decision.MessageID), 30),
		"motivos: " + Sanitize(joinReasons(decision.ReasonCodes), 80),
	}
	if decision.ChannelEvidence != nil {
		keys := make([]string, 0, len(decision.ChannelEvidence.ControlVersions))
		for key := range decision.ChannelEvidence.ControlVersions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		versions := make([]string, 0, len(keys))
		for _, key := range keys {
			versions = append(versions, fmt.Sprintf("%s=%d", Sanitize(key, 36),
				decision.ChannelEvidence.ControlVersions[key]))
		}
		lines = append(lines,
			"resultado: "+Sanitize(string(decision.Stage), 16)+" / "+Sanitize(string(decision.Outcome), 16),
			"scope decisivo: "+Sanitize(string(decision.ChannelEvidence.DecidingScope), 20),
			"versiones: "+strings.Join(versions, ", "),
		)
	}
	if evidence := decision.OrchestratorEvidence; evidence != nil {
		actions := make([]string, 0, len(evidence.AllowedActions))
		for _, action := range evidence.AllowedActions {
			actions = append(actions, Sanitize(string(action), 24))
		}
		guards := make([]string, 0, len(evidence.Guards))
		for _, guard := range evidence.Guards {
			guards = append(guards, Sanitize(string(guard.Guard), 32)+"="+
				Sanitize(string(guard.Outcome), 16))
		}
		model := "—"
		if evidence.DecisionModel != "" {
			model = Sanitize(string(evidence.DecisionModel), 40)
		}
		call := "—"
		if evidence.GlimmerCallID != "" {
			call = Sanitize(string(evidence.GlimmerCallID), 40)
		}
		lines = append(lines,
			"acción: "+Sanitize(string(evidence.SelectedAction), 24)+"  permitidas: "+strings.Join(actions, ", "),
			"guardas: "+emptyLabel(strings.Join(guards, ", ")),
			"política: "+Sanitize(string(evidence.PolicyVersion), 40)+"  modelo: "+model+"  llamada: "+call,
			"ejecución: "+Sanitize(string(evidence.ExecutionStatus), 20)+"  canal: "+Sanitize(string(evidence.ChannelDecisionID), 40),
		)
	}
	return append(lines, "", "esc vuelve a la lista")
}

func (m Model) modelLines() []string {
	lines := make([]string, 0, 8)
	if m.modelStatus.ActiveProfile == nil {
		lines = append(lines, "perfil: sin perfil activo")
	} else {
		lines = append(lines, "perfil: "+Sanitize(string(m.modelStatus.ActiveProfile.Profile), 32)+
			"  fuente="+Sanitize(string(m.modelStatus.ActiveProfile.Source), 24))
	}
	if m.modelStatus.LastCall == nil {
		lines = append(lines, "última llamada: sin llamadas registradas")
	} else {
		lines = append(lines, fmt.Sprintf("última llamada: %s  %s  %d ms  edad=%s",
			Sanitize(string(m.modelStatus.LastCall.CallID), 40),
			Sanitize(string(m.modelStatus.LastCall.Status), 20),
			m.modelStatus.LastCall.LatencyMS,
			formatAge(m.panelAt, m.modelStatus.LastCall.ObservedAt)))
	}
	lines = append(lines,
		"revisión base: "+metadataLabel(m.modelStatus.Deployment.BaseRevision),
		"revisión adapter: "+metadataLabel(m.modelStatus.Deployment.AdapterRevision),
		"dataset: "+metadataLabel(m.modelStatus.Deployment.DatasetVersion),
		"variante A/B: "+metadataLabel(m.modelStatus.Deployment.ABVariant),
	)
	return lines
}

func metadataLabel(metadata protocol.ModelMetadata) string {
	switch metadata.Availability {
	case protocol.MetadataAvailabilityKnown:
		return Sanitize(string(metadata.Value), 60)
	case protocol.MetadataAvailabilityUnknown:
		return "desconocido"
	default:
		return "no disponible"
	}
}

func joinReasons(reasons []protocol.ReasonCode) string {
	values := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		values = append(values, string(reason))
	}
	return strings.Join(values, ",")
}

func appendPageHint(lines []string, cursor protocol.PageCursor) []string {
	if cursor == "" {
		return lines
	}
	return append(lines, "", "n / avpág carga la página siguiente")
}

func onOffLabel(value bool) string {
	if value {
		return "ON"
	}
	return "OFF"
}

func formatAge(now, then time.Time) string {
	if now.IsZero() || then.IsZero() || now.Before(then) {
		return "0s"
	}
	age := now.Sub(then)
	switch {
	case age >= 24*time.Hour:
		return fmt.Sprintf("%dd", int(age/(24*time.Hour)))
	case age >= time.Hour:
		return fmt.Sprintf("%dh", int(age/time.Hour))
	case age >= time.Minute:
		return fmt.Sprintf("%dm", int(age/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(age/time.Second))
	}
}

func emptyLabel(value string) string {
	if value == "" {
		return "—"
	}
	return value
}

func (m Model) tabLines() []string {
	lines := make([]string, 0, 2)
	current := ""
	for i, tab := range panelTabs {
		label := tab.name
		if i == m.activeTab {
			label = styles.Selected(label)
		}
		separator := ""
		if current != "" {
			separator = " · "
		}
		if current != "" && ansi.StringWidth(current+separator+label) > m.width {
			lines = append(lines, current)
			current = label
			continue
		}
		current += separator + label
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func modeLabel(mode string) string {
	clean := Sanitize(mode, 16)
	if clean == "" {
		clean = "—"
	}
	return styles.Mode(clean, "["+clean+"]")
}

func renderNarrow(width, height int) string {
	lines := []string{
		styles.Warning("gifford panel necesita al menos 80x24"),
		fmt.Sprintf("terminal actual: %dx%d", width, height),
		"redimensiona el terminal o pulsa q para salir",
	}
	return fitView(lines, max(width, 1), max(height, 1))
}

func fitView(lines []string, width, height int) string {
	if len(lines) > height {
		lines = lines[:height]
	}
	for i := range lines {
		lines[i] = strings.TrimRight(ansi.Truncate(lines[i], width, ""), " ")
	}
	return strings.Join(lines, "\n")
}
