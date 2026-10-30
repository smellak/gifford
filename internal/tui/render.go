package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/x/ansi"

	"github.com/smellak/gifford/internal/tui/styles"
)

const (
	minimumWidth  = 80
	minimumHeight = 24
)

type panelTab struct {
	name    string
	missing bool
}

var panelTabs = []panelTab{
	{name: "Agents", missing: true},
	{name: "Messages", missing: true},
	{name: "Artifacts"},
	{name: "Links"},
	{name: "Loops"},
	{name: "Approvals"},
	{name: "Nodes"},
	{name: "Decisions", missing: true},
	{name: "Events"},
	{name: "Capture"},
	{name: "Model", missing: true},
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
			"?              cerrar ayuda",
			"q / ctrl+c     salir",
		)
	} else {
		tab := panelTabs[m.activeTab]
		lines = append(lines, styles.Heading(tab.name), "")
		if tab.missing {
			lines = append(lines, "falta el método de lectura (8.4b)")
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
			"APPROVALS —    MODEL —    CAPTURE —",
		}
	}

	global := modeLabel(string(m.global.Mode))
	node := "node=" + nodeID
	if m.nodeID != "" && m.hasNode {
		node += " " + modeLabel(string(m.node.EffectiveMode))
	}
	return []string{
		styles.Header("GIFFORD CHANNEL") + " " + global + "    " + node,
		"APPROVALS —    MODEL —    CAPTURE —",
	}
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
		lines[i] = ansi.Truncate(lines[i], width, "")
	}
	return strings.Join(lines, "\n")
}
