// Package styles owns every ANSI style token used by the panel.
package styles

import "github.com/charmbracelet/x/ansi"

var (
	headerStyle   = ansi.NewStyle().Bold()
	headingStyle  = ansi.NewStyle().Bold()
	selectedStyle = ansi.NewStyle().Bold().Underline(true)
	mutedStyle    = ansi.NewStyle().Faint()
	warningStyle  = ansi.NewStyle().Bold()
	activeStyle   = ansi.NewStyle().Bold()
	pausedStyle   = ansi.NewStyle().Faint()
	drainingStyle = ansi.NewStyle().Underline(true)
	offStyle      = ansi.NewStyle().Bold().Reverse(true)
)

// Header renders the panel title token.
func Header(text string) string { return headerStyle.Styled(text) }

// Heading renders a section heading token.
func Heading(text string) string { return headingStyle.Styled(text) }

// Selected renders the active tab token.
func Selected(text string) string { return selectedStyle.Styled(text) }

// Muted renders secondary help text.
func Muted(text string) string { return mutedStyle.Styled(text) }

// Warning renders a warning token.
func Warning(text string) string { return warningStyle.Styled(text) }

// Mode renders one channel mode without coupling styles to protocol types.
func Mode(mode, text string) string {
	switch mode {
	case "ACTIVE":
		return activeStyle.Styled(text)
	case "PAUSED":
		return pausedStyle.Styled(text)
	case "DRAINING":
		return drainingStyle.Styled(text)
	case "OFF":
		return offStyle.Styled(text)
	default:
		return mutedStyle.Styled(text)
	}
}
