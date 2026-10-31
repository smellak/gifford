package input

import (
	"strconv"
	"strings"
)

// TerminalUnitKind is the result of cutting a terminal-originated input unit.
// The cut is shared with keycap so measurement and the live proxy agree about
// report boundaries. None of the complete kinds carries report payload beyond
// this package.
type TerminalUnitKind uint8

const (
	// TerminalUnitNone means the head is ordinary input, an unknown sequence,
	// or an overlong/invalid terminal string that must degrade to ordinary
	// input rather than swallow later keys.
	TerminalUnitNone TerminalUnitKind = iota
	// TerminalUnitIncomplete means the head can still become a complete
	// terminal unit. Callers retain it without authorizing delivery.
	TerminalUnitIncomplete
	// TerminalUnitReport is a terminal response or focus report.
	TerminalUnitReport
	// TerminalUnitMouseMotion is pointer motion with no pressed button.
	TerminalUnitMouseMotion
	// TerminalUnitMouseAction is a click, release, drag, or wheel action. The
	// proxy keeps its existing human-priority semantics for this kind.
	TerminalUnitMouseAction
)

// maxTerminalStringBytes bounds retained DCS, OSC, and APC ambiguity. A
// sequence that crosses the bound is replayed as ordinary input from its
// introducer, preserving keys and bounding retained memory without a timer.
const maxTerminalStringBytes = 4096

const (
	c1DCS = 0x90
	c1CSI = 0x9b
	c1ST  = 0x9c
	c1OSC = 0x9d
	c1APC = 0x9f
)

// CutTerminalUnit cuts one recognized terminal-originated unit from the head
// of b. n is nonzero only for a complete unit. Split feeds produce the same
// boundary as whole feeds; incomplete control strings retain at most the fixed
// limit above before degrading conservatively to ordinary input.
func CutTerminalUnit(b []byte) (n int, kind TerminalUnitKind) {
	if len(b) == 0 {
		return 0, TerminalUnitNone
	}
	if len(b) == 1 && b[0] == esc {
		return 0, TerminalUnitIncomplete
	}
	if intro, osc, ok := terminalStringIntro(b); ok {
		return cutTerminalString(b, intro, osc)
	}
	return cutCSIReport(b)
}

// TerminalStringPrefix reports whether b starts with a DCS, OSC, or APC
// introducer. It lets keycap distinguish a string whose completion must be
// byte-driven from ordinary truncated CSI keys that its key timer may reject.
func TerminalStringPrefix(b []byte) bool {
	_, _, ok := terminalStringIntro(b)
	return ok
}

// terminalStringIntro recognizes the 7-bit and 8-bit DCS, OSC, and APC
// introducers. OSC alone also accepts BEL as its terminator.
func terminalStringIntro(b []byte) (intro int, osc, ok bool) {
	if len(b) == 0 {
		return 0, false, false
	}
	switch b[0] {
	case c1DCS, c1APC:
		return 1, false, true
	case c1OSC:
		return 1, true, true
	case esc:
		if len(b) < 2 {
			return 0, false, false
		}
		switch b[1] {
		case 'P', '_':
			return 2, false, true
		case ']':
			return 2, true, true
		}
	}
	return 0, false, false
}

func cutTerminalString(b []byte, intro int, osc bool) (int, TerminalUnitKind) {
	if len(b) <= intro {
		return 0, TerminalUnitIncomplete
	}
	for i := intro; i < len(b); i++ {
		if i+1 > maxTerminalStringBytes {
			return 0, TerminalUnitNone
		}
		switch b[i] {
		case 0x18, 0x1a: // CAN and SUB cancel a control string.
			return 0, TerminalUnitNone
		case '\a':
			if osc {
				return i + 1, TerminalUnitReport
			}
		case c1ST:
			return i + 1, TerminalUnitReport
		case esc:
			if i+1 == len(b) {
				return 0, TerminalUnitIncomplete
			}
			if b[i+1] == '\\' {
				return i + 2, TerminalUnitReport
			}
			// An ESC other than ST ends the ambiguity. Replaying from the
			// introducer preserves the human escape sequence that followed.
			return 0, TerminalUnitNone
		}
	}
	if len(b) >= maxTerminalStringBytes {
		return 0, TerminalUnitNone
	}
	return 0, TerminalUnitIncomplete
}

func cutCSIReport(b []byte) (int, TerminalUnitKind) {
	head := 0
	switch {
	case len(b) >= 2 && b[0] == esc && b[1] == '[':
		head = 2
	case b[0] == c1CSI:
		head = 1
	default:
		return 0, TerminalUnitNone
	}
	end := -1
	for i := head; i < len(b); i++ {
		c := b[i]
		if c >= 0x40 && c <= 0x7e {
			end = i
			break
		}
		if c < 0x20 || c == 0x7f {
			return 0, TerminalUnitNone
		}
	}
	if end < 0 {
		return 0, TerminalUnitIncomplete
	}
	body, final := string(b[head:end]), b[end]
	if final == 'M' && body == "" { // X10: CSI M Cb Cx Cy.
		if len(b) < end+4 {
			return 0, TerminalUnitIncomplete
		}
		button := int(b[end+1]) - 32
		return end + 4, mouseUnit(button)
	}
	if (final == 'M' || final == 'm') && strings.HasPrefix(body, "<") {
		button, ok := sgrMouseButton(body)
		if !ok {
			return 0, TerminalUnitNone
		}
		return end + 1, mouseUnit(button)
	}
	if body == "" && (final == 'I' || final == 'O') {
		return end + 1, TerminalUnitReport
	}
	if isDAReport(body, final) || isDECRPMReport(body, final) || isCPRReport(body, final) {
		return end + 1, TerminalUnitReport
	}
	return 0, TerminalUnitNone
}

func sgrMouseButton(body string) (int, bool) {
	parts := strings.Split(strings.TrimPrefix(body, "<"), ";")
	if len(parts) != 3 {
		return 0, false
	}
	values := [3]int{}
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return 0, false
		}
		values[i] = value
	}
	return values[0], true
}

func mouseUnit(button int) TerminalUnitKind {
	if button < 0 {
		return TerminalUnitMouseAction
	}
	// Both SGR and X10 encode motion in bit 5 and "no button" in the
	// low two bits. X10 has already had its 32-byte offset removed.
	if button&32 != 0 && button&3 == 3 && button&64 == 0 {
		return TerminalUnitMouseMotion
	}
	return TerminalUnitMouseAction
}

func isDAReport(body string, final byte) bool {
	if final != 'c' || len(body) < 2 || !strings.ContainsRune("?>=", rune(body[0])) {
		return false
	}
	return numericParams(body[1:], 1, -1)
}

func isDECRPMReport(body string, final byte) bool {
	if final != 'y' || !strings.HasSuffix(body, "$") {
		return false
	}
	body = strings.TrimPrefix(body, "?")
	return numericParams(strings.TrimSuffix(body, "$"), 2, 2)
}

func isCPRReport(body string, final byte) bool {
	if final != 'R' {
		return false
	}
	body = strings.TrimPrefix(body, "?")
	return numericParams(body, 2, 2)
}

func numericParams(s string, minParts, maxParts int) bool {
	parts := strings.Split(s, ";")
	if len(parts) < minParts || maxParts >= 0 && len(parts) > maxParts {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}
