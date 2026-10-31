package input

import (
	"bytes"
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

// maxTerminalStringBytes bounds retained recognized DCS and OSC response
// ambiguity. A sequence that crosses the bound is replayed as ordinary input
// from its introducer, preserving keys and bounding memory without a timer.
const maxTerminalStringBytes = 4096

const (
	c1DCS = 0x90
	c1CSI = 0x9b
	c1ST  = 0x9c
	c1OSC = 0x9d
	c1APC = 0x9f
)

type terminalStringFamily uint8

const (
	terminalStringDCS terminalStringFamily = iota
	terminalStringOSC
	terminalStringAPC
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
	if intro, family, ok := terminalStringIntro(b); ok {
		return cutTerminalString(b, intro, family)
	}
	return cutCSIReport(b)
}

// TerminalStringPrefix reports whether b remains a prefix of a recognized
// DCS or OSC response. It lets keycap distinguish a string whose completion
// must be byte-driven from ordinary truncated keys that its timer may reject.
func TerminalStringPrefix(b []byte) bool {
	intro, family, ok := terminalStringIntro(b)
	if !ok {
		return false
	}
	_, kind := cutTerminalString(b, intro, family)
	return kind == TerminalUnitIncomplete || kind == TerminalUnitReport
}

// terminalStringIntro recognizes the 7-bit and 8-bit DCS, OSC, and APC
// introducers. OSC alone also accepts BEL as its terminator.
func terminalStringIntro(b []byte) (intro int, family terminalStringFamily, ok bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	switch b[0] {
	case c1DCS:
		return 1, terminalStringDCS, true
	case c1OSC:
		return 1, terminalStringOSC, true
	case c1APC:
		return 1, terminalStringAPC, true
	case esc:
		if len(b) < 2 {
			return 0, 0, false
		}
		switch b[1] {
		case 'P':
			return 2, terminalStringDCS, true
		case ']':
			return 2, terminalStringOSC, true
		case '_':
			return 2, terminalStringAPC, true
		}
	}
	return 0, 0, false
}

func cutTerminalString(b []byte, intro int, family terminalStringFamily) (int, TerminalUnitKind) {
	end, terminator := len(b), 0
	for i := intro; i < len(b); i++ {
		if i+1 > maxTerminalStringBytes {
			return 0, TerminalUnitNone
		}
		switch b[i] {
		case 0x18, 0x1a: // CAN and SUB cancel a control string.
			return 0, TerminalUnitNone
		case '\a':
			if family == terminalStringOSC {
				end, terminator = i, 1
			}
		case c1ST:
			end, terminator = i, 1
		case esc:
			if i+1 == len(b) {
				end = i
				break
			}
			if b[i+1] == '\\' {
				end, terminator = i, 2
				break
			}
			// An ESC other than ST ends the ambiguity. Replaying from the
			// introducer preserves the human escape sequence that followed.
			return 0, TerminalUnitNone
		}
		if end != len(b) {
			break
		}
	}
	possible, complete := terminalStringResponse(family, b[intro:end])
	if !possible {
		return 0, TerminalUnitNone
	}
	if terminator != 0 {
		if !complete {
			return 0, TerminalUnitNone
		}
		return end + terminator, TerminalUnitReport
	}
	if len(b) >= maxTerminalStringBytes {
		return 0, TerminalUnitNone
	}
	return 0, TerminalUnitIncomplete
}

// terminalStringResponse admits only response families with a concrete wire
// grammar used by supported harnesses. An arbitrary DCS, OSC, or APC envelope
// is still ordinary input: Alt+P, Alt+], or Alt+_ followed by typed bytes.
// possible says more bytes can still make a response; complete says a
// terminator may validly end it now.
func terminalStringResponse(family terminalStringFamily, body []byte) (possible, complete bool) {
	switch family {
	case terminalStringDCS:
		return dcsResponse(body)
	case terminalStringOSC:
		return oscResponse(body)
	case terminalStringAPC:
		return false, false
	default:
		return false, false
	}
}

func dcsResponse(body []byte) (possible, complete bool) {
	// XTVERSION replies with DCS > | text ST. A non-empty printable
	// version string is required before the terminator.
	const xtVersion = ">|"
	if bytes.HasPrefix([]byte(xtVersion), body) {
		return true, false
	}
	if bytes.HasPrefix(body, []byte(xtVersion)) {
		payload := body[len(xtVersion):]
		return printableTerminalPayload(payload), len(payload) > 0
	}

	// DECRQSS and XTGETTCAP replies start with a success flag followed
	// by "$r" or "+r". Their value may be empty on a negative reply.
	for _, prefix := range []string{"0$r", "1$r", "0+r", "1+r"} {
		if len(body) < len(prefix) && bytes.HasPrefix([]byte(prefix), body) {
			return true, false
		}
		if bytes.HasPrefix(body, []byte(prefix)) {
			return printableTerminalPayload(body[len(prefix):]), true
		}
	}

	// DA3 replies with DECRPTUI: "!|" followed by the eight hexadecimal
	// digits of its site code and serial number.
	const da3 = "!|"
	if bytes.HasPrefix([]byte(da3), body) {
		return true, false
	}
	if bytes.HasPrefix(body, []byte(da3)) {
		payload := body[len(da3):]
		if len(payload) > 8 {
			return false, false
		}
		for _, c := range payload {
			if !isHex(c) {
				return false, false
			}
		}
		return true, len(payload) == 8
	}
	return false, false
}

func oscResponse(body []byte) (possible, complete bool) {
	// OSC 11 is the background-colour reply exercised by the supported
	// harnesses. XParseColor replies are rgb:R/G/B with one to four hex
	// digits per component.
	const backgroundRGB = "11;rgb:"
	if bytes.HasPrefix([]byte(backgroundRGB), body) {
		return true, false
	}
	if bytes.HasPrefix(body, []byte(backgroundRGB)) {
		return rgbResponse(body[len(backgroundRGB):])
	}
	return oscPaletteResponse(body)
}

func oscPaletteResponse(body []byte) (possible, complete bool) {
	// OSC 4 replies repeat the queried palette index and replace "?" with
	// one XParseColor rgb value. Keep this to one index/value pair: terminals
	// may answer a multi-index query with multiple independent replies.
	const palette = "4;"
	if bytes.HasPrefix([]byte(palette), body) {
		return true, false
	}
	if !bytes.HasPrefix(body, []byte(palette)) {
		return false, false
	}
	rest := body[len(palette):]
	separator := bytes.IndexByte(rest, ';')
	if separator < 0 {
		return decimalBytes(rest), false
	}
	if separator == 0 || !decimalBytes(rest[:separator]) {
		return false, false
	}
	const rgb = "rgb:"
	value := rest[separator+1:]
	if bytes.HasPrefix([]byte(rgb), value) {
		return true, false
	}
	if !bytes.HasPrefix(value, []byte(rgb)) {
		return false, false
	}
	return rgbResponse(value[len(rgb):])
}

func rgbResponse(value []byte) (possible, complete bool) {
	parts := bytes.Split(value, []byte{'/'})
	if len(parts) > 3 {
		return false, false
	}
	for i, part := range parts {
		if len(part) > 4 || i < len(parts)-1 && len(part) == 0 {
			return false, false
		}
		for _, c := range part {
			if !isHex(c) {
				return false, false
			}
		}
	}
	if len(parts) != 3 || len(parts[2]) == 0 {
		return true, false
	}
	return true, true
}

func printableTerminalPayload(payload []byte) bool {
	for _, c := range payload {
		if c < 0x20 || c == 0x7f || c > 0x7e {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func decimalBytes(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
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
	if isDAReport(body, final) || isDECRPMReport(body, final) || isCPRReport(body, final) ||
		isDSRReport(body, final) || isKittyFlagsReport(body, final) ||
		isXTWINOPSReport(body, final) || isXTQMODKEYSReport(body, final) {
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

func isDSRReport(body string, final byte) bool {
	if final != 'n' {
		return false
	}
	return body == "0" || body == "?997;1" || body == "?997;2"
}

func isKittyFlagsReport(body string, final byte) bool {
	return final == 'u' && strings.HasPrefix(body, "?") && numericParams(body[1:], 1, 1)
}

func isXTWINOPSReport(body string, final byte) bool {
	if final != 't' {
		return false
	}
	parts := strings.Split(body, ";")
	if len(parts) != 3 || parts[0] != "4" && parts[0] != "6" && parts[0] != "8" {
		return false
	}
	return numericParams(strings.Join(parts[1:], ";"), 2, 2)
}

func isXTQMODKEYSReport(body string, final byte) bool {
	if final != 'm' || !strings.HasPrefix(body, ">") {
		return false
	}
	parts := strings.Split(body[1:], ";")
	if len(parts) != 2 {
		return false
	}
	switch parts[0] {
	case "0", "1", "2", "3", "4", "6", "7":
		return numericParams(parts[1], 1, 1)
	default:
		return false
	}
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
