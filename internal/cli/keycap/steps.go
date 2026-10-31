package keycap

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
)

// StepKind names one guided measurement of the tour.
type StepKind string

// Tour steps, in the order the guide prompts them.
const (
	StepSubmit        StepKind = "submit"
	StepInsertNewline StepKind = "insert_newline"
	StepClearLine     StepKind = "clear_line"
	StepCancel        StepKind = "cancel"
	StepEsc           StepKind = "esc"
	StepCtrlC         StepKind = "ctrl_c"
	StepBackspace     StepKind = "backspace"
	StepArrowUp       StepKind = "arrow_up"
	StepArrowDown     StepKind = "arrow_down"
	StepArrowLeft     StepKind = "arrow_left"
	StepArrowRight    StepKind = "arrow_right"
	StepHome          StepKind = "home"
	StepEnd           StepKind = "end"
	StepTab           StepKind = "tab"
	StepPaste         StepKind = "paste"
)

// tour is the full guided sequence: the four profile sequences first,
// then the evidence keys for the terminal mode, then the paste.
var tour = []StepKind{
	StepSubmit, StepInsertNewline, StepCancel, StepClearLine,
	StepEsc, StepCtrlC, StepBackspace,
	StepArrowUp, StepArrowDown, StepArrowLeft, StepArrowRight,
	StepHome, StepEnd, StepTab,
	StepPaste,
}

// prompt is the stderr line guiding one step.
func prompt(s StepKind) string {
	switch s {
	case StepSubmit:
		return "Pulsa la tecla que ENVIA (Enter)."
	case StepInsertNewline:
		return "Pulsa la tecla de SALTO DE LINEA dentro del borrador."
	case StepCancel:
		return "Pulsa la tecla que DEJA VACIO UN BORRADOR CON TEXTO."
	case StepClearLine:
		return "Pulsa la tecla que BORRA LA LINEA."
	case StepEsc:
		return "Pulsa ESCAPE."
	case StepCtrlC:
		return "Pulsa CONTROL-C."
	case StepBackspace:
		return "Pulsa RETROCESO."
	case StepArrowUp:
		return "Pulsa FLECHA ARRIBA."
	case StepArrowDown:
		return "Pulsa FLECHA ABAJO."
	case StepArrowLeft:
		return "Pulsa FLECHA IZQUIERDA."
	case StepArrowRight:
		return "Pulsa FLECHA DERECHA."
	case StepHome:
		return "Pulsa INICIO (Home)."
	case StepEnd:
		return "Pulsa FIN (End)."
	case StepTab:
		return "Pulsa TABULADOR."
	case StepPaste:
		return "Pega un texto de varias lineas."
	}
	return "Pulsa la tecla indicada."
}

// keyShape is how one measured unit arrived: plain xterm shapes, CSI u
// (kitty-style extended keyboard) or modifyOtherKeys.
type keyShape string

const (
	shapeXterm       keyShape = "xterm"
	shapeCSIU        keyShape = "csi_u"
	shapeModifyOther keyShape = "modify_other"
)

// shapeOf classifies measured bytes by syntax alone, without any
// profile: a trailing u over digits is CSI u, ESC [ 27 ; .. ~ is
// modifyOtherKeys, anything else counts as plain xterm.
func shapeOf(unit []byte) keyShape {
	if len(unit) < 4 || unit[0] != 0x1b || unit[1] != '[' {
		return shapeXterm
	}
	if unit[len(unit)-1] == 'u' && digitsOrColon(unit[2:len(unit)-1]) {
		return shapeCSIU
	}
	if bytes.HasPrefix(unit, []byte("\x1b[27;")) && unit[len(unit)-1] == '~' {
		return shapeModifyOther
	}
	return shapeXterm
}

// digitsOrColon reports whether every byte is a digit, ";" or ":".
func digitsOrColon(b []byte) bool {
	for _, c := range b {
		if c != ';' && c != ':' && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// csiUFirstParam returns the codepoint head of a CSI u unit: the digits
// before the first ";" or ":", or "" when the unit is not CSI u shaped.
func csiUFirstParam(unit []byte) string {
	if shapeOf(unit) != shapeCSIU {
		return ""
	}
	head := string(unit[2 : len(unit)-1])
	if i := strings.IndexAny(head, ";:"); i >= 0 {
		head = head[:i]
	}
	return head
}

const (
	pasteOn  = "\x1b[200~"
	pasteOff = "\x1b[201~"
)

// newNeutralParser classifies without profile sequences: only the generic
// xterm table answers, so submit-shaped bytes stay non-text while typed
// letters stay text. Parsers are single-use here because a trailing ESC
// would pollute the next verification.
func newNeutralParser() *input.Parser {
	return input.NewParser(profiles.Profile{})
}

// verify reports whether unit is a plausible answer for the step. Every
// step accepts at most one key unit; pasted blobs at other steps are
// rejected so stray pastes never become sequences. A unit the parser
// cannot resolve at all is rejected too.
func verify(step StepKind, unit []byte) bool {
	if len(unit) == 0 || bytes.HasPrefix(unit, []byte(pasteOn)) || terminalUnit(unit) {
		return false
	}
	switch step {
	case StepSubmit, StepInsertNewline, StepClearLine, StepCancel:
		parser := newNeutralParser()
		evs := parser.Feed(unit)
		if parser.Pending() || len(evs) != 1 {
			return false
		}
		return evs[0].Kind != input.KindText && evs[0].Kind != input.KindTerminal
	case StepEsc:
		return len(unit) == 1 && unit[0] == 0x1b || csiUFirstParam(unit) == "27"
	case StepCtrlC:
		return len(unit) == 1 && unit[0] == 0x03 || csiUFirstParam(unit) == "99"
	case StepBackspace:
		return len(unit) == 1 && (unit[0] == 0x7f || unit[0] == 0x08) ||
			csiUFirstParam(unit) == "127"
	case StepTab:
		return isKey(unit, input.KeyTab) || csiUFirstParam(unit) == "9"
	case StepArrowUp:
		return isKey(unit, input.KeyArrowUp)
	case StepArrowDown:
		return isKey(unit, input.KeyArrowDown)
	case StepArrowLeft:
		return isKey(unit, input.KeyArrowLeft)
	case StepArrowRight:
		return isKey(unit, input.KeyArrowRight)
	case StepHome:
		return isKey(unit, input.KeyHome)
	case StepEnd:
		return isKey(unit, input.KeyEnd)
	case StepPaste:
		return true
	}
	return false
}

// isKey reports whether unit parses to the expected navigation key under
// the generic table, or arrives in an extended shape carrying the same
// key: plain CSI/SS3 arrows and home/end, or their CSI u form.
func isKey(unit []byte, want input.KeyCode) bool {
	for _, ev := range newNeutralParser().Feed(unit) {
		if ev.Kind == input.KindKey && ev.Key.Code == want {
			return true
		}
	}
	if shapeOf(unit) != shapeXterm {
		return true
	}
	if len(unit) == 3 && unit[0] == 0x1b && unit[1] == 'O' {
		switch unit[2] {
		case 'A':
			return want == input.KeyArrowUp
		case 'B':
			return want == input.KeyArrowDown
		case 'C':
			return want == input.KeyArrowRight
		case 'D':
			return want == input.KeyArrowLeft
		case 'H':
			return want == input.KeyHome
		case 'F':
			return want == input.KeyEnd
		}
	}
	return false
}

// completeStep cuts one key unit off the head of buf for a non-paste
// step: exactly one UTF-8 rune outside escapes, one full CSI/SS3/Alt
// sequence, or a whole bracketed paste that strayed here. It reports the
// unit, the rest, and whether the head is complete; a lone ESC never
// completes here, the timer closes it.
func completeStep(buf []byte) (unit, rest []byte, complete bool) {
	if len(buf) == 0 {
		return nil, buf, false
	}
	if n, kind := input.CutTerminalUnit(buf); kind != input.TerminalUnitNone {
		if kind == input.TerminalUnitIncomplete {
			return nil, buf, false
		}
		return buf[:n], buf[n:], true
	}
	if bytes.HasPrefix(buf, []byte(pasteOn)) {
		if i := bytes.Index(buf, []byte(pasteOff)); i >= 0 {
			end := i + len(pasteOff)
			return buf[:end], buf[end:], true
		}
		return nil, buf, false
	}
	if buf[0] != 0x1b {
		_, size := utf8.DecodeRune(buf)
		if size == 0 || (size == 1 && !utf8.FullRune(buf)) {
			return nil, buf, false
		}
		return buf[:size], buf[size:], true
	}
	if len(buf) == 1 {
		return nil, buf, false
	}
	switch buf[1] {
	case '[':
		for i := 2; i < len(buf); i++ {
			c := buf[i]
			if c >= 0x40 && c <= 0x7E {
				return buf[:i+1], buf[i+1:], true
			}
			if c < 0x20 || c == 0x7f {
				return buf[:i+1], buf[i+1:], true
			}
		}
		return nil, buf, false
	case 'O':
		if len(buf) < 3 {
			return nil, buf, false
		}
		return buf[:3], buf[3:], true
	default:
		_, size := utf8.DecodeRune(buf[1:])
		if size == 0 || (size == 1 && !utf8.FullRune(buf[1:])) {
			return nil, buf, false
		}
		return buf[:1+size], buf[1+size:], true
	}
}

// terminalUnit reports whether unit is a complete terminal response, focus
// report, or mouse unit. Keycap ignores all of them: clicks remain human
// priority in the live proxy, but they are never answers to a keyboard tour.
func terminalUnit(unit []byte) bool {
	n, kind := input.CutTerminalUnit(unit)
	return n == len(unit) && (kind == input.TerminalUnitReport ||
		kind == input.TerminalUnitMouseMotion || kind == input.TerminalUnitMouseAction)
}

// measurement is one finished tour: the raw bytes per sequence step, the
// paste finding, the extended keyboard finding, and why measured stays
// false when something is missing.
type measurement struct {
	submit        []byte
	insertNewline []byte
	clearLine     []byte
	cancel        []byte
	shapes        []keyShape
	pasteWrapped  *bool
	version       string
	incomplete    []string
}

// extended resolves the profile value from the recorded shapes: any CSI
// u vote wins, then any modifyOtherKeys vote, else plain xterm. Mixed
// extended votes fail closed with a reason.
func (m *measurement) extended() (profiles.ExtendedKeyboard, string) {
	var csi, modify bool
	for _, s := range m.shapes {
		switch s {
		case shapeCSIU:
			csi = true
		case shapeModifyOther:
			modify = true
		default:
			// Plain xterm arrivals vote nothing.
		}
	}
	if csi && modify {
		return profiles.ExtendedNone, "mixed csi_u and modify_other_keys arrivals"
	}
	if csi {
		return profiles.ExtendedCSIU, ""
	}
	if modify {
		return profiles.ExtendedModifyOtherKeys, ""
	}
	return profiles.ExtendedNone, ""
}

// record keeps one verified step: sequence bytes go to their field and
// every unit votes its shape.
func (m *measurement) record(step StepKind, unit []byte) {
	cp := append([]byte(nil), unit...)
	m.shapes = append(m.shapes, shapeOf(unit))
	switch step {
	case StepSubmit:
		m.submit = cp
	case StepInsertNewline:
		m.insertNewline = cp
	case StepClearLine:
		m.clearLine = cp
	case StepCancel:
		m.cancel = cp
	default:
		// Evidence keys carry no sequence bytes.
	}
}

// complete reports whether every step the profile needs was measured:
// the four sequences and the paste. Navigation keys only vote the
// extended mode, so a missing one cannot fail the profile alone.
func (m *measurement) complete() bool {
	return len(m.submit) > 0 && len(m.insertNewline) > 0 &&
		len(m.clearLine) > 0 && len(m.cancel) > 0 && m.pasteWrapped != nil
}

// yamlEscape renders bytes as a YAML double-quoted scalar body: the
// short escapes where they exist, \xHH for other controls, \uHHHH
// above ASCII, printable ASCII untouched.
func yamlEscape(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch r {
		case '\r':
			sb.WriteString(`\r`)
		case '\n':
			sb.WriteString(`\n`)
		case '\t':
			sb.WriteString(`\t`)
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		default:
			switch {
			case r == 0x1b:
				sb.WriteString(`\x1b`)
			case r < 0x20 || r == 0x7f:
				fmt.Fprintf(&sb, `\x%02x`, r)
			case r < 0x80:
				sb.WriteRune(r)
			case r <= 0xffff:
				fmt.Fprintf(&sb, `\u%04x`, r)
			default:
				fmt.Fprintf(&sb, `\U%08x`, r)
			}
		}
	}
	return sb.String()
}

// validateCandidate loads the rendered profile with the production
// rules and replays every stored sequence through a fresh parser: each
// must resolve to exactly its own event. This catches NUL bytes,
// duplicated sequences and ambiguous prefixes before anything touches
// the disk.
func validateCandidate(id, content string, m *measurement) error {
	prof, err := profiles.ParseFile(id, []byte(content))
	if err != nil {
		return fmt.Errorf("keycap: candidate profile invalid: %w", err)
	}
	for _, tc := range []struct {
		name string
		kind input.Kind
		seq  []byte
	}{
		{"submit", input.KindSubmit, m.submit},
		{"insert_newline", input.KindInsertNewline, m.insertNewline},
		{"clear_line", input.KindClearLine, m.clearLine},
		{"cancel", input.KindCancel, m.cancel},
	} {
		evs := input.NewParser(prof).Feed(tc.seq)
		if len(evs) != 1 || evs[0].Kind != tc.kind {
			return fmt.Errorf("keycap: %s bytes %q do not replay to %s", tc.name, tc.seq, tc.kind)
		}
	}
	return nil
}

// renderProfile builds profiles/<id>.yaml from a measurement: every
// measured value, the detected version pinned in version_range, and the
// input line and optional input placeholder preserved verbatim because keycap
// never measures them. ext is the already resolved extended_keyboard value.
func renderProfile(id string, m *measurement, inputLine, inputPlaceholder, versionRange string, ext profiles.ExtendedKeyboard) string {
	var sb strings.Builder
	sb.WriteString("# Measured with gifford keycap; input_line and input_placeholder stay hand-tuned.\n")
	fmt.Fprintf(&sb, "harness: %s\n", id)
	sb.WriteString("measured: true\n")
	fmt.Fprintf(&sb, "version_range: \"%s\"\n", yamlEscape(versionRange))
	fmt.Fprintf(&sb, "submit: \"%s\"\n", yamlEscape(string(m.submit)))
	fmt.Fprintf(&sb, "insert_newline: \"%s\"\n", yamlEscape(string(m.insertNewline)))
	fmt.Fprintf(&sb, "clear_line: \"%s\"\n", yamlEscape(string(m.clearLine)))
	fmt.Fprintf(&sb, "cancel: \"%s\"\n", yamlEscape(string(m.cancel)))
	fmt.Fprintf(&sb, "bracketed_paste: %v\n", m.pasteWrapped != nil && *m.pasteWrapped)
	fmt.Fprintf(&sb, "extended_keyboard: %s\n", ext)
	fmt.Fprintf(&sb, "input_line: \"%s\"\n", yamlEscape(inputLine))
	if inputPlaceholder != "" {
		fmt.Fprintf(&sb, "input_placeholder: \"%s\"\n", yamlEscape(inputPlaceholder))
	}
	return sb.String()
}
