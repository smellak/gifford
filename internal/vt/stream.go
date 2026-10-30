package vt

import (
	"bytes"
	"unicode/utf8"

	"github.com/rivo/uniseg"
)

const maxHeldBytes = 256

type parseState uint8

const (
	stateGround parseState = iota
	stateEscape
	stateEscapeIntermediate
	stateCSIEntry
	stateCSIParam
	stateCSIIntermediate
	stateDCSEntry
	stateDCSParam
	stateDCSIntermediate
	stateDCSData
	stateOSC
	stateAPC
	statePM
	stateSOS
	stateUTF8One
	stateUTF8Two
	stateUTF8Three
)

type csiTracker struct {
	empty         bool
	private       bool
	modeGrammar   bool
	has69         bool
	has2026       bool
	haveParam     bool
	paramTooLarge bool
	param         int
}

func newCSITracker() csiTracker { return csiTracker{empty: true, modeGrammar: true} }

func (t *csiTracker) collect(b byte) {
	switch {
	case b < 0x20 || b == 0x7f:
		return
	case b == '?' && t.empty:
		t.empty = false
		t.private = true
	case b >= '0' && b <= '9':
		t.empty = false
		if !t.modeGrammar {
			return
		}
		t.haveParam = true
		digit := int(b - '0')
		if !t.paramTooLarge {
			if t.param > (2026-digit)/10 {
				t.paramTooLarge = true
			} else {
				t.param = t.param*10 + digit
			}
		}
	case b == ';':
		t.empty = false
		t.finishParam()
	default:
		t.empty = false
		t.modeGrammar = false
	}
}

func (t *csiTracker) finishParam() {
	if t.private && t.modeGrammar && t.haveParam && !t.paramTooLarge {
		switch t.param {
		case 69:
			t.has69 = true
		case 2026:
			t.has2026 = true
		}
	}
	t.param = 0
	t.haveParam = false
	t.paramTooLarge = false
}

// holdStart returns the prefix to feed now and the parser state after that
// prefix. Only the last grapheme of trailing ground-state text is held.
func holdStart(data []byte, start parseState) (int, parseState) {
	state := start
	tailStart := 0
	for i, b := range data {
		var boundary bool
		state, boundary, _ = advance(state, b)
		if boundary {
			tailStart = i + 1
		}
	}
	if !isTextState(state) || tailStart == len(data) {
		return len(data), state
	}

	tail := data[tailStart:]
	complete := completeUTF8Prefix(tail)
	hold := complete
	if complete > 0 {
		graphemes := uniseg.NewGraphemes(string(tail[:complete]))
		for graphemes.Next() {
			hold, _ = graphemes.Positions()
		}
	}
	if complete == 0 {
		hold = 0
	}
	if len(tail)-hold > maxHeldBytes {
		return len(data), state
	}
	end := tailStart + hold
	prefixState := start
	for _, b := range data[:end] {
		prefixState, _, _ = advance(prefixState, b)
	}
	return end, prefixState
}

func isTextState(state parseState) bool {
	return state == stateGround || state == stateUTF8One || state == stateUTF8Two || state == stateUTF8Three
}

func completeUTF8Prefix(p []byte) int {
	for i := 0; i < len(p); {
		if !utf8.FullRune(p[i:]) {
			return i
		}
		_, size := utf8.DecodeRune(p[i:])
		i += size
	}
	return len(p)
}

func (e *Emulator) feed(data []byte, start parseState) {
	state := start
	var controls bytes.Buffer
	printableStart := -1
	flushControls := func() {
		e.writeTerm(controls.Bytes())
		controls.Reset()
	}
	flushPrintable := func(end int) {
		if printableStart >= 0 {
			e.feedPrintable(data[printableStart:end])
			printableStart = -1
		}
	}
	for i, b := range data {
		before := state
		var printable bool
		state, _, printable = advance(state, b)

		translateCSIU := e.trackCSI(before, state, b)
		if before == stateEscape && state == stateGround && b == 'c' {
			e.leftRightMargins = false
			e.syncOpen = false
		}
		if printable {
			flushControls()
			if printableStart < 0 {
				printableStart = i
			}
			continue
		}
		flushPrintable(i)
		// x/vt v0.1.0 implements CSI s but not CSI u. Once x/vt is in
		// the CSI parser state, ESC 8 restores the same saved cursor.
		if translateCSIU {
			controls.WriteString("\x1b8")
		} else {
			controls.WriteByte(b)
		}
	}
	flushPrintable(len(data))
	flushControls()
}

func isCSIFinal(b byte) bool { return b >= 0x40 && b <= 0x7e }

func isCSIState(state parseState) bool {
	return state == stateCSIEntry || state == stateCSIParam || state == stateCSIIntermediate
}

func (e *Emulator) trackCSI(before, after parseState, b byte) bool {
	translate := isCSIState(before) && isCSIFinal(b) && b == 'u' && e.csi.empty && !e.leftRightMargins
	entered := after == stateCSIEntry && (!isCSIState(before) || b == 0x9b)
	if entered {
		e.csi = newCSITracker()
		return translate
	}
	if !isCSIState(before) {
		return translate
	}
	switch {
	case isCSIFinal(b):
		e.finishCSI(b)
	case isCSIState(after):
		e.csi.collect(b)
	default:
		e.csi = csiTracker{}
	}
	return translate
}

func (e *Emulator) finishCSI(final byte) {
	e.csi.finishParam()
	if e.csi.private && e.csi.modeGrammar && (final == 'h' || final == 'l') {
		enabled := final == 'h'
		if e.csi.has69 {
			e.leftRightMargins = enabled
		}
		if e.csi.has2026 {
			e.syncOpen = enabled
		}
	}
	e.csi = csiTracker{}
}

func (e *Emulator) feedPrintable(p []byte) {
	graphemes := uniseg.NewGraphemes(string(p))
	last := 0
	for graphemes.Next() {
		width := graphemes.Width()
		if width <= 1 {
			continue
		}
		from, to := graphemes.Positions()
		e.writeTerm(p[last:from])
		cursor := e.term.CursorPosition()
		if cursor.X+width > e.term.Width() {
			e.writeTerm([]byte("\r\n"))
		}
		e.writeTerm(p[from:to])
		last = to
	}
	e.writeTerm(p[last:])
}

// normaliseUTF8 keeps x/vt's printable-text result independent of Write
// boundaries. Bytes inside terminal control sequences remain exact because an
// invalid byte there can cancel a sequence instead of becoming text. Write
// preserves a valid incomplete text suffix; Flush resolves it to U+FFFD.
func normaliseUTF8(p []byte, start parseState, preserveIncomplete bool) []byte {
	if utf8.Valid(p) {
		return p
	}
	out := make([]byte, 0, len(p))
	state := start
	for pos := 0; pos < len(p); {
		b := p[pos]
		if state != stateGround {
			out = append(out, b)
			state, _, _ = advance(state, b)
			pos++
			continue
		}
		if b < utf8.RuneSelf {
			out = append(out, b)
			state, _, _ = advance(state, b)
			pos++
			continue
		}
		// Raw C1 bytes are terminal controls, including the required 8-bit
		// CSI form. They are not malformed printable UTF-8.
		if b >= 0x80 && b <= 0x9f {
			out = append(out, b)
			state, _, _ = advance(state, b)
			pos++
			continue
		}
		remaining := p[pos:]
		if preserveIncomplete && !utf8.FullRune(remaining) {
			out = append(out, remaining...)
			break
		}
		r, size := utf8.DecodeRune(remaining)
		if r == utf8.RuneError && size == 1 {
			from := len(out)
			out = utf8.AppendRune(out, utf8.RuneError)
			for _, replacement := range out[from:] {
				state, _, _ = advance(state, replacement)
			}
			pos++
			continue
		}
		out = append(out, remaining[:size]...)
		for _, printable := range remaining[:size] {
			state, _, _ = advance(state, printable)
		}
		pos += size
	}
	return out
}

// advance is a small ECMA-48 boundary tracker. x/vt remains the parser of
// record; this only identifies trailing ground-state printable text.
func advance(state parseState, b byte) (parseState, bool, bool) {
	switch state {
	case stateUTF8One:
		return stateGround, false, true
	case stateUTF8Two:
		return stateUTF8One, false, true
	case stateUTF8Three:
		return stateUTF8Two, false, true
	case stateOSC:
		switch b {
		case 0x07, 0x18, 0x1a, 0x9c:
			return stateGround, true, false
		case 0x1b:
			return stateEscape, false, false
		default:
			return stateOSC, false, false
		}
	case stateDCSData:
		switch b {
		case 0x18, 0x1a, 0x9c:
			return stateGround, true, false
		case 0x1b:
			return stateEscape, false, false
		default:
			return stateDCSData, false, false
		}
	case stateDCSEntry:
		// x/ansi accepts ESC as the first DCS data byte for terminal
		// passthrough sequences instead of starting an escape sequence.
		if b == 0x1b {
			return stateDCSData, false, false
		}
	case stateAPC, statePM, stateSOS:
		switch b {
		case 0x18, 0x1a, 0x9c:
			return stateGround, true, false
		case 0x1b:
			return stateEscape, false, false
		}
		if b <= 0x7f {
			return state, false, false
		}
	case stateGround, stateEscape, stateEscapeIntermediate,
		stateCSIEntry, stateCSIParam, stateCSIIntermediate,
		stateDCSParam, stateDCSIntermediate:
	}

	// These transitions apply from every non-data state in x/ansi's parser.
	switch b {
	case 0x18, 0x1a, 0x9c:
		return stateGround, true, false
	case 0x1b:
		return stateEscape, false, false
	case 0x90:
		return stateDCSEntry, false, false
	case 0x98:
		return stateSOS, false, false
	case 0x9b:
		return stateCSIEntry, false, false
	case 0x9d:
		return stateOSC, false, false
	case 0x9e:
		return statePM, false, false
	case 0x9f:
		return stateAPC, false, false
	}
	if b >= 0x80 && b <= 0x97 || b == 0x99 || b == 0x9a {
		return stateGround, true, false
	}
	if next, ok := utf8StartState(b); ok {
		return next, false, true
	}

	switch state {
	case stateGround:
		switch {
		case b < 0x20 || b == 0x7f:
			return stateGround, true, false
		case b >= 0x20 && b <= 0x7e:
			return stateGround, false, true
		default:
			return stateGround, true, false
		}
	case stateEscape:
		switch b {
		case '[':
			return stateCSIEntry, false, false
		case ']':
			return stateOSC, false, false
		case 'P':
			return stateDCSEntry, false, false
		case '_':
			return stateAPC, false, false
		case '^':
			return statePM, false, false
		case 'X':
			return stateSOS, false, false
		default:
			switch {
			case b < 0x20 || b == 0x7f:
				return stateEscape, false, false
			case b >= 0x20 && b <= 0x2f:
				return stateEscapeIntermediate, false, false
			default:
				return stateGround, true, false
			}
		}
	case stateEscapeIntermediate:
		switch {
		case b < 0x20 || b == 0x7f:
			return stateEscapeIntermediate, false, false
		case b >= 0x20 && b <= 0x2f:
			return stateEscapeIntermediate, false, false
		default:
			return stateGround, true, false
		}
	case stateCSIEntry:
		switch {
		case b < 0x20 || b == 0x7f:
			return stateCSIEntry, false, false
		case b >= 0x20 && b <= 0x2f:
			return stateCSIIntermediate, false, false
		case b >= 0x30 && b <= 0x3f:
			return stateCSIParam, false, false
		default:
			return stateGround, true, false
		}
	case stateCSIParam:
		switch {
		case b < 0x20 || b == 0x7f:
			return stateCSIParam, false, false
		case b >= 0x20 && b <= 0x2f:
			return stateCSIIntermediate, false, false
		case b >= 0x30 && b <= 0x3f:
			return stateCSIParam, false, false
		default:
			return stateGround, true, false
		}
	case stateCSIIntermediate:
		if b < 0x30 || b == 0x7f {
			return stateCSIIntermediate, false, false
		}
		return stateGround, true, false
	case stateDCSEntry:
		switch {
		case b >= 0x08 && b <= 0x0d || isCSIFinal(b):
			return stateDCSData, false, false
		case b < 0x20 || b == 0x7f:
			return stateDCSEntry, false, false
		case b >= 0x20 && b <= 0x2f:
			return stateDCSIntermediate, false, false
		case b >= 0x30 && b <= 0x3f:
			return stateDCSParam, false, false
		default:
			return stateGround, true, false
		}
	case stateDCSParam:
		switch {
		case b < 0x20 || b == 0x7f:
			return stateDCSParam, false, false
		case b >= 0x20 && b <= 0x2f:
			return stateDCSIntermediate, false, false
		case b >= 0x30 && b <= 0x3f:
			return stateDCSParam, false, false
		case isCSIFinal(b):
			return stateDCSData, false, false
		default:
			return stateGround, true, false
		}
	case stateDCSIntermediate:
		switch {
		case b < 0x30 || b == 0x7f:
			return stateDCSIntermediate, false, false
		case b >= 0x30 && b <= 0x7e:
			return stateDCSData, false, false
		default:
			return stateGround, true, false
		}
	default:
		return stateGround, true, false
	}
}

func utf8StartState(b byte) (parseState, bool) {
	switch {
	case b >= 0xc2 && b <= 0xdf:
		return stateUTF8One, true
	case b >= 0xe0 && b <= 0xef:
		return stateUTF8Two, true
	case b >= 0xf0 && b <= 0xf4:
		return stateUTF8Three, true
	default:
		return stateGround, false
	}
}
