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
	stateCSI
	stateDCS
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
	empty, private, has69, haveParam bool
	param                            int
}

func newCSITracker() csiTracker { return csiTracker{empty: true} }

func (t *csiTracker) collect(b byte) {
	switch {
	case b < 0x20 || b == 0x7f:
		return
	case b == '?' && t.empty:
		t.empty = false
		t.private = true
	case b >= '0' && b <= '9':
		t.empty = false
		t.haveParam = true
		if t.param <= 1_000 {
			t.param = t.param*10 + int(b-'0')
		}
	case b == ';':
		t.empty = false
		t.finishParam()
	default:
		t.empty = false
	}
}

func (t *csiTracker) finishParam() {
	if t.private && t.haveParam && t.param == 69 {
		t.has69 = true
	}
	t.param = 0
	t.haveParam = false
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
	return tailStart + hold, stateGround
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

func (e *Emulator) trackCSI(before, after parseState, b byte) bool {
	translate := before == stateCSI && isCSIFinal(b) && b == 'u' && e.csi.empty && !e.leftRightMargins
	entered := after == stateCSI && (before != stateCSI || b == 0x9b)
	if entered {
		e.csi = newCSITracker()
		return translate
	}
	if before != stateCSI {
		return translate
	}
	switch {
	case isCSIFinal(b):
		e.finishCSI(b)
	case after == stateCSI:
		e.csi.collect(b)
	default:
		e.csi = csiTracker{}
	}
	return translate
}

func (e *Emulator) finishCSI(final byte) {
	e.csi.finishParam()
	mode69 := e.csi.private && e.csi.has69 && (final == 'h' || final == 'l')
	if mode69 {
		e.leftRightMargins = final == 'h'
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
	case stateGround, stateEscape, stateCSI, stateDCS:
	}

	// These transitions apply from every non-data state in x/ansi's parser.
	switch b {
	case 0x18, 0x1a, 0x9c:
		return stateGround, true, false
	case 0x1b:
		return stateEscape, false, false
	case 0x90:
		return stateDCS, false, false
	case 0x98:
		return stateSOS, false, false
	case 0x9b:
		return stateCSI, false, false
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
			return stateCSI, false, false
		case ']':
			return stateOSC, false, false
		case 'P':
			return stateDCS, false, false
		case '_':
			return stateAPC, false, false
		case '^':
			return statePM, false, false
		case 'X':
			return stateSOS, false, false
		default:
			if b >= 0x30 && b <= 0x7e {
				return stateGround, true, false
			}
			return stateEscape, false, false
		}
	case stateCSI:
		if isCSIFinal(b) {
			return stateGround, true, false
		}
		return stateCSI, false, false
	case stateDCS:
		if isCSIFinal(b) {
			return stateDCSData, false, false
		}
		return stateDCS, false, false
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
