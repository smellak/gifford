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
	stateOSC
	stateDCS
	stateAPC
	statePM
	stateSOS
	stateOSCEscape
	stateDCSEscape
	stateAPCEscape
	statePMEscape
	stateSOSEscape
)

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
	if state != stateGround || tailStart == len(data) {
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
		return len(data), stateGround
	}
	return tailStart + hold, stateGround
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
	csiEmpty := e.csiEmpty
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
		if before == stateEscape && b == '[' {
			csiEmpty = true
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
		if before == stateCSI && b == 'u' && csiEmpty {
			controls.WriteString("\x1b8")
		} else {
			controls.WriteByte(b)
		}
		if before == stateCSI {
			csiEmpty = false
		}
	}
	flushPrintable(len(data))
	flushControls()
	e.csiEmpty = csiEmpty
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
	case stateGround:
		switch {
		case b == 0x1b:
			return stateEscape, false, false
		case b < 0x20 || b == 0x7f:
			return stateGround, true, false
		default:
			return stateGround, false, true
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
		case 0x1b:
			return stateEscape, false, false
		default:
			if b >= 0x30 && b <= 0x7e {
				return stateGround, true, false
			}
			return stateEscape, false, false
		}
	case stateCSI:
		if b == 0x1b {
			return stateEscape, false, false
		}
		if b >= 0x40 && b <= 0x7e {
			return stateGround, true, false
		}
		return stateCSI, false, false
	case stateOSC:
		return advanceString(stateOSC, stateOSCEscape, b, true)
	case stateDCS:
		return advanceString(stateDCS, stateDCSEscape, b, false)
	case stateAPC:
		return advanceString(stateAPC, stateAPCEscape, b, false)
	case statePM:
		return advanceString(statePM, statePMEscape, b, false)
	case stateSOS:
		return advanceString(stateSOS, stateSOSEscape, b, false)
	case stateOSCEscape:
		return advanceStringEscape(stateOSC, stateOSCEscape, b)
	case stateDCSEscape:
		return advanceStringEscape(stateDCS, stateDCSEscape, b)
	case stateAPCEscape:
		return advanceStringEscape(stateAPC, stateAPCEscape, b)
	case statePMEscape:
		return advanceStringEscape(statePM, statePMEscape, b)
	case stateSOSEscape:
		return advanceStringEscape(stateSOS, stateSOSEscape, b)
	default:
		return stateGround, true, false
	}
}

func advanceString(base, escaped parseState, b byte, bell bool) (parseState, bool, bool) {
	if bell && b == 0x07 {
		return stateGround, true, false
	}
	if b == 0x1b {
		return escaped, false, false
	}
	return base, false, false
}

func advanceStringEscape(base, escaped parseState, b byte) (parseState, bool, bool) {
	if b == '\\' {
		return stateGround, true, false
	}
	if b == 0x1b {
		return escaped, false, false
	}
	return base, false, false
}
