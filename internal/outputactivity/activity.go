// Package outputactivity classifies one raw PTY read for ANALYZE activity.
// It is deliberately narrower than a terminal emulator: only control
// sequences whose idempotence can be proved from small, explicit state may be
// ignored. Every byte still belongs to the raw output path.
package outputactivity

import (
	"bytes"
	"strconv"
)

const (
	// Version is the activity input produced from preserved PTY-read
	// boundaries and the conservative stateful classifier below.
	Version = "effective-read-v2"
	// LegacyVersion names recordings whose activity was derived from the
	// time and byte count of aggregated io.chunk events without filtering.
	LegacyVersion = "raw-chunk-v1"
	// ReconstructedVersion applies Detector to each aggregated io.chunk of
	// an older capture. It recovers what the raw stream proves, but cannot
	// recover the original PTY-read boundaries or timestamps.
	ReconstructedVersion = "effective-chunk-v1"
)

// ValidVersion reports an activity input version this binary can replay.
func ValidVersion(v string) bool {
	return v == Version || v == ReconstructedVersion || v == LegacyVersion
}

// Detector retains only the terminal state needed to prove that a covered
// sequence is inert. Its zero value starts at the terminal defaults.
type Detector struct {
	initialized          bool
	renditionKnown       bool
	rendition            rendition
	bracketedPasteKnown  bool
	bracketedPaste       bool
	modifyOtherKeysKnown bool
	modifyOtherKeys      uint8
	lastChanged          bool
}

// Observe reports whether one complete raw PTY read is effective ANALYZE
// activity. A read is inert only when every byte belongs to a covered,
// complete control sequence and none changes state along the way. A change
// remains activity even when a later sequence in the same read restores the
// starting state.
func (d *Detector) Observe(read []byte) bool {
	d.init()
	effective := false
	for i := 0; i < len(read); {
		switch read[i] {
		case 0x1b:
			if i+1 >= len(read) {
				d.invalidate()
				return true
			}
			switch read[i+1] {
			case '[':
				end, params, intermediates, final, ok := scanCSI(read, i+2)
				if !ok {
					d.invalidate()
					return true
				}
				if !d.observeCSI(params, intermediates, final) {
					d.invalidate()
					effective = true
				} else if d.lastChanged {
					effective = true
				}
				i = end
			case ']', 'P', '^', '_':
				end, ok := scanStringControl(read, i+2)
				d.invalidate()
				effective = true
				if !ok {
					return true
				}
				i = end
			default:
				// All other ESC forms are outside the proved subset. Consume
				// the introducer and its next byte; later covered sequences
				// may re-establish known state.
				d.invalidate()
				effective = true
				i += 2
			}
		case 0x9b:
			end, params, intermediates, final, ok := scanCSI(read, i+1)
			if !ok {
				d.invalidate()
				return true
			}
			if !d.observeCSI(params, intermediates, final) {
				d.invalidate()
				effective = true
			} else if d.lastChanged {
				effective = true
			}
			i = end
		default:
			// Printable bytes, C0 controls and unhandled C1 controls are
			// activity. Ordinary text and line controls do not themselves
			// make the tracked rendition or bracketed-paste state unknown.
			if read[i] >= 0x80 && read[i] <= 0x9f {
				d.invalidate()
			}
			effective = true
			i++
		}
	}
	return effective
}

// lastChanged is scratch set by observeCSI. It lives outside the semantic
// state and is overwritten for every complete CSI.
func (d *Detector) observeCSI(params, intermediates []byte, final byte) bool {
	d.lastChanged = false
	if len(intermediates) != 0 {
		return false
	}
	if final == 'm' && sgrParams(params) {
		d.lastChanged = d.observeSGR(params)
		return true
	}
	if final == 'm' {
		if level, ok := modifyOtherKeysParam(params); ok {
			if !d.modifyOtherKeysKnown {
				d.lastChanged = true
			} else {
				d.lastChanged = d.modifyOtherKeys != level
			}
			d.modifyOtherKeys, d.modifyOtherKeysKnown = level, true
			return true
		}
	}
	if bytes.Equal(params, []byte("?2004")) && (final == 'h' || final == 'l') {
		next := final == 'h'
		if !d.bracketedPasteKnown {
			d.lastChanged = true
		} else {
			d.lastChanged = d.bracketedPaste != next
		}
		d.bracketedPaste, d.bracketedPasteKnown = next, true
		return true
	}
	return false
}

func (d *Detector) init() {
	if d.initialized {
		return
	}
	d.initialized = true
	d.renditionKnown = true
	d.bracketedPasteKnown = true
	d.modifyOtherKeysKnown = true
}

func (d *Detector) invalidate() {
	d.renditionKnown = false
	d.bracketedPasteKnown = false
	d.modifyOtherKeysKnown = false
}

// modifyOtherKeysParam recognizes xterm's modifyOtherKeys assignment. The
// Antigravity CLI periodically reasserts level 2 next to bracketed paste.
// A fresh terminal starts at level 0; only the three specified levels are
// covered, so every other private CSI remains conservative activity.
func modifyOtherKeysParam(params []byte) (uint8, bool) {
	if len(params) != 4 || params[0] != '>' || params[1] != '4' || params[2] != ';' {
		return 0, false
	}
	if params[3] < '0' || params[3] > '2' {
		return 0, false
	}
	return params[3] - '0', true
}

// color is one SGR color: zero is the default, one an indexed color and two
// an RGB color.
type color struct {
	kind    uint8
	r, g, b int
}

type rendition struct {
	bold, faint, italic      bool
	underline, blink         uint8
	inverse, conceal, strike bool
	framed, encircled        bool
	overline, fraktur        bool
	proportional             bool
	font, ideogram           uint8
	script                   int8
	fg, bg, underlineColor   color
}

func sgrParams(p []byte) bool {
	for _, b := range p {
		if (b < '0' || b > '9') && b != ';' {
			return false
		}
	}
	return true
}

func splitParams(raw []byte) ([]int, bool) {
	if len(raw) == 0 {
		return []int{0}, true
	}
	parts := bytes.Split(raw, []byte{';'})
	out := make([]int, len(parts))
	for i, p := range parts {
		if len(p) == 0 {
			out[i] = 0
			continue
		}
		n, err := strconv.Atoi(string(p))
		if err != nil || n < 0 || n > 65535 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

func (d *Detector) observeSGR(raw []byte) bool {
	params, ok := splitParams(raw)
	if !ok {
		d.renditionKnown = false
		return true
	}
	known := d.renditionKnown
	r := d.rendition
	changed := !known
	set := func(dst any, value any) bool {
		switch p := dst.(type) {
		case *bool:
			v := value.(bool)
			if *p == v {
				return false
			}
			*p = v
		case *uint8:
			v := value.(uint8)
			if *p == v {
				return false
			}
			*p = v
		case *int8:
			v := value.(int8)
			if *p == v {
				return false
			}
			*p = v
		case *color:
			v := value.(color)
			if *p == v {
				return false
			}
			*p = v
		}
		return true
	}
	unsupported := false
	for i := 0; i < len(params); i++ {
		p := params[i]
		if !known && p != 0 {
			// A partial change cannot establish the attributes not named.
			continue
		}
		switch {
		case p == 0:
			if known && r != (rendition{}) {
				changed = true
			}
			r, known = rendition{}, true
		case p == 1:
			changed = set(&r.bold, true) || changed
		case p == 2:
			changed = set(&r.faint, true) || changed
		case p == 3:
			changed = set(&r.italic, true) || changed
		case p == 4:
			changed = set(&r.underline, uint8(1)) || changed
		case p == 5 || p == 6:
			changed = set(&r.blink, uint8(p-4)) || changed
		case p == 7:
			changed = set(&r.inverse, true) || changed
		case p == 8:
			changed = set(&r.conceal, true) || changed
		case p == 9:
			changed = set(&r.strike, true) || changed
		case p >= 10 && p <= 19:
			changed = set(&r.font, uint8(p-10)) || changed
		case p == 20:
			changed = set(&r.fraktur, true) || changed
		case p == 21:
			changed = set(&r.underline, uint8(2)) || changed
		case p == 22:
			changed = set(&r.bold, false) || changed
			changed = set(&r.faint, false) || changed
		case p == 23:
			changed = set(&r.italic, false) || changed
			changed = set(&r.fraktur, false) || changed
		case p == 24:
			changed = set(&r.underline, uint8(0)) || changed
		case p == 25:
			changed = set(&r.blink, uint8(0)) || changed
		case p == 26:
			changed = set(&r.proportional, true) || changed
		case p == 27:
			changed = set(&r.inverse, false) || changed
		case p == 28:
			changed = set(&r.conceal, false) || changed
		case p == 29:
			changed = set(&r.strike, false) || changed
		case p >= 30 && p <= 37:
			changed = set(&r.fg, color{kind: 1, r: p - 30}) || changed
		case p == 38 || p == 48 || p == 58:
			c, used, valid := extendedColor(params[i+1:])
			if !valid {
				unsupported = true
				break
			}
			i += used
			switch p {
			case 38:
				changed = set(&r.fg, c) || changed
			case 48:
				changed = set(&r.bg, c) || changed
			case 58:
				changed = set(&r.underlineColor, c) || changed
			}
		case p == 39:
			changed = set(&r.fg, color{}) || changed
		case p >= 40 && p <= 47:
			changed = set(&r.bg, color{kind: 1, r: p - 40}) || changed
		case p == 49:
			changed = set(&r.bg, color{}) || changed
		case p == 50:
			changed = set(&r.proportional, false) || changed
		case p == 51:
			changed = set(&r.framed, true) || changed
			changed = set(&r.encircled, false) || changed
		case p == 52:
			changed = set(&r.framed, false) || changed
			changed = set(&r.encircled, true) || changed
		case p == 53:
			changed = set(&r.overline, true) || changed
		case p == 54:
			changed = set(&r.framed, false) || changed
			changed = set(&r.encircled, false) || changed
		case p == 55:
			changed = set(&r.overline, false) || changed
		case p == 59:
			changed = set(&r.underlineColor, color{}) || changed
		case p >= 60 && p <= 64:
			changed = set(&r.ideogram, uint8(p-59)) || changed
		case p == 65:
			changed = set(&r.ideogram, uint8(0)) || changed
		case p == 73:
			changed = set(&r.script, int8(1)) || changed
		case p == 74:
			changed = set(&r.script, int8(-1)) || changed
		case p == 75:
			changed = set(&r.script, int8(0)) || changed
		case p >= 90 && p <= 97:
			changed = set(&r.fg, color{kind: 1, r: p - 90 + 8}) || changed
		case p >= 100 && p <= 107:
			changed = set(&r.bg, color{kind: 1, r: p - 100 + 8}) || changed
		default:
			unsupported = true
		}
		if unsupported {
			break
		}
	}
	if unsupported {
		d.renditionKnown = false
		return true
	}
	d.rendition, d.renditionKnown = r, known
	return changed
}

func extendedColor(p []int) (color, int, bool) {
	if len(p) >= 2 && p[0] == 5 && p[1] >= 0 && p[1] <= 255 {
		return color{kind: 1, r: p[1]}, 2, true
	}
	if len(p) >= 4 && p[0] == 2 {
		for _, n := range p[1:4] {
			if n < 0 || n > 255 {
				return color{}, 0, false
			}
		}
		return color{kind: 2, r: p[1], g: p[2], b: p[3]}, 4, true
	}
	return color{}, 0, false
}

// scanCSI returns the index after one complete CSI. start is the first byte
// after the introducer.
func scanCSI(b []byte, start int) (end int, params, intermediates []byte, final byte, ok bool) {
	i := start
	paramStart := i
	for i < len(b) && b[i] >= 0x30 && b[i] <= 0x3f {
		i++
	}
	params = b[paramStart:i]
	intermediateStart := i
	for i < len(b) && b[i] >= 0x20 && b[i] <= 0x2f {
		i++
	}
	intermediates = b[intermediateStart:i]
	if i >= len(b) || b[i] < 0x40 || b[i] > 0x7e {
		return 0, nil, nil, 0, false
	}
	return i + 1, params, intermediates, b[i], true
}

// scanStringControl consumes OSC/DCS/PM/APC through BEL or ST. BEL ends OSC
// in practice; accepting it for the other strings only makes the scanner
// stop earlier, never makes the read inert because these controls are always
// effective.
func scanStringControl(b []byte, start int) (int, bool) {
	for i := start; i < len(b); i++ {
		if b[i] == 0x07 {
			return i + 1, true
		}
		if b[i] == 0x9c {
			return i + 1, true
		}
		if b[i] == 0x1b && i+1 < len(b) && b[i+1] == '\\' {
			return i + 2, true
		}
	}
	return 0, false
}
