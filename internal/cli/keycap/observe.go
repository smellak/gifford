package keycap

import (
	"bytes"
	"strings"
)

// observer scans the harness output for keyboard mode negotiation: the
// bracketed-paste switch and the extended keyboard pushes. It keeps no
// bytes beyond a short tail, so output content is never retained.
type observer struct {
	tail       []byte
	paste      *bool
	kitty      bool
	modifyKeys bool
}

// scan folds one output piece into the negotiation findings. The tail
// reassembles sequences split across reads; a split wider than the tail
// simply misses, and arrivals stay the ground truth.
func (o *observer) scan(data []byte) {
	window := append(append([]byte(nil), o.tail...), data...)
	hOn := bytes.LastIndex(window, []byte("\x1b[?2004h"))
	hOff := bytes.LastIndex(window, []byte("\x1b[?2004l"))
	if hOn >= 0 || hOff >= 0 {
		on := hOn > hOff
		o.paste = &on
	}
	for i := 0; i+4 <= len(window); i++ {
		if window[i] != 0x1b || window[i+1] != '[' || window[i+2] != '>' {
			continue
		}
		j := i + 3
		for j < len(window) && isDigit(window[j]) {
			j++
		}
		if j >= len(window) {
			continue
		}
		switch window[j] {
		case 'u':
			o.kitty = true
		case ';':
			head := string(window[i+3 : j])
			k := j + 1
			for k < len(window) && isDigit(window[k]) {
				k++
			}
			if head == "4" && k < len(window) && window[k] == 'm' {
				o.modifyKeys = true
			}
		}
	}
	if len(window) > 64 {
		window = window[len(window)-64:]
	}
	o.tail = append(o.tail[:0], window...)
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// describe sums up what the harness negotiated, for the closing report.
func (o *observer) describe() string {
	var parts []string
	switch {
	case o.paste == nil:
		parts = append(parts, "sin bracketed paste")
	case *o.paste:
		parts = append(parts, "bracketed paste")
	default:
		parts = append(parts, "bracketed paste desactivado")
	}
	switch {
	case o.kitty && o.modifyKeys:
		parts = append(parts, "teclado kitty y modifyOtherKeys")
	case o.kitty:
		parts = append(parts, "teclado kitty")
	case o.modifyKeys:
		parts = append(parts, "modifyOtherKeys")
	default:
		parts = append(parts, "sin teclado extendido")
	}
	return strings.Join(parts, ", ")
}
