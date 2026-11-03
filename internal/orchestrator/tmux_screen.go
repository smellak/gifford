package orchestrator

import "strings"

// tmuxScreenRows cuts one escape-coded pane capture into visible rows. The
// analyst route holds a single capture per window, so the rows the analysis
// sees and the rows every screen comparison hashes come from the same
// bytes: the escape-coded capture feeds the detector, and this cut feeds
// the submission and the projection. Only 7-bit sequences are stripped,
// which is what the pane dump carries; anything else passes through so both
// ends keep comparing the same bytes.
func tmuxScreenRows(screen []byte) []string {
	parts := strings.Split(string(tmuxStripScreen(screen)), "\n")
	for i := range parts {
		parts[i] = strings.TrimSuffix(parts[i], "\r")
	}
	return parts
}

// tmuxStripScreen removes terminal escape sequences from a pane dump,
// leaving visible cells and line breaks. Multibyte text passes through
// untouched: only ASCII introducers are special.
func tmuxStripScreen(screen []byte) []byte {
	out := make([]byte, 0, len(screen))
	i := 0
	for i < len(screen) {
		if screen[i] != 0x1b {
			out = append(out, screen[i])
			i++
			continue
		}
		i++
		if i >= len(screen) {
			break
		}
		switch screen[i] {
		case '[':
			// CSI: parameters and intermediates up to the final byte.
			i++
			for i < len(screen) && (screen[i] < 0x40 || screen[i] > 0x7e) {
				i++
			}
			if i < len(screen) {
				i++
			}
		case ']', 'P', 'X', '^', '_':
			// OSC, DCS, SOS, PM and APC: data up to BEL or ESC backslash.
			i++
			for i < len(screen) {
				if screen[i] == 0x07 {
					i++
					break
				}
				if screen[i] == 0x1b && i+1 < len(screen) && screen[i+1] == '\\' {
					i += 2
					break
				}
				i++
			}
		case '(', ')', '*', '+', '#', '%', '/', '.', '-', '$':
			// Two-byte designators such as the charset selects.
			i += 2
			if i > len(screen) {
				i = len(screen)
			}
		default:
			// Single-byte functions: save, restore, line feeds, modes.
			i++
		}
	}
	return out
}
