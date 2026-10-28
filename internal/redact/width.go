package redact

import (
	"unicode"

	"golang.org/x/text/width"
)

// CellWidth sums the v1 width model over s: 0 for C0, DEL, C1 and the
// Mn, Me and Cf categories (combining marks, ZWJ, BOM); 2 for East Asian
// Wide and Fullwidth runes; 1 otherwise, East Asian Ambiguous included.
// There is no grapheme segmentation: the model is a precondition on the
// input grid, not a promise about what a terminal draws.
func CellWidth(s string) int {
	n := 0
	for _, r := range s {
		n += runeWidth(r)
	}
	return n
}

// runeWidth is the v1 width of one rune.
func runeWidth(r rune) int {
	if unicode.IsControl(r) {
		return 0
	}
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || unicode.Is(unicode.Cf, r) {
		return 0
	}
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return 2
	default:
		return 1
	}
}

// isClusterForming reports whether r starts or continues a grapheme cluster
// a per-rune sum cannot size: ZWJ, variation selectors, regional
// indicators, skin tones, tags and the keycap mark.
func isClusterForming(r rune) bool {
	switch {
	case r == 0x200d:
		return true
	case r >= 0xfe00 && r <= 0xfe0f:
		return true
	case r >= 0xe0100 && r <= 0xe01ef:
		return true
	case r >= 0x1f1e6 && r <= 0x1f1ff:
		return true
	case r >= 0x1f3fb && r <= 0x1f3ff:
		return true
	case r >= 0xe0020 && r <= 0xe007f:
		return true
	case r == 0x20e3:
		return true
	default:
		return false
	}
}

// isExtenderAfter reports whether r, right after a masked span, extends its
// last cluster: the ZWJ, selectors, tones and tags, and the keycap mark.
// Regional indicators are excluded on purpose: a flag next to a span is
// copied as it is.
func isExtenderAfter(r rune) bool {
	switch {
	case r == 0x200d:
		return true
	case r >= 0xfe00 && r <= 0xfe0f:
		return true
	case r >= 0xe0100 && r <= 0xe01ef:
		return true
	case r >= 0x1f3fb && r <= 0x1f3ff:
		return true
	case r >= 0xe0020 && r <= 0xe007f:
		return true
	case r == 0x20e3:
		return true
	default:
		return false
	}
}

// isBlankRune reports the runes a soft wrap may end on: the space and the
// box drawing and block elements, so a TUI frame in the last column never
// joins two rows into one logical line.
func isBlankRune(r rune) bool {
	return r == 0x20 || (r >= 0x2500 && r <= 0x259f)
}
