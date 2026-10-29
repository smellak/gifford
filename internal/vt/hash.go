package vt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// Hash returns a versioned, normalised digest of the visible screen.
func Hash(s Snapshot) protocol.Digest {
	rows := make([]string, len(s.Lines))
	for i, line := range s.Lines {
		rows[i] = normalise(line)
	}
	mode := "main"
	if s.AltScreen {
		mode = "alt"
	}
	text := "gifford-vt-hash-v1\n" +
		fmt.Sprintf("%dx%d\n", s.Cols, s.Rows) +
		mode + "\n" + strings.Join(rows, "\n")
	sum := sha256.Sum256([]byte(text))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

func normalise(line string) string {
	var out strings.Builder
	for _, r := range line {
		switch {
		case r >= '0' && r <= '9':
			out.WriteByte('0')
		case spinnerRune(r):
			out.WriteByte('*')
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}

func spinnerRune(r rune) bool {
	if r >= '\u2800' && r <= '\u28ff' ||
		r >= '\u25d0' && r <= '\u25d3' ||
		r >= '\u25f0' && r <= '\u25f7' {
		return true
	}
	switch r {
	case '\u2722', '\u2733', '\u2736', '\u2738', '\u2739', '\u273a', '\u273b', '\u273d':
		return true
	default:
		return false
	}
}
