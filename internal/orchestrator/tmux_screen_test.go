//go:build linux

package orchestrator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTmuxScreenRowsStripsSingleCapture pins the single-capture cut: the
// rows the analysis sees and every screen comparison hashes come from the
// same escape-coded capture, with the sequences stripped and the line
// breaks kept.
func TestTmuxScreenRowsStripsSingleCapture(t *testing.T) {
	t.Parallel()
	// The idle fixture screen is the reference content: the cut must
	// recover its rows with and without escape sequences.
	idleRows := strings.Split(tmuxContractIdleScreen, "\n")
	cases := map[string]struct {
		screen []byte
		rows   []string
	}{
		"plain": {
			screen: []byte(tmuxContractIdleScreen),
			rows:   idleRows,
		},
		"sgr color": {
			screen: []byte("\x1b[32mcompleted\x1b[0m fixture task\n$ "),
			rows:   idleRows,
		},
		"256 color and cursor motion": {
			screen: []byte("\x1b[38;2;128;128;128mdim\x1b[0m\x1b[24;80H\n$ "),
			rows:   []string{"dim", "$ "},
		},
		"osc hyperlink": {
			screen: []byte("\x1b]8;;https://example.invalid\x1b\\link\x1b]8;;\x1b\\ text\n$ "),
			rows:   []string{"link text", "$ "},
		},
		"osc terminated by bel": {
			screen: []byte("\x1b]0;title\x07plain\n$ "),
			rows:   []string{"plain", "$ "},
		},
		"charset select": {
			screen: []byte("\x1b(0line\x1b(B\n$ "),
			rows:   []string{"line", "$ "},
		},
		"crlf": {
			screen: []byte("first\r\nsecond\r\n$ "),
			rows:   []string{"first", "second", "$ "},
		},
		"utf8 survives": {
			screen: []byte("tarea \xe2\x9c\x93 hecha\n$ "),
			rows:   []string{"tarea ✓ hecha", "$ "},
		},
		"truncated sequence": {
			screen: []byte("text\x1b[38;2;1"),
			rows:   []string{"text"},
		},
		"bare escape at end": {
			screen: []byte("text\x1b"),
			rows:   []string{"text"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.rows, tmuxScreenRows(tc.screen))
		})
	}
}
