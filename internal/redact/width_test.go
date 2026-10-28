package redact_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestCellWidth pins the v1 width model: controls and combining marks are
// 0, wide runes are 2, everything else (Ambiguous included) is 1. The
// expectations are the model's arithmetic, not what a terminal draws.
func TestCellWidth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"ascii", "hello", 5},
		{"cjk", "密码", 4},
		{"combining", "e\u0301", 1},
		{"ambiguous", "•│→", 3},
		{"zwj", "\u200d", 0},
		{"escape", "\x1b", 0},
		{"del", "\x7f", 0},
		{"c1", "\u0085", 0},
		{"woman zwj laptop", "\U0001F469\u200D\U0001F4BB", 4},
		{"heart with vs16", "\u2764\uFE0F", 1},
		{"flag", "\U0001F1EA\U0001F1F8", 2},
		{"padded frame", "│ok│", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, redact.CellWidth(tc.input))
		})
	}
}
