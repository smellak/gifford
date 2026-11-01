package vt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testABC       = "abc"
	testABCD      = "abcd"
	testCombining = "éx"
	testHello     = "hello"
	testNew       = "new"
	testNone      = "none"
	testSame      = "same"
)

type corpusFixture struct {
	name                 string
	cols, rows           int
	input                string
	lines                []string
	cursorCol, cursorRow int
	checkCursor          bool
	altScreen            bool
	checkAlt             bool
	cursorVisible        bool
	checkCursorVisible   bool
}

var corpus = []corpusFixture{
	{name: "text-crlf-wrap", cols: 10, rows: 3, input: testHello + "\r\n0123456789AB", lines: []string{testHello, "0123456789", "AB"}, cursorCol: 2, cursorRow: 2, checkCursor: true},
	{name: "cup-ed-el", cols: 10, rows: 3, input: "aaaaaaaaaa\r\nbbbbbbbbbb\r\ncccccccccc\x1b[2;3H\x1b[K\x1b[3;1H\x1b[1K", lines: []string{"aaaaaaaaaa", "bb", " ccccccccc"}},
	{name: "ed2-clear", cols: 6, rows: 2, input: "xxxx\r\nyyyy\x1b[2J\x1b[Hz", lines: []string{"z", ""}},
	{name: "alt-screen-exit", cols: 8, rows: 2, input: "main\x1b[?1049h\x1b[Halt!\x1b[?1049l", lines: []string{"main", ""}, checkAlt: true},
	{name: "alt-screen-on", cols: 8, rows: 2, input: "main\x1b[?1049h\x1b[H\x1b[2Jalt!", lines: []string{"alt!", ""}, altScreen: true, checkAlt: true},
	{name: "scroll-region", cols: 5, rows: 4, input: "top\r\n1\r\n2\r\nbot\x1b[2;3r\x1b[3;1H\n" + testNew + "\x1b[r", lines: []string{"top", "2", testNew, "bot"}},
	{name: "sgr-attrs", cols: 12, rows: 1, input: "\x1b[1;31mred\x1b[0m \x1b[38;5;208mo\x1b[38;2;1;2;3mt\x1b[m", lines: []string{"red ot"}},
	{name: "wide-cjk", cols: 6, rows: 1, input: "漢字ab", lines: []string{"漢字ab"}},
	{name: "wide-emoji", cols: 6, rows: 1, input: "a🙂b", lines: []string{"a🙂b"}},
	{name: "wide-cursor", cols: 8, rows: 1, input: "漢x\x1b[4GY", lines: []string{"漢xY"}, cursorCol: 4, checkCursor: true},
	{name: "wide-last-column", cols: 5, rows: 2, input: "abcd漢", lines: []string{testABCD, "漢"}},
	{name: "combining", cols: 6, rows: 1, input: testCombining, lines: []string{testCombining}, cursorCol: 2, checkCursor: true},
	{name: "box-utf8", cols: 6, rows: 3, input: "┌──┐\r\n│> │\r\n└──┘", lines: []string{"┌──┐", "│> │", "└──┘"}},
	{name: "dec-graphics", cols: 6, rows: 1, input: "\x1b(0lqqk\x1b(B", lines: []string{"┌──┐"}},
	{name: "il-dl", cols: 4, rows: 3, input: "a\r\nb\r\nc\x1b[2;1H\x1b[L", lines: []string{"a", "", "b"}},
	{name: "ich-dch", cols: 6, rows: 1, input: "abcd\x1b[2G\x1b[2@\x1b[1P", lines: []string{"a bcd"}},
	{name: "decsc-decrc", cols: 6, rows: 2, input: "ab\x1b7\r\nzz\x1b8c", lines: []string{testABC, "zz"}, cursorCol: 3, checkCursor: true},
	{name: "csi-s-u", cols: 6, rows: 2, input: "ab\x1b[s\r\nzz\x1b[uc", lines: []string{testABC, "zz"}, cursorCol: 3, checkCursor: true},
	{name: "osc-title", cols: 10, rows: 1, input: "\x1b]0;my title\x07a\x1b]2;t2\x1b\\b", lines: []string{"ab"}},
	{name: "osc-title-claude-bel", cols: 20, rows: 1, input: "\x1b]0;✳ text\x07\x1b[H❯\u00a0", lines: []string{"❯\u00a0"}, cursorCol: 2, checkCursor: true},
	{name: "osc-title-claude-st", cols: 20, rows: 1, input: "\x1b]0;✳ text\x1b\\\x1b[H❯\u00a0", lines: []string{"❯\u00a0"}, cursorCol: 2, checkCursor: true},
	{name: "osc-title-check", cols: 10, rows: 1, input: "ab\x1b]0;✓ text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-quote", cols: 10, rows: 1, input: "ab\x1b]0;“ text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-u-diaeresis", cols: 10, rows: 1, input: "ab\x1b]0;Ü text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-c1-rune", cols: 10, rows: 1, input: "ab\x1b]0;\u009c text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-lone-st", cols: 10, rows: 1, input: "ab\x1b]0;text\x9ccd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-e-acute", cols: 10, rows: 1, input: "ab\x1b]0;é text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-white-star", cols: 10, rows: 1, input: "ab\x1b]0;❂ text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc-title-braille", cols: 10, rows: 1, input: "ab\x1b]0;⠐ text\x07cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "dcs-utf8-c1-continuation", cols: 10, rows: 1, input: "ab\x1bPq✳ text\x1b\\cd", lines: []string{testABCD}, cursorCol: 4, checkCursor: true},
	{name: "osc8-link", cols: 10, rows: 1, input: "\x1b]8;;https://example.invalid\x1b\\link\x1b]8;;\x1b\\", lines: []string{"link"}},
	{name: "osc52", cols: 10, rows: 1, input: "\x1b]52;c;aGVsbG8=\x07ok", lines: []string{"ok"}},
	{name: "modes-ignored", cols: 10, rows: 1, input: "\x1b[?2004h\x1b[?2026h\x1b[?1004h\x1b[?1000h\x1b[?1006h\x1b[>1u\x1b[?25lok\x1b[?2026l\x1b[<u", lines: []string{"ok"}, cursorCol: 2, checkCursor: true, checkCursorVisible: true},
	{name: "tabs", cols: 20, rows: 1, input: "a\tb\tc", lines: []string{"a       b       c"}},
	{name: "queries", cols: 10, rows: 1, input: "\x1b[6n\x1b[c\x1b[>c\x1b[?2048hok", lines: []string{"ok"}},
	{name: "invalid-utf8", cols: 10, rows: 1, input: "a\xffb\xc3"},
	{name: "huge-params", cols: 10, rows: 2, input: "\x1b[99999;99999H\x1b[999999999999A\x1b[" + strings.Repeat("9;", 500) + "mok"},
	{name: "ris", cols: 6, rows: 2, input: "xx\r\nyy\x1bcz", lines: []string{"z", ""}},
	{name: "pending-wrap", cols: 5, rows: 2, input: "abcde\rX", lines: []string{"Xbcde", ""}, cursorCol: 1, checkCursor: true},
	{name: "spinner-clock", cols: 20, rows: 1, input: "⠋ Working 12:34:56", lines: []string{"⠋ Working 12:34:56"}},
}

func TestCorpus(t *testing.T) {
	t.Parallel()
	for _, fixture := range corpus {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			snapshot := render(t, fixture.cols, fixture.rows, []byte(fixture.input))
			if fixture.lines != nil {
				require.Equal(t, fixture.lines, snapshot.Lines)
			}
			if fixture.checkCursor {
				require.Equal(t, fixture.cursorCol, snapshot.CursorCol)
				require.Equal(t, fixture.cursorRow, snapshot.CursorRow)
			}
			if fixture.checkAlt {
				require.Equal(t, fixture.altScreen, snapshot.AltScreen)
			}
			if fixture.checkCursorVisible {
				require.Equal(t, fixture.cursorVisible, snapshot.CursorVisible)
			}
		})
	}
}

func render(t *testing.T, cols, rows int, writes ...[]byte) Snapshot {
	t.Helper()
	emulator, err := New(cols, rows)
	require.NoError(t, err)
	for _, write := range writes {
		n, writeErr := emulator.Write(write)
		require.NoError(t, writeErr)
		require.Len(t, write, n)
	}
	require.NoError(t, emulator.Flush())
	snapshot := emulator.Snapshot()
	require.NoError(t, emulator.Close())
	return snapshot
}
