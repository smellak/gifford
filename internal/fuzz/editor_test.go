package fuzz

import (
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// abc is a draft the editor tests type.
const abc = "abc"

// TestEditorKeys runs the fake harness's editor over each key it knows:
// every line it gets is a record of what it did with it.
func TestEditorKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		keys string
		want []Record
	}{
		{"enter", hello + "\r", []Record{{RecordLine, hello}}},
		{"empty enter", "\r", []Record{{RecordLine, ""}}},
		{"backspace", "helxx\x7f\x7flo\r", []Record{{RecordLine, hello}}},
		{"ctrl-h", "ab\x08c\r", []Record{{RecordLine, "ac"}}},
		{"backspace on empty", "\x7f\x7fa\r", []Record{{RecordLine, "a"}}},
		{"backspace a rune", "añ\x7f\r", []Record{{RecordLine, "a"}}},
		{"ctrl-u", "abc\x15de\r", []Record{{RecordClear, abc}, {RecordLine, "de"}}},
		{"ctrl-c", "abc\x03de\r", []Record{{RecordCancel, abc}, {RecordLine, "de"}}},
		{"paste", "a\x1b[200~b\rc\x03\x1b[201~d\r", []Record{{RecordLine, "ab\rc\x03d"}}},
		{"paste end alone", "a\x1b[201~b\r", []Record{{RecordLine, "ab"}}},
		{"other sequences", "a\x1b[Ab\x1bxc\r", []Record{{RecordLine, abc}}},
		{"ss3", "a\x1bOBb\x1bOAc\r", []Record{{RecordLine, abc}}},
		{"alt rune", "a\x1béb\x1b\x1bc\r", []Record{{RecordLine, abc}}},
		{"ctrl arrow", "ab\x1b[1;5Dc\r", []Record{{RecordLine, abc}}},
		{"controls kept", "a\nb\tc\r", []Record{{RecordLine, "a\nb\tc"}}},
		{"eof with a draft", "ab\r cd\x04ef\r", []Record{{RecordLine, "ab"}, {RecordEOF, " cd"}}},
		{"eof on empty", "ab\r\x04", []Record{{RecordLine, "ab"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, Expected([]byte(tc.keys)))
		})
	}
}

// TestEditorChunking feeds the same keys whole, byte by byte and cut at
// random: the records, the draft and the last screen line are the same,
// even with escape sequences, SS3 and Alt keys, paste markers and runes
// cut in two.
func TestEditorChunking(t *testing.T) {
	t.Parallel()
	keys := []byte("héllo\x7f\x7fwörld\r\x1b[200~pa\rsté\x1b[201~ end\x15again\x1b[Cx\x03more\r\x1b[200~x\x1b[201~\r" +
		"up\x1bOAalt\x1béx\x1bxctrl\x1b[1;5D\r")
	var whole editor
	_, want := whole.feed(keys)
	require.NotEmpty(t, want)
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var e editor
		var got []Record
		var screen []byte
		for rest := keys; len(rest) > 0; {
			n := 1 + r.IntN(min(len(rest), 6))
			out, records := e.feed(rest[:n])
			got = append(got, records...)
			screen = append(screen, out...)
			rest = rest[n:]
		}
		require.Equal(t, want, got)
		require.Equal(t, string(whole.draft), string(e.draft))
		require.False(t, e.pasting)
		require.Empty(t, e.pending)
		require.True(t, strings.HasSuffix(string(screen), "\r\n"+Prompt+drawn), "the screen ends on a fresh prompt")
	}
}

// TestEditorScreen draws the draft on one line after the prompt, a
// control byte as ?, a finished line kept above the fresh prompt, and
// every draw ends on a sequence.
func TestEditorScreen(t *testing.T) {
	t.Parallel()
	var e editor
	require.Equal(t, "fz ready\r\n"+Prompt+drawn, string(e.start()))
	out, _ := e.feed([]byte("ab"))
	require.Equal(t, "\r\x1b[K"+Prompt+"ab"+drawn, string(out))
	out, _ = e.feed([]byte("\x1b[200~c\nd\x1b[201~"))
	require.Equal(t, "\r\x1b[K"+Prompt+"abc?d"+drawn, string(out))
	out, _ = e.feed([]byte("\r"))
	require.Equal(t, "\r\n"+Prompt+drawn, string(out))
	out, _ = e.feed([]byte("x\x03"))
	require.Equal(t, "\r\x1b[K"+Prompt+"x^C\r\n"+Prompt+drawn, string(out))
	out, _ = e.feed([]byte("\x1b[A"))
	require.Empty(t, out, "a key the editor ignores draws nothing")
	out, records := e.feed([]byte("\x04after"))
	require.Empty(t, out)
	require.Empty(t, records)
	out, records = e.feed([]byte("more\r"))
	require.Empty(t, out, "nothing after Ctrl-D")
	require.Empty(t, records)
}

// TestRecordsReadBack writes records as the harness does and reads them
// back exactly, whatever bytes a line holds.
func TestRecordsReadBack(t *testing.T) {
	t.Parallel()
	records := []Record{
		{RecordLine, "plain"},
		{RecordLine, ""},
		{RecordCancel, "with \"quotes\" and spaces "},
		{RecordClear, "\x1b[200~\r\n\x00"},
		{RecordEOF, "bad \xff utf-8 é"},
	}
	var b strings.Builder
	for _, r := range records {
		b.WriteString(r.String() + "\n")
	}
	got, err := ParseRecords([]byte(b.String()))
	require.NoError(t, err)
	require.Equal(t, records, got)

	got, err = ParseRecords(nil)
	require.NoError(t, err)
	require.Empty(t, got)
	for _, bad := range []string{"line\n", "nope \"x\"\n", "line x\n", "line \"open\n"} {
		_, err := ParseRecords([]byte(bad))
		require.ErrorIs(t, err, errRecord, bad)
	}
}
