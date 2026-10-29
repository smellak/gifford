package input

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
)

const (
	testCafe   = "caf\xc3\xa9"
	testEAcute = "\xc3\xa9"
)

var bs = string(rune(92))

func synthYAML(keyboard string) string {
	return strings.Join([]string{
		`harness: synth`,
		`measured: true`,
		`version_range: "*"`,
		`submit: "\r"`,
		`insert_newline: "\n"`,
		`clear_line: "` + bs + `u0015"`,
		`cancel: "` + bs + `u0003"`,
		`bracketed_paste: true`,
		`extended_keyboard: ` + keyboard,
		`input_line: "^(.*)$"`,
		"",
	}, "\n")
}

func mustProfile(keyboard string) profiles.Profile {
	p, err := profiles.ParseFile("synth", []byte(synthYAML(keyboard)))
	if err != nil {
		panic(err)
	}
	return p
}

func defaultProfile() profiles.Profile {
	ps, err := profiles.Load()
	if err != nil {
		panic(err)
	}
	p, err := ps.Profile("codex", "1.0")
	if err != nil {
		panic(err)
	}
	return p
}

func txt(s string) Event          { return Event{Kind: KindText, Text: s} }
func unk(s string) Event          { return Event{Kind: KindUnknown, Raw: s} }
func key(c KeyCode) Event         { return Event{Kind: KindKey, Key: Key{Code: c}} }
func keym(c KeyCode, m Mod) Event { return Event{Kind: KindKey, Key: Key{Code: c, Mod: m}} }
func keyr(r rune, m Mod) Event {
	return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: m, Rune: r}}
}

func feedAll(p *Parser, chunks ...[]byte) []Event {
	out := make([]Event, 0, 8)
	for _, c := range chunks {
		out = append(out, p.Feed(c)...)
	}
	out = append(out, p.Flush()...)
	if len(out) == 0 {
		return nil
	}
	return out
}

func TestSequences(t *testing.T) {
	t.Parallel()

	cases := map[string][]Event{
		"hello":        {txt("h"), txt("e"), txt("l"), txt("l"), txt("o")},
		testCafe:       {txt("c"), txt("a"), txt("f"), txt(testEAcute)},
		"\r":           {{Kind: KindSubmit}},
		"\x03":         {{Kind: KindCancel}},
		"\x15":         {{Kind: KindClearLine}},
		"\n":           {{Kind: KindInsertNewline}},
		" ":            {txt(" ")},
		"\x1b[A":       {key(KeyArrowUp)},
		"\x1b[B":       {key(KeyArrowDown)},
		"\x1b[D":       {key(KeyArrowLeft)},
		"\x1b[1;5C":    {keym(KeyArrowRight, ModCtrl)},
		"\x1b[1;2A":    {keym(KeyArrowUp, ModShift)},
		"\x1b[1;3D":    {keym(KeyArrowLeft, ModAlt)},
		"\x1b[1;8F":    {keym(KeyEnd, ModShift|ModAlt|ModCtrl)},
		"\x1b[H":       {key(KeyHome)},
		"\x1b[F":       {key(KeyEnd)},
		"\x1b[3~":      {key(KeyDelete)},
		"\x1b[3;5~":    {keym(KeyDelete, ModCtrl)},
		"\x1b[2~":      {key(KeyInsert)},
		"\x1b[5~":      {key(KeyPageUp)},
		"\x1b[6~":      {key(KeyPageDown)},
		"\x1b[Z":       {keym(KeyTab, ModShift)},
		"\x1bOP":       {key(KeyF1)},
		"\x1bOS":       {key(KeyF4)},
		"\x1b[15~":     {key(KeyF5)},
		"\x1b[24~":     {key(KeyF12)},
		"\x7f":         {key(KeyBackspace)},
		"\x08":         {key(KeyBackspace)},
		"\x09":         {key(KeyTab)},
		"\x1bb":        {keyr('b', ModAlt)},
		"\x1b\x7f":     {keym(KeyBackspace, ModAlt)},
		"\x1b\xc3\xa9": {keyr('é', ModAlt)},
		"\x1b[200~ab\x1b[201~": {
			{Kind: KindPasteStart}, txt("a"), txt("b"), {Kind: KindPasteEnd},
		},
		"\x1b[201~":     {{Kind: KindPasteEnd}},
		"\x1b[9X":       {unk("\x1b[9X")},
		"\x1b[M":        {unk("\x1b[M")},
		"\xff":          {unk("\xff")},
		"\x00":          {unk("\x00")},
		"\x1b[97;5u":    {unk("\x1b[97;5u")},
		"\x1b[27;5;97~": {unk("\x1b[27;5;97~")},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := feedAll(NewParser(defaultProfile()), []byte(name))
			require.Equal(t, want, got)
		})
	}
}

func TestExtendedModes(t *testing.T) {
	t.Parallel()

	u := feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97;5u"))
	require.Equal(t, []Event{keyr('a', ModCtrl)}, u)

	u = feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97u"))
	require.Equal(t, []Event{keyr('a', 0)}, u)

	m := feedAll(NewParser(mustProfile("modify_other_keys")), []byte("\x1b[27;5;97~"))
	require.Equal(t, []Event{keyr('a', ModCtrl)}, m)

	m = feedAll(NewParser(mustProfile("modify_other_keys")), []byte("\x1b[27;5;13~"))
	require.Equal(t, []Event{{Kind: KindKey, Key: Key{Code: KeyExtended, Mod: ModCtrl, Rune: 13}}}, m)
}

func TestProfileWinsOverGeneric(t *testing.T) {
	t.Parallel()

	raw := strings.Replace(synthYAML("none"), `submit: "\r"`, `submit: "\x1b[A"`, 1)
	p, err := profiles.ParseFile("synth", []byte(raw))
	require.NoError(t, err)
	got := feedAll(NewParser(p), []byte("\x1b[A"))
	require.Equal(t, []Event{{Kind: KindSubmit}}, got)
}

func TestFlushResolvesPending(t *testing.T) {
	t.Parallel()

	p := NewParser(defaultProfile())
	require.Empty(t, p.Feed([]byte("\x1b")))
	require.Equal(t, []Event{unk("\x1b")}, p.Flush())
	require.Empty(t, p.Flush())

	p = NewParser(defaultProfile())
	require.Empty(t, p.Feed([]byte("\x1b[1")))
	require.Equal(t, []Event{unk("\x1b[1")}, p.Flush())

	p = NewParser(defaultProfile())
	got := feedAll(p, []byte("\x1b[200~ab"))
	require.Equal(t, []Event{{Kind: KindPasteStart}, txt("a"), txt("b")}, got)
}

// TestUnterminatedWaits shows an unterminated CSI head stays pending (no
// length cap, no chunk-dependent cut) and Flush reports it whole.
func TestUnterminatedWaits(t *testing.T) {
	t.Parallel()

	in := "\x1b[" + strings.Repeat("1", 70)
	p := NewParser(defaultProfile())
	require.Empty(t, p.Feed([]byte(in)))
	require.Equal(t, []Event{unk(in)}, p.Flush())
}

// TestSplitEquivalence feeds every input whole, byte by byte and at every
// two-way cut: the events must match exactly.
func TestSplitEquivalence(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"hello\r",
		"\x1b[A\x1b[3~\x7f",
		"\x1b[200~a\xc3\xa9\x1b[201~",
		"\x1b[1;5C\x1bOP\x1bb",
		"\x1b[97;5u\x1b[27;5;97~",
		"\xff\x00\x1b[9X",
		"a\x1b[200~",
		testCafe,
		"\x1b\xc3\xa9",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			whole := feedAll(NewParser(defaultProfile()), []byte(in))
			byByte := feedAllBytes(NewParser(defaultProfile()), []byte(in))
			require.Equal(t, whole, byByte)
			for cut := 1; cut < len(in); cut++ {
				p := NewParser(defaultProfile())
				got := append([]Event{}, p.Feed([]byte(in[:cut]))...)
				got = append(got, p.Feed([]byte(in[cut:]))...)
				got = append(got, p.Flush()...)
				require.Equal(t, whole, got, "cut at %d", cut)
			}
		})
	}
}

func feedAllBytes(p *Parser, b []byte) []Event {
	chunks := make([][]byte, 0, len(b))
	for i := range b {
		chunks = append(chunks, b[i:i+1])
	}
	return feedAll(p, chunks...)
}

var fuzzProfile = mustProfile("csi_u")

// FuzzParser holds the chunking contract under random bytes: whole,
// byte-at-a-time and cut feeds agree, and nothing panics.
func FuzzParser(f *testing.F) {
	seeds := []string{
		"", "hi", "\r", "\x03", "\x1b", "\x1b[", "\x1b[A",
		"\x1b[200~x\x1b[201~", "\x1b[97;5u", "\x1b[27;5;97~",
		testCafe, "\xff\xfe", "\x1bOP", "\x1bb",
	}
	for _, s := range seeds {
		f.Add([]byte(s), 0)
		f.Add([]byte(s), 1)
		f.Add([]byte(s), len(s))
	}
	f.Fuzz(func(t *testing.T, data []byte, cut int) {
		whole := feedAll(NewParser(fuzzProfile), data)
		byByte := feedAllBytes(NewParser(fuzzProfile), data)
		require.Equal(t, whole, byByte)
		n := len(data) + 1
		at := ((cut % n) + n) % n
		p := NewParser(fuzzProfile)
		got := feedAll(p, data[:at], data[at:])
		require.Equal(t, whole, got)
	})
}
