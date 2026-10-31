package input

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
)

const (
	testMod27  = "\x1b[27;5;97~"
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

func terminal() Event { return Event{Kind: KindTerminal} }

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
		"\x1bP":        {keyr('P', ModAlt)},
		"\x1b]":        {keyr(']', ModAlt)},
		"\x1b\\":       {keyr('\\', ModAlt)},
		"\x1b\x7f":     {keym(KeyBackspace, ModAlt)},
		"\x1b\xc3\xa9": {keyr('é', ModAlt)},
		"\x1b[200~ab\x1b[201~": {
			{Kind: KindPasteStart}, txt("a"), txt("b"), {Kind: KindPasteEnd},
		},
		"\x1b[200~x\x1b[201~\r": {
			{Kind: KindPasteStart}, txt("x"), {Kind: KindPasteEnd}, {Kind: KindSubmit},
		},
		"\x1bOA":     {key(KeyArrowUp)},
		"\x1bOB":     {key(KeyArrowDown)},
		"\x1bOC":     {key(KeyArrowRight)},
		"\x1bOD":     {key(KeyArrowLeft)},
		"\x1bOH":     {key(KeyHome)},
		"\x1bOF":     {key(KeyEnd)},
		"\x1b[201~":  {{Kind: KindPasteEnd}},
		"\x1b[9X":    {unk("\x1b[9X")},
		"\x1b[M":     {unk("\x1b[M")},
		"\xff":       {unk("\xff")},
		"\x00":       {unk("\x00")},
		"\x1b[97;5u": {unk("\x1b[97;5u")},
		testMod27:    {unk(testMod27)},
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

	m := feedAll(NewParser(mustProfile("modify_other_keys")), []byte(testMod27))
	require.Equal(t, []Event{keyr('a', ModCtrl)}, m)

	m = feedAll(NewParser(mustProfile("modify_other_keys")), []byte("\x1b[27;5;13~"))
	require.Equal(t, []Event{{Kind: KindKey, Key: Key{Code: KeyExtended, Mod: ModCtrl, Rune: 13}}}, m)

	u = feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97;5:1u"))
	require.Equal(t, []Event{keyr('a', ModCtrl)}, u)

	u = feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97;5:2u"))
	require.Equal(t, []Event{keyr('a', ModCtrl)}, u)

	u = feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97;5:3u"))
	require.Equal(t, []Event{unk("\x1b[97;5:3u")}, u)

	u = feedAll(NewParser(mustProfile("csi_u")), []byte("\x1b[97;5:9u"))
	require.Equal(t, []Event{unk("\x1b[97;5:9u")}, u)

	m = feedAll(NewParser(mustProfile("modify_other_keys")), []byte("\x1b[27;5;97u"))
	require.Equal(t, []Event{unk("\x1b[27;5;97u")}, m)
}

func TestTerminalReportsAndPassiveInput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"DCS XTVERSION":         "\x1bP>|tmux 3.4\x1b\\",
		"DCS with C1 ST":        "\x1bP1$r0m\x9c",
		"OSC with ST":           "\x1b]11;rgb:1111/2222/3333\x1b\\",
		"OSC with BEL":          "\x1b]11;rgb:1111/2222/3333\a",
		"8-bit DCS":             "\x90>|terminal\x9c",
		"8-bit OSC":             "\x9d11;rgb:1/2/3\x9c",
		"primary DA":            "\x1b[?1;2c",
		"secondary DA":          "\x1b[>0;95;0c",
		"tertiary DA":           "\x1b[=1;2c",
		"DECRPM":                "\x1b[?2004;1$y",
		"ANSI mode report":      "\x1b[4;1$y",
		"CPR":                   "\x1b[12;40R",
		"private CPR":           "\x1b[?12;40R",
		"focus in":              "\x1b[I",
		"focus out":             "\x1b[O",
		"SGR passive motion":    "\x1b[<35;12;5M",
		"X10 passive motion":    "\x1b[MC!!",
		"8-bit CSI focus in":    "\x9bI",
		"8-bit CSI private CPR": "\x9b?12;40R",
	}
	for name, sequence := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, []Event{terminal()}, feedAll(NewParser(defaultProfile()), []byte(sequence)))
		})
	}
}

func TestMouseActionsStayHumanPriorityAsOneUnit(t *testing.T) {
	t.Parallel()

	for name, sequence := range map[string]string{
		"SGR press":   "\x1b[<0;12;5M",
		"SGR release": "\x1b[<0;12;5m",
		"SGR wheel":   "\x1b[<65;12;5M",
		"SGR drag":    "\x1b[<32;12;5M",
		"X10 press":   "\x1b[M !!",
		"X10 wheel":   "\x1b[Ma!!",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, []Event{unk(sequence)}, feedAll(NewParser(defaultProfile()), []byte(sequence)))
		})
	}
}

func TestTerminalReportsAreSplitInvariant(t *testing.T) {
	t.Parallel()

	for name, sequence := range map[string]string{
		"DCS ST":          "\x1bP>|tmux 3.4\x1b\\",
		"OSC ST":          "\x1b]11;rgb:1/2/3\x1b\\",
		"OSC BEL":         "\x1b]11;rgb:1/2/3\a",
		"DA":              "\x1b[?1;2c",
		"DECRPM":          "\x1b[?2004;1$y",
		"CPR":             "\x1b[12;40R",
		"SGR motion":      "\x1b[<35;12;5M",
		"X10 motion":      "\x1b[MC!!",
		"focus":           "\x1b[I",
		"SGR mouse click": "\x1b[<0;12;5M",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := feedAll(NewParser(defaultProfile()), []byte(sequence))
			require.Equal(t, want, feedAllBytes(NewParser(defaultProfile()), []byte(sequence)))
			for cut := 1; cut < len(sequence); cut++ {
				p := NewParser(defaultProfile())
				got := append([]Event{}, p.Feed([]byte(sequence[:cut]))...)
				got = append(got, p.Feed([]byte(sequence[cut:]))...)
				got = append(got, p.Flush()...)
				require.Equal(t, want, got, "cut at %d", cut)
			}
		})
	}
}

func TestTerminalLikeHumanInputReplaysPromptly(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		sequence string
		want     []Event
		wait     bool
	}{
		"Alt+P":           {"\x1bP", []Event{keyr('P', ModAlt)}, true},
		"Alt+]":           {"\x1b]", []Event{keyr(']', ModAlt)}, true},
		"Alt+_":           {"\x1b_", []Event{keyr('_', ModAlt)}, false},
		"DCS-like typing": {"\x1bPabc", []Event{keyr('P', ModAlt), txt("a"), txt("b"), txt("c")}, false},
		"OSC-like typing": {"\x1b]11;x", []Event{keyr(']', ModAlt), txt("1"), txt("1"), txt(";"), txt("x")}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(defaultProfile())
			got := p.Feed([]byte(tc.sequence))
			require.Equal(t, tc.wait, p.Pending())
			if tc.wait {
				require.Empty(t, got)
				got = p.Flush()
			}
			require.Equal(t, tc.want, got)
			require.False(t, p.Pending())
		})
	}
}

func TestUnknownTerminalStringsRemainHumanInput(t *testing.T) {
	t.Parallel()

	for name, sequence := range map[string]string{
		"DCS":       "\x1bPtyped-draft\x1b\\",
		"OSC":       "\x1b]typed-draft\x1b\\",
		"APC":       "\x1b_Gpayload\x1b\\",
		"8-bit DCS": "\x90typed-draft\x9c",
		"8-bit OSC": "\x9dtyped-draft\x9c",
		"8-bit APC": "\x9fGpayload\x9c",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			whole := feedAll(NewParser(defaultProfile()), []byte(sequence))
			require.NotEmpty(t, whole)
			require.NotContains(t, whole, terminal())
			require.Equal(t, whole, feedAllBytes(NewParser(defaultProfile()), []byte(sequence)))
		})
	}
}

func TestTruncatedRecognizedResponsesReplayHumanInput(t *testing.T) {
	t.Parallel()

	for name, sequence := range map[string]string{
		"XTVERSION": "\x1bP>|tmux 3.4",
		"OSC 11":    "\x1b]11;rgb:1/2",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(defaultProfile())
			require.Empty(t, p.Feed([]byte(sequence)))
			require.True(t, p.Pending())
			events := p.Flush()
			require.False(t, p.Pending())
			require.NotEmpty(t, events)
			require.NotContains(t, events, terminal())
		})
	}
}

func TestTerminalStringBoundAndInterruptedKey(t *testing.T) {
	t.Parallel()

	p := NewParser(defaultProfile())
	tooLong := "\x1bP>|" + strings.Repeat("x", maxTerminalStringBytes)
	events := p.Feed([]byte(tooLong))
	require.False(t, p.Pending(), "overlong ambiguity must not retain memory")
	require.Equal(t, keyr('P', ModAlt), events[0])
	require.Len(t, events, 3+maxTerminalStringBytes)

	p = NewParser(defaultProfile())
	require.Empty(t, p.Feed([]byte("\x1bP>|tmux")))
	events = p.Feed([]byte("\x1b[A"))
	require.False(t, p.Pending())
	require.Equal(t, keyr('P', ModAlt), events[0])
	require.Equal(t, key(KeyArrowUp), events[len(events)-1], "the key interrupting the fragment is preserved")
}

func TestBELTerminatesOnlyOSC(t *testing.T) {
	t.Parallel()

	p := NewParser(defaultProfile())
	events := p.Feed([]byte("\x1bP>|terminal\a"))
	require.False(t, p.Pending())
	require.NotEmpty(t, events, "BEL invalidates a DCS response instead of terminating one")
	require.NotContains(t, events, terminal())

	p = NewParser(defaultProfile())
	require.Equal(t, []Event{terminal()}, p.Feed([]byte("\x1b]11;rgb:1/2/3\a")))
	require.False(t, p.Pending())
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
		"\x1b[200~x\x1b[201~\r",
		"\x1bOA\x1bOH",
		testMod27,
		"\x1b[97;5:1u",
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
		"\x1b[200~x\x1b[201~", "\x1b[97;5u", testMod27,
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

// TestPendingHoldsAPartialSequence feeds key sequences cut in two: the
// first part leaves Pending set, the rest clears it, whole sequences and
// text inside a paste never set it, and Flush clears it. Pending reads
// the parser and changes nothing: the events are those of an uncut feed.
func TestPendingHoldsAPartialSequence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, first, rest string }{
		{"lone escape", "\x1b", "x"},
		{"cut csi", "\x1b[1;5", "A"},
		{"cut ss3", "\x1bO", "A"},
		{"cut paste start", "\x1b[20", "0~ab\x1b[201~"},
		{"cut paste end", "\x1b[200~ab\x1b[20", "1~"},
		{"cut alt rune", "\x1b\xc3", "\xa9"},
		{"cut rune", "\xc3", "\xa9"},
		{"cut rune in a paste", "\x1b[200~\xc3", "\xa9\x1b[201~"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(defaultProfile())
			require.False(t, p.Pending(), "a new parser holds nothing")
			got := p.Feed([]byte(tc.first))
			require.True(t, p.Pending(), "the first part is held back")
			require.True(t, p.Pending(), "reading it changes nothing")
			got = append(got, p.Feed([]byte(tc.rest))...)
			require.False(t, p.Pending(), "the whole sequence holds nothing back")
			require.Equal(t, feedAll(NewParser(defaultProfile()), []byte(tc.first+tc.rest)), got)
		})
	}

	p := NewParser(defaultProfile())
	for _, whole := range []string{"x", "\x1b[1;5A", "\x1bOA", "\x1bx", testEAcute, "\x1b[200~", "pasted text", "\x1b[201~", "\r"} {
		p.Feed([]byte(whole))
		require.False(t, p.Pending(), "%q is whole", whole)
	}
	p.Feed([]byte("\x1b["))
	require.True(t, p.Pending())
	p.Flush()
	require.False(t, p.Pending(), "Flush resolves what was held")
}
