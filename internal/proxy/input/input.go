// Package input parses the bytes a human types into typed events: one
// incremental, pure and deterministic parser over the key stream. It feeds
// the draft guard (which decides what reaches the harness); it never
// persists or logs typed text itself. Event payloads live in memory only,
// so input.* metadata downstream must carry types and lengths, never
// content.
//
// Splitting the byte flow never changes the events: Feed may be called
// with any chunking, one byte at a time or whole buffers. A trailing ESC
// stays pending until the next byte or Flush; the parser owns no timers.
package input

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
)

// Kind names the type of an input event.
type Kind string

// Event kinds.
const (
	// KindText is one printable rune of typed text.
	KindText Kind = "text"
	// KindSubmit sends the draft (the profile's submit sequence).
	KindSubmit Kind = "submit"
	// KindInsertNewline breaks the line inside the draft.
	KindInsertNewline Kind = "insert_newline"
	// KindClearLine erases the draft line.
	KindClearLine Kind = "clear_line"
	// KindCancel cancels the draft (usually Ctrl-C).
	KindCancel Kind = "cancel"
	// KindPasteStart opens a bracketed paste.
	KindPasteStart Kind = "paste_start"
	// KindPasteEnd closes a bracketed paste.
	KindPasteEnd Kind = "paste_end"
	// KindKey is an editing, navigation, function or extended key.
	KindKey Kind = "key"
	// KindUnknown is a byte sequence no rule recognises; Raw keeps it so
	// nothing typed is ever lost.
	KindUnknown Kind = "unknown"
)

// KeyCode names a non-printable key.
type KeyCode string

// Key codes.
const (
	KeyArrowUp    KeyCode = "arrow_up"
	KeyArrowDown  KeyCode = "arrow_down"
	KeyArrowLeft  KeyCode = "arrow_left"
	KeyArrowRight KeyCode = "arrow_right"
	KeyHome       KeyCode = "home"
	KeyEnd        KeyCode = "end"
	KeyInsert     KeyCode = "insert"
	KeyDelete     KeyCode = "delete"
	KeyPageUp     KeyCode = "page_up"
	KeyPageDown   KeyCode = "page_down"
	KeyBackspace  KeyCode = "backspace"
	KeyTab        KeyCode = "tab"
	KeyF1         KeyCode = "f1"
	KeyF2         KeyCode = "f2"
	KeyF3         KeyCode = "f3"
	KeyF4         KeyCode = "f4"
	KeyF5         KeyCode = "f5"
	KeyF6         KeyCode = "f6"
	KeyF7         KeyCode = "f7"
	KeyF8         KeyCode = "f8"
	KeyF9         KeyCode = "f9"
	KeyF10        KeyCode = "f10"
	KeyF11        KeyCode = "f11"
	KeyF12        KeyCode = "f12"
	// KeyRune is a printable rune with modifiers (Alt+letter, CSI u,
	// modifyOtherKeys over ASCII); Rune carries it.
	KeyRune KeyCode = "rune"
	// KeyExtended is a modifyOtherKeys key with no rune mapping; Rune
	// carries its numeric keycode so the consumer can still forward it.
	KeyExtended KeyCode = "extended"
)

// Mod holds key modifiers as a bitmask.
type Mod uint8

// Modifiers.
const (
	ModShift Mod = 1 << iota
	ModAlt
	ModCtrl
)

// Key is a KindKey payload.
type Key struct {
	Code KeyCode
	Mod  Mod
	Rune rune
}

// Event is one parsed input unit. Text is set only for KindText (exactly
// one rune), Raw only for KindUnknown (the unrecognised bytes), Key only
// for KindKey.
type Event struct {
	Kind Kind
	Text string
	Key  Key
	Raw  string
}

// profileSeq pairs a profile byte sequence with the event it produces.
type profileSeq struct {
	seq  string
	kind Kind
}

// Parser is an incremental input parser bound to one harness profile. The
// zero value is not usable; build one with NewParser. A Parser is not safe
// for concurrent use.
type Parser struct {
	seqs        []profileSeq
	csiU        bool
	modifyOther bool
	pending     []byte
	inPaste     bool
}

// Unresolved bytes wait without any length cap: deciding "this is not a
// sequence" from a prefix alone would depend on Feed chunking, so the
// parser only resolves on terminators or Flush. Profile sequences, Alt
// followers and UTF-8 runes bound themselves; an unterminated CSI head
// or an open paste accumulate until their closer, a final byte, or Flush.
// A malicious flow of digits after ESC [ could retain memory; session
// byte budgets above this package own that bound.

// NewParser binds a parser to a profile: its submit, insert_newline,
// clear_line and cancel sequences win over the generic xterm table, and
// its extended_keyboard mode gates CSI u / modifyOtherKeys decoding.
// Empty sequences are ignored so a zero Profile cannot wedge the parser.
func NewParser(p profiles.Profile) *Parser {
	seqs := []profileSeq{
		{p.Submit, KindSubmit},
		{p.InsertNewline, KindInsertNewline},
		{p.ClearLine, KindClearLine},
		{p.Cancel, KindCancel},
	}
	kept := seqs[:0]
	for _, s := range seqs {
		if s.seq != "" {
			kept = append(kept, s)
		}
	}
	sort.SliceStable(kept, func(i, j int) bool { return len(kept[i].seq) > len(kept[j].seq) })
	return &Parser{
		seqs:        kept,
		csiU:        p.ExtendedKeyboard == profiles.ExtendedCSIU,
		modifyOther: p.ExtendedKeyboard == profiles.ExtendedModifyOtherKeys,
	}
}

// Feed parses a chunk and returns its events, possibly none: a trailing
// ESC or a split sequence stays pending. Feed never panics and never
// returns an error; unrecognised bytes become KindUnknown events.
func (p *Parser) Feed(chunk []byte) []Event {
	p.pending = append(p.pending, chunk...)
	var out []Event
	for len(p.pending) > 0 {
		ev, n, wait := p.step()
		if wait {
			break
		}
		out = append(out, ev)
		p.pending = p.pending[n:]
	}
	return out
}

// Flush resolves pending bytes at end of stream: whatever is still
// incomplete becomes one unknown event. An open paste stays open; only
// buffered bytes resolve.
func (p *Parser) Flush() []Event {
	if len(p.pending) == 0 {
		return nil
	}
	ev := Event{Kind: KindUnknown, Raw: string(p.pending)}
	p.pending = p.pending[:0]
	return []Event{ev}
}

const (
	esc      = 0x1b
	pasteOn  = "\x1b[200~"
	pasteOff = "\x1b[201~"
)

// step classifies the head of pending, returning the event, the bytes it
// consumes, and whether more bytes are needed. When wait is true pending
// holds exactly one incomplete unit: anything resolvable ahead of it was
// already emitted, so Flush can report it whole.
func (p *Parser) step() (Event, int, bool) {
	b := p.pending
	if p.inPaste {
		return p.stepPaste()
	}
	for _, s := range p.seqs {
		if bytes.HasPrefix(b, []byte(s.seq)) {
			return Event{Kind: s.kind}, len(s.seq), false
		}
		if len(b) < len(s.seq) && strings.HasPrefix(s.seq, string(b)) {
			return Event{}, 0, true
		}
	}
	if b[0] == esc {
		return p.stepEscape()
	}
	switch b[0] {
	case '\t':
		return Event{Kind: KindKey, Key: Key{Code: KeyTab}}, 1, false
	case 0x7f, 0x08:
		return Event{Kind: KindKey, Key: Key{Code: KeyBackspace}}, 1, false
	}
	return stepRune(b, false)
}

// stepPaste classifies inside a bracketed paste: everything up to the
// closing sequence is text, even control bytes and escape-looking runs,
// because the terminal sends pasted content literally. Consuming the
// closer drops paste mode so the next byte parses normally again.
func (p *Parser) stepPaste() (Event, int, bool) {
	b := p.pending
	if bytes.HasPrefix(b, []byte(pasteOff)) {
		p.inPaste = false
		return Event{Kind: KindPasteEnd}, len(pasteOff), false
	}
	if len(b) < len(pasteOff) && strings.HasPrefix(pasteOff, string(b)) {
		return Event{}, 0, true
	}
	return stepRune(b, true)
}

// stepRune consumes one UTF-8 unit: a printable rune becomes text,
// anything else unknown. In a paste every valid rune counts as text. An
// incomplete trailing rune waits for more bytes.
func stepRune(b []byte, paste bool) (Event, int, bool) {
	if b[0] < utf8.RuneSelf {
		if b[0] < 0x20 || b[0] == 0x7f {
			if paste {
				return Event{Kind: KindText, Text: string(b[:1])}, 1, false
			}
			return Event{Kind: KindUnknown, Raw: string(b[:1])}, 1, false
		}
		return Event{Kind: KindText, Text: string(b[:1])}, 1, false
	}
	// FullRune is false only for a valid-but-truncated prefix; an invalid
	// encoding already counts as full (width-1 error rune).
	if !utf8.FullRune(b) {
		return Event{}, 0, true
	}
	r, size := utf8.DecodeRune(b)
	if r == utf8.RuneError && size == 1 {
		return Event{Kind: KindUnknown, Raw: string(b[:1])}, 1, false
	}
	if paste {
		return Event{Kind: KindText, Text: string(b[:size])}, size, false
	}
	if !isPrint(r) {
		return Event{Kind: KindUnknown, Raw: string(b[:size])}, size, false
	}
	return Event{Kind: KindText, Text: string(b[:size])}, size, false
}

func isPrint(r rune) bool {
	return unicode.IsPrint(r)
}

// stepEscape classifies at ESC: CSI, SS3, Alt+byte, or a lone ESC waiting
// for its follower.
func (p *Parser) stepEscape() (Event, int, bool) {
	b := p.pending
	if len(b) == 1 {
		return Event{}, 0, true
	}
	switch b[1] {
	case '[':
		return p.stepCSI()
	case 'O':
		return stepSS3(b)
	}
	return stepAlt(b)
}

// stepCSI parses ESC [ params final. Profile sequences already won above;
// here live paste markers, arrows, editing keys, function keys, the
// profile-gated extended modes, and anything else a CSI can carry (which
// becomes one unknown event, never a panic and never a byte drip).
func (p *Parser) stepCSI() (Event, int, bool) {
	b := p.pending
	end := -1
	for i := 2; i < len(b); i++ {
		c := b[i]
		if c >= 0x40 && c <= 0x7E {
			end = i
			break
		}
		if c < 0x20 || c == 0x7f {
			return Event{Kind: KindUnknown, Raw: string(b[:i+1])}, i + 1, false
		}
	}
	if end < 0 {
		return Event{}, 0, true
	}
	seq := string(b[:end+1])
	params, final := string(b[2:end]), b[end]
	switch {
	case seq == pasteOn:
		p.inPaste = true
		return Event{Kind: KindPasteStart}, end + 1, false
	case seq == pasteOff:
		p.inPaste = false
		return Event{Kind: KindPasteEnd}, end + 1, false
	case final == 'u' && p.csiU:
		return decodeCSIU(seq, params)
	case final == '~' && paramsSplits27(params) && p.modifyOther:
		return decodeModifyOther(seq, params)
	}
	if key, ok := decodeCSIKey(params, final); ok {
		return Event{Kind: KindKey, Key: key}, end + 1, false
	}
	return Event{Kind: KindUnknown, Raw: seq}, end + 1, false
}

// paramsSplits27 reports the modifyOtherKeys head "27;".
func paramsSplits27(params string) bool {
	head, _, ok := strings.Cut(params, ";")
	return ok && head == "27"
}

// decodeCSIKey maps arrows, home/end and editing keys, plain or with a
// ;modifier suffix (1 means none, 2-8 shift/alt/ctrl bits).
func decodeCSIKey(params string, final byte) (Key, bool) {
	base, mods, ok := splitMods(params)
	if !ok {
		return Key{}, false
	}
	var code KeyCode
	switch {
	case len(base) <= 1 && final >= 'A' && final <= 'D':
		code = [...]KeyCode{KeyArrowUp, KeyArrowDown, KeyArrowRight, KeyArrowLeft}[final-'A']
	case len(base) <= 1 && final == 'H':
		code = KeyHome
	case len(base) <= 1 && final == 'F':
		code = KeyEnd
	case final == '~':
		code = tildeKey(base)
	case final == 'Z' && (base == "" || base == "1"):
		return Key{Code: KeyTab, Mod: ModShift}, true
	}
	if code == "" {
		return Key{}, false
	}
	mod, ok := decodeMod(mods)
	if !ok {
		return Key{}, false
	}
	return Key{Code: code, Mod: mod}, true
}

// tildeKey maps "~"-terminated numeric CSI params to editing and function
// keys; "" means no known key.
func tildeKey(base string) KeyCode {
	switch base {
	case "1", "7":
		return KeyHome
	case "2":
		return KeyInsert
	case "3":
		return KeyDelete
	case "4", "8":
		return KeyEnd
	case "5":
		return KeyPageUp
	case "6":
		return KeyPageDown
	case "11":
		return KeyF1
	case "12":
		return KeyF2
	case "13":
		return KeyF3
	case "14":
		return KeyF4
	case "15":
		return KeyF5
	case "17":
		return KeyF6
	case "18":
		return KeyF7
	case "19":
		return KeyF8
	case "20":
		return KeyF9
	case "21":
		return KeyF10
	case "23":
		return KeyF11
	case "24":
		return KeyF12
	}
	return ""
}

// splitMods splits "base[;modifier]" CSI params; a missing modifier
// means none, a present one must be 2-8 (checked by the caller).
func splitMods(params string) (base, mods string, ok bool) {
	if params == "" || params == "1" {
		return params, "", true
	}
	if !strings.Contains(params, ";") {
		return params, "", true
	}
	base, mods, ok = strings.Cut(params, ";")
	if !ok || strings.Contains(mods, ";") || base == "" {
		return "", "", false
	}
	return base, mods, true
}

// decodeMod maps CSI modifier numbers 2-8 to bits (value minus one:
// shift=1, alt=2, ctrl=4).
func decodeMod(mods string) (Mod, bool) {
	if mods == "" {
		return 0, true
	}
	n, err := strconv.Atoi(mods)
	if err != nil || n < 2 || n > 8 {
		return 0, false
	}
	// n-1 bitmask: bit0 shift, bit1 alt, bit2 ctrl.
	var m Mod
	if (n-1)&1 != 0 {
		m |= ModShift
	}
	if (n-1)&2 != 0 {
		m |= ModAlt
	}
	if (n-1)&4 != 0 {
		m |= ModCtrl
	}
	return m, true
}

// decodeCSIU decodes CSI u (kitty-style): codepoint[;mods[:kind]] u. Extra
// ";" params past the modifiers carry no key information and are ignored.
// The key event kind after ":" is press (1), repeat (2) or absent, all of
// which surface as the same Key; release (3) and anything else have no
// place in this model and fail closed as unknown.
func decodeCSIU(seq, params string) (Event, int, bool) {
	rest := params
	codeStr, rest, _ := strings.Cut(rest, ";")
	modKind, _, _ := strings.Cut(rest, ";")
	modStr, kindStr, _ := strings.Cut(modKind, ":")
	code, err := strconv.Atoi(codeStr)
	if err != nil || code < 0 || code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF) {
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
	mod, ok := decodeMod(modStr)
	if !ok {
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
	switch kindStr {
	case "", "1", "2":
		return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: mod, Rune: rune(code)}}, len(seq), false
	default:
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
}

// decodeModifyOther decodes ESC [ 27 ; mods ; keycode ~. ASCII printables
// (and tab) map to runes; other keycodes stay numeric under KeyExtended so
// the consumer can still forward them.
func decodeModifyOther(seq, params string) (Event, int, bool) {
	parts := strings.Split(params, ";")
	if len(parts) != 3 {
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
	mod, ok := decodeMod(parts[1])
	if !ok {
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
	keycode, err := strconv.Atoi(parts[2])
	if err != nil || keycode < 0 || keycode > 0x10FFFF {
		return Event{Kind: KindUnknown, Raw: seq}, len(seq), false
	}
	if keycode == '\t' || (keycode >= 0x20 && keycode <= 0x7E) {
		return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: mod, Rune: rune(keycode)}}, len(seq), false
	}
	return Event{Kind: KindKey, Key: Key{Code: KeyExtended, Mod: mod, Rune: rune(keycode)}}, len(seq), false
}

// stepSS3 classifies ESC O final: P-S are F1-F4, A-D and H/F are the
// application-cursor arrows and Home/End, anything else is Alt+O with
// the follower left pending.
func stepSS3(b []byte) (Event, int, bool) {
	if len(b) < 3 {
		return Event{}, 0, true
	}
	switch b[2] {
	case 'P':
		return Event{Kind: KindKey, Key: Key{Code: KeyF1}}, 3, false
	case 'Q':
		return Event{Kind: KindKey, Key: Key{Code: KeyF2}}, 3, false
	case 'R':
		return Event{Kind: KindKey, Key: Key{Code: KeyF3}}, 3, false
	case 'S':
		return Event{Kind: KindKey, Key: Key{Code: KeyF4}}, 3, false
	case 'A':
		return Event{Kind: KindKey, Key: Key{Code: KeyArrowUp}}, 3, false
	case 'B':
		return Event{Kind: KindKey, Key: Key{Code: KeyArrowDown}}, 3, false
	case 'C':
		return Event{Kind: KindKey, Key: Key{Code: KeyArrowRight}}, 3, false
	case 'D':
		return Event{Kind: KindKey, Key: Key{Code: KeyArrowLeft}}, 3, false
	case 'H':
		return Event{Kind: KindKey, Key: Key{Code: KeyHome}}, 3, false
	case 'F':
		return Event{Kind: KindKey, Key: Key{Code: KeyEnd}}, 3, false
	}
	return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: ModAlt, Rune: 'O'}}, 2, false
}

// stepAlt classifies ESC followed by one unit: a printable unit becomes an
// Alt+key, Tab/Backspace keep their code with Alt, anything else unknown.
func stepAlt(b []byte) (Event, int, bool) {
	c := b[1]
	switch {
	case c == '\t':
		return Event{Kind: KindKey, Key: Key{Code: KeyTab, Mod: ModAlt}}, 2, false
	case c == 0x7f || c == 0x08:
		return Event{Kind: KindKey, Key: Key{Code: KeyBackspace, Mod: ModAlt}}, 2, false
	case c >= 0x20 && c < 0x7f:
		return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: ModAlt, Rune: rune(c)}}, 2, false
	case c >= utf8.RuneSelf:
		if !utf8.FullRune(b[1:]) {
			return Event{}, 0, true
		}
		r, size := utf8.DecodeRune(b[1:])
		if r == utf8.RuneError && size == 1 {
			return Event{Kind: KindUnknown, Raw: string(b[:2])}, 2, false
		}
		if !unicode.IsPrint(r) {
			return Event{Kind: KindUnknown, Raw: string(b[:1+size])}, 1 + size, false
		}
		return Event{Kind: KindKey, Key: Key{Code: KeyRune, Mod: ModAlt, Rune: r}}, 1 + size, false
	default:
		return Event{Kind: KindUnknown, Raw: string(b[:2])}, 2, false
	}
}
