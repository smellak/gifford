package fuzz

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Prompt starts the fake harness's input line, and InputLine is the
// profile pattern that reads the draft after it. The prompt has no
// trailing space: a screen line loses its trailing spaces, and an empty
// draft must still match.
const (
	Prompt    = "fz>"
	InputLine = `^fz>(.*)$`
)

// Keys the fake harness understands outside a paste.
const (
	keySubmit    = '\r'
	keyBackspace = 0x7f
	keyCtrlH     = 0x08
	keyClearLine = 0x15
	keyCancel    = 0x03
	keyEOF       = 0x04
	keyEscape    = 0x1b
)

// Bracketed paste markers, as a terminal sends them around pasted text.
const (
	pasteStart = "\x1b[200~"
	pasteEnd   = "\x1b[201~"
)

// drawn ends every draw: an attribute reset changes nothing on screen,
// but it ends the text, so an emulator need not hold the last rune back
// for a combining mark that may follow. A real TUI ends its draws on a
// sequence too.
const drawn = "\x1b[0m"

// RecordKind names what the fake harness did with a line.
type RecordKind string

// Record kinds.
const (
	// RecordLine is a line submitted with Enter.
	RecordLine RecordKind = "line"
	// RecordCancel is a line dropped with Ctrl-C.
	RecordCancel RecordKind = "cancel"
	// RecordClear is a line erased with Ctrl-U.
	RecordClear RecordKind = "clear"
	// RecordEOF is a line still being edited when Ctrl-D ended the
	// harness.
	RecordEOF RecordKind = "eof"
)

// Record is one line the fake harness received, and what it did with it.
type Record struct {
	Kind RecordKind
	Text string
}

// String is the record as the harness writes it: the kind as a marker,
// then the line quoted, so any byte reads back exactly.
func (r Record) String() string { return string(r.Kind) + " " + strconv.Quote(r.Text) }

// errRecord marks a record file line that does not parse.
var errRecord = errors.New("fuzz: bad harness record")

// ParseRecords reads a record file back, one record per line.
func ParseRecords(data []byte) ([]Record, error) {
	var out []Record
	for n, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		kind, quoted, ok := strings.Cut(line, " ")
		if !ok {
			return nil, fmt.Errorf("%w: line %d has no text", errRecord, n+1)
		}
		switch RecordKind(kind) {
		case RecordLine, RecordCancel, RecordClear, RecordEOF:
		default:
			return nil, fmt.Errorf("%w: line %d has kind %q", errRecord, n+1, kind)
		}
		text, err := strconv.Unquote(quoted)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %w", errRecord, n+1, err)
		}
		out = append(out, Record{Kind: RecordKind(kind), Text: text})
	}
	return out, nil
}

// editor is the fake harness's line editor: one draft, edited with
// backspace, Ctrl-U, Ctrl-C and bracketed paste, sent with Enter. It
// keeps every line it was given as a record, and draws the draft on one
// screen line after the prompt. Feeding the same bytes in any chunking
// gives the same records: an escape sequence or paste marker cut between
// two feeds waits for the rest.
type editor struct {
	draft   []byte
	pending []byte
	pasting bool
	dirty   bool
	eof     bool
	records []Record
	screen  []byte
}

// start is what the harness draws once it reads keys raw: a ready line
// and the first prompt.
func (e *editor) start() []byte { return []byte("fz ready\r\n" + Prompt + drawn) }

// feed edits the draft with data. It returns the screen bytes that draw
// the result and the records the data completed. After Ctrl-D it takes
// nothing more.
func (e *editor) feed(data []byte) ([]byte, []Record) {
	if e.eof {
		return nil, nil
	}
	data = append(e.pending, data...)
	e.pending = nil
	for len(data) > 0 && !e.eof {
		if e.pasting {
			data = e.paste(data)
			continue
		}
		data = e.key(data)
	}
	if e.dirty {
		e.redraw()
	}
	if len(e.screen) > 0 {
		e.screen = append(e.screen, drawn...)
	}
	screen, records := e.screen, e.records
	e.screen, e.records = nil, nil
	return screen, records
}

// paste takes pasted bytes literally up to the end marker. A tail that
// may begin the marker waits for the next feed.
func (e *editor) paste(data []byte) []byte {
	if i := bytes.Index(data, []byte(pasteEnd)); i >= 0 {
		e.insert(data[:i])
		e.pasting = false
		return data[i+len(pasteEnd):]
	}
	keep := 0
	for n := min(len(data), len(pasteEnd)-1); n > 0; n-- {
		if bytes.HasSuffix(data, []byte(pasteEnd[:n])) {
			keep = n
			break
		}
	}
	e.insert(data[:len(data)-keep])
	e.pending = append(e.pending, data[len(data)-keep:]...)
	return nil
}

// key takes one key off the head of data and returns the rest.
func (e *editor) key(data []byte) []byte {
	switch data[0] {
	case keySubmit:
		e.finish(RecordLine, "\r\n")
	case keyBackspace, keyCtrlH:
		if len(e.draft) > 0 {
			_, size := utf8.DecodeLastRune(e.draft)
			e.draft = e.draft[:len(e.draft)-size]
			e.dirty = true
		}
	case keyClearLine:
		e.records = append(e.records, Record{Kind: RecordClear, Text: string(e.draft)})
		e.draft = nil
		e.dirty = true
	case keyCancel:
		e.finish(RecordCancel, "^C\r\n")
	case keyEOF:
		if len(e.draft) > 0 {
			e.records = append(e.records, Record{Kind: RecordEOF, Text: string(e.draft)})
		}
		e.eof = true
	case keyEscape:
		return e.escape(data)
	default:
		e.insert(data[:1])
	}
	return data[1:]
}

// escape takes one escape sequence: a paste start opens a paste, any
// other CSI sequence, an SS3 key or an Alt key, the escape and one whole
// rune, is a key the harness ignores. A sequence cut short, a rune
// included, waits for the next feed.
func (e *editor) escape(data []byte) []byte {
	if len(data) < 2 {
		e.pending = append(e.pending, data...)
		return nil
	}
	switch data[1] {
	case '[':
		for i := 2; i < len(data); i++ {
			if c := data[i]; c >= 0x40 && c <= 0x7e {
				if string(data[:i+1]) == pasteStart {
					e.pasting = true
				}
				return data[i+1:]
			}
		}
	case 'O':
		if len(data) >= 3 {
			return data[3:]
		}
	default:
		if utf8.FullRune(data[1:]) {
			_, size := utf8.DecodeRune(data[1:])
			return data[1+size:]
		}
	}
	e.pending = append(e.pending, data...)
	return nil
}

// insert adds bytes to the draft as they came.
func (e *editor) insert(b []byte) {
	if len(b) == 0 {
		return
	}
	e.draft = append(e.draft, b...)
	e.dirty = true
}

// finish ends the draft with a record of kind, draws its last state,
// then end and a fresh prompt.
func (e *editor) finish(kind RecordKind, end string) {
	if e.dirty {
		e.redraw()
	}
	e.records = append(e.records, Record{Kind: kind, Text: string(e.draft)})
	e.draft = nil
	e.screen = append(e.screen, end+Prompt...)
}

// redraw draws the whole input line again. A control byte shows as ?,
// so the draft stays on its one line whatever it holds.
func (e *editor) redraw() {
	e.screen = append(e.screen, "\r\x1b[K"+Prompt...)
	for _, r := range string(e.draft) {
		if r < 0x20 || r == 0x7f || r == utf8.RuneError {
			r = '?'
		}
		e.screen = utf8.AppendRune(e.screen, r)
	}
	e.dirty = false
}

// Expected is what the fake harness records when it gets keys alone,
// with nothing else typed in between: what the human meant to send.
func Expected(keys []byte) []Record {
	var e editor
	_, records := e.feed(keys)
	return records
}
