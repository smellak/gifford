package redacttest

import (
	embed "embed"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

//go:embed testdata/*.screen testdata/messages.text
var testdata embed.FS

// screenStems lists the fixture screens in Screens order with the
// synthetic harness each one targets.
func screenStems() []struct {
	stem    string
	harness protocol.Harness
} {
	return []struct {
		stem    string
		harness protocol.Harness
	}{
		{"claude-code", protocol.HarnessClaudeCode},
		{"codex", protocol.HarnessCodex},
		{"muse-code", protocol.HarnessMuseCode},
		{"opencode", protocol.HarnessOpencode},
		{"antigravity", protocol.HarnessAntigravity},
	}
}

// SeededScreen is one filled harness screen: the template with every slot
// replaced, the wanted output, the expected substitution counts and the
// secret fragments a test must never find in the output.
type SeededScreen struct {
	Name    string
	Harness protocol.Harness
	Input   redact.Screen
	Want    []string
	Counts  redact.Counts
	Secrets []string
}

// SeededText is one filled free-text fixture entry.
type SeededText struct {
	Name    string
	Input   string
	Want    string
	Counts  redact.Counts
	Secrets []string
}

// Screens loads the five seeded harness screens in file order.
func Screens() ([]SeededScreen, error) {
	files := screenStems()
	out := make([]SeededScreen, 0, len(files))
	for _, sf := range files {
		raw, err := testdata.ReadFile("testdata/" + sf.stem + ".screen")
		if err != nil {
			return nil, fmt.Errorf("redacttest: read %s.screen: %w", sf.stem, err)
		}
		screen, err := parseScreen(sf.stem+".screen", sf.harness, string(raw))
		if err != nil {
			return nil, err
		}
		out = append(out, screen)
	}
	return out, nil
}

// Texts loads the free-text fixture entries in file order.
func Texts() ([]SeededText, error) {
	raw, err := testdata.ReadFile("testdata/messages.text")
	if err != nil {
		return nil, fmt.Errorf("redacttest: read messages.text: %w", err)
	}
	return parseTexts("messages.text", string(raw))
}

// slot is one parsed template slot.
type slot struct {
	kind   string
	name   string
	line   int
	sliced bool
	start  int
	end    int
	endSet bool
}

// filled is one slot replaced with its value on both sides. Only kind
// slots count; cont fragments mask without counting.
type filled struct {
	value   string
	masked  bool
	counted bool
	kind    redact.Kind
}

// parseScreen fills one screen template. The template is the golden:
// Want follows from the slots, so no golden copy can drift.
func parseScreen(file string, harness protocol.Harness, raw string) (SeededScreen, error) {
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if len(lines) == 0 {
		return SeededScreen{}, fmt.Errorf("redacttest: %s: missing header", file)
	}
	cols, rows, err := parseHeader(file, lines[0])
	if err != nil {
		return SeededScreen{}, err
	}
	if len(lines)-1 != rows {
		return SeededScreen{}, fmt.Errorf(
			"redacttest: %s: want %d rows, have %d", file, rows, len(lines)-1,
		)
	}
	inputs := make([]string, 0, rows)
	wants := make([]string, 0, rows)
	counts := redact.Counts{}
	byKind := make(map[redact.Kind]int64)
	var secrets []string
	for i, template := range lines[1:] {
		input, want, fills, err := fillRow(file, i, template)
		if err != nil {
			return SeededScreen{}, err
		}
		if redact.CellWidth(input) > cols {
			return SeededScreen{}, fmt.Errorf(
				"redacttest: %s row %d: %d cells wider than %d", file, i, redact.CellWidth(input), cols,
			)
		}
		inputs = append(inputs, padRow(input, cols))
		wants = append(wants, padRow(want, cols))
		for _, f := range fills {
			if f.counted {
				counts.Total++
				byKind[f.kind]++
			}
			if f.masked && utf8.RuneCountInString(f.value) >= 4 {
				secrets = append(secrets, f.value)
			}
		}
	}
	if counts.Total > 0 {
		counts.ByKind = byKind
	}
	return SeededScreen{
		Name:    strings.TrimSuffix(file, ".screen"),
		Harness: harness,
		Input:   redact.Screen{Rows: inputs, Cols: cols},
		Want:    wants,
		Counts:  counts,
		Secrets: secrets,
	}, nil
}

// parseHeader reads the fixture header line.
func parseHeader(file, header string) (int, int, error) {
	rest, ok := strings.CutPrefix(header, "gifford-redact-fixture v1 screen cols=")
	if !ok {
		return 0, 0, fmt.Errorf("redacttest: %s: bad header %q", file, header)
	}
	colsText, rowsText, ok := strings.Cut(rest, " rows=")
	if !ok {
		return 0, 0, fmt.Errorf("redacttest: %s: bad header %q", file, header)
	}
	cols, err := strconv.Atoi(colsText)
	if err != nil || cols <= 0 {
		return 0, 0, fmt.Errorf("redacttest: %s: bad cols %q", file, colsText)
	}
	nrows, err := strconv.Atoi(rowsText)
	if err != nil || nrows <= 0 {
		return 0, 0, fmt.Errorf("redacttest: %s: bad rows %q", file, rowsText)
	}
	return cols, nrows, nil
}

// padRow right-pads a row with spaces to the screen width.
func padRow(row string, cols int) string {
	if n := cols - redact.CellWidth(row); n > 0 {
		return row + strings.Repeat(" ", n)
	}
	return row
}

// fillRow replaces every slot of one template row on both sides.
func fillRow(file string, row int, template string) (string, string, []filled, error) {
	var input, want strings.Builder
	fills := make([]filled, 0, strings.Count(template, "{{"))
	rest := template
	for {
		open := strings.Index(rest, "{{")
		if open < 0 {
			input.WriteString(rest)
			want.WriteString(rest)
			return input.String(), want.String(), fills, nil
		}
		finish := strings.Index(rest[open:], "}}")
		if finish < 0 {
			return "", "", nil, fmt.Errorf("redacttest: %s row %d: unterminated slot", file, row)
		}
		finish += open
		slot, err := parseSlot(file, row, rest[open+2:finish])
		if err != nil {
			return "", "", nil, err
		}
		value, err := resolveSlot(file, row, slot)
		if err != nil {
			return "", "", nil, err
		}
		input.WriteString(rest[:open])
		want.WriteString(rest[:open])
		input.WriteString(value)
		f := filled{value: value}
		switch slot.kind {
		case "keep":
			want.WriteString(value)
		case "cont":
			want.WriteString(placeholders(value))
			f.masked = true
		default:
			want.WriteString(placeholders(value))
			f.masked = true
			f.counted = true
			f.kind = redact.Kind(slot.kind)
		}
		fills = append(fills, f)
		rest = rest[finish+2:]
	}
}

// placeholders returns one placeholder per input cell.
func placeholders(value string) string {
	return strings.Repeat(string(redact.Placeholder), redact.CellWidth(value))
}

// parseSlot parses the inside of one slot: kind:name[#line][[a:b]].
func parseSlot(file string, row int, inner string) (slot, error) {
	fail := func(reason string) (slot, error) {
		return slot{}, fmt.Errorf("redacttest: %s row %d: bad slot {{%s}}: %s", file, row, inner, reason)
	}
	kind, rest, ok := strings.Cut(inner, ":")
	if !ok || kind == "" {
		return fail("missing kind")
	}
	if kind != "cont" && kind != "keep" && !redact.Kind(kind).Valid() {
		return fail("unknown kind")
	}
	s := slot{kind: kind, line: -1}
	name := rest
	if i := strings.IndexAny(name, "#["); i >= 0 {
		s.name = name[:i]
		rest = name[i:]
	} else {
		s.name = name
		rest = ""
	}
	if s.name == "" {
		return fail("missing name")
	}
	for len(rest) > 0 {
		switch rest[0] {
		case '#':
			end := strings.IndexByte(rest, '[')
			num := rest[1:]
			if end >= 0 {
				num = rest[1:end]
				rest = rest[end:]
			} else {
				rest = ""
			}
			n, err := strconv.Atoi(num)
			if err != nil || n < 0 {
				return fail("bad line")
			}
			s.line = n
		case '[':
			finish := strings.IndexByte(rest, ']')
			if finish < 0 {
				return fail("bad slice")
			}
			bounds := rest[1:finish]
			rest = rest[finish+1:]
			a, b, ok := strings.Cut(bounds, ":")
			if !ok {
				return fail("bad slice")
			}
			s.sliced = true
			var err error
			if s.start, err = atoiOpt(a); err != nil {
				return fail("bad slice")
			}
			if s.end, s.endSet, err = atoiEnd(b); err != nil {
				return fail("bad slice")
			}
		default:
			return fail("bad slot")
		}
	}
	return s, nil
}

// atoiOpt parses an optional bound, defaulting to zero.
func atoiOpt(text string) (int, error) {
	if text == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("bad number %q: %w", text, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative number %q", text)
	}
	return n, nil
}

// atoiEnd parses the optional slice end, reporting whether it was set.
func atoiEnd(text string) (int, bool, error) {
	if text == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return 0, false, fmt.Errorf("bad number %q: %w", text, err)
	}
	if n < 0 {
		return 0, false, fmt.Errorf("negative number %q", text)
	}
	return n, true, nil
}

// resolveSlot fills one slot from the fixture values.
func resolveSlot(file string, row int, s slot) (string, error) {
	fail := func(reason string) (string, error) {
		return "", fmt.Errorf("redacttest: %s row %d: value %q: %s", file, row, s.name, reason)
	}
	value, err := Value(s.name)
	if err != nil {
		return "", fmt.Errorf("redacttest: %s row %d: %w", file, row, err)
	}
	if strings.Contains(value, "\n") {
		if s.line < 0 {
			return fail("multi-line value needs #line")
		}
		picked := strings.Split(value, "\n")
		if s.line >= len(picked) {
			return fail("bad #line")
		}
		value = picked[s.line]
	} else if s.line >= 0 {
		return fail("bad #line")
	}
	if s.sliced {
		runes := []rune(value)
		end := len(runes)
		if s.endSet {
			end = s.end
		}
		if s.start > end || end > len(runes) {
			return fail("bad slice")
		}
		value = string(runes[s.start:end])
	}
	return value, nil
}

// parseTexts fills the free-text entries opened by == name lines.
func parseTexts(file, raw string) ([]SeededText, error) {
	var out []SeededText
	var name string
	var rows []string
	flush := func() error {
		if name == "" {
			return nil
		}
		input, want, counts, secrets, err := fillText(file, name, rows)
		if err != nil {
			return err
		}
		out = append(out, SeededText{Name: name, Input: input, Want: want, Counts: counts, Secrets: secrets})
		name = ""
		rows = nil
		return nil
	}
	for i, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		if rest, ok := strings.CutPrefix(line, "== "); ok {
			if err := flush(); err != nil {
				return nil, err
			}
			name = rest
			if name == "" {
				return nil, fmt.Errorf("redacttest: %s line %d: empty entry name", file, i)
			}
			continue
		}
		if name == "" {
			return nil, fmt.Errorf("redacttest: %s line %d: text outside an entry", file, i)
		}
		rows = append(rows, line)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// fillText fills one text entry, joining its lines with newlines.
func fillText(file, name string, rows []string) (string, string, redact.Counts, []string, error) {
	inputs := make([]string, 0, len(rows))
	wants := make([]string, 0, len(rows))
	counts := redact.Counts{}
	byKind := make(map[redact.Kind]int64)
	var secrets []string
	for i, template := range rows {
		input, want, fills, err := fillRow(file, i, template)
		if err != nil {
			return "", "", counts, nil, fmt.Errorf("redacttest: entry %s: %w", name, err)
		}
		inputs = append(inputs, input)
		wants = append(wants, want)
		for _, f := range fills {
			if f.counted {
				counts.Total++
				byKind[f.kind]++
			}
			if f.masked && utf8.RuneCountInString(f.value) >= 4 {
				secrets = append(secrets, f.value)
			}
		}
	}
	if counts.Total > 0 {
		counts.ByKind = byKind
	}
	return strings.Join(inputs, "\n"), strings.Join(wants, "\n"), counts, secrets, nil
}
