package redact

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// placeholder is the Placeholder rune as a string for masking output.
const placeholder = "•"

// span is one masked byte range inside a logical line. Offsets are bytes:
// a span off a rune boundary or outside its line fails the call.
type span struct {
	line       int
	start, end int
}

// finding is one substitution: one or more spans of one kind, counted
// once. Multi-line findings (a key block, a wrapped tail) mask every part
// but count a single substitution.
type finding struct {
	kind  Kind
	parts []span
}

// rule reports findings over the logical lines of one call. Rules see
// validated input only; a rule that panics or reports a bad span fails the
// call with ErrInternal.
type rule interface {
	find(lines []string) []finding
}

// ruleSet is the versioned rule bundle behind a Redactor. The v1 rules
// arrive with New; tests use their own versioned bundles.
type ruleSet struct {
	version string
	rules   []rule
}

// rowSeg maps one slice of a logical line back to its screen row.
type rowSeg struct {
	row        int
	start, end int
}

// maskedLine is one masked logical line. starts maps each input rune to
// the output rune where it begins, so rows split back on rune counts: a
// placeholder is three bytes and byte offsets do not survive masking.
type maskedLine struct {
	text   string
	starts []int
}

// run executes the bundle over the logical lines, masks every finding and
// counts one substitution per kind per finding: a finding counts only when
// no earlier finding masked any of its bytes, else it only widens the
// mask. op names the caller for error text.
func run(lines []string, rs *ruleSet, op string) ([]maskedLine, Counts, error) {
	findings := make([]finding, 0, len(rs.rules))
	for _, ru := range rs.rules {
		findings = append(findings, ru.find(lines)...)
	}
	for _, f := range findings {
		if !f.kind.Valid() {
			return nil, Counts{}, internalf(op, "unknown kind")
		}
		if len(f.parts) == 0 {
			return nil, Counts{}, internalf(op, "finding without spans")
		}
	}
	sort.SliceStable(findings, func(i, j int) bool {
		pi, pj := findings[i].kind.priority(), findings[j].kind.priority()
		if pi != pj {
			return pi < pj
		}
		ai, aj := findings[i].parts[0], findings[j].parts[0]
		if ai.line != aj.line {
			return ai.line < aj.line
		}
		return ai.start < aj.start
	})
	masked := make([][]bool, len(lines))
	for i, line := range lines {
		masked[i] = make([]bool, len(line))
	}
	counts := Counts{}
	byKind := make(map[Kind]int64)
	for _, f := range findings {
		ext, err := extendParts(op, lines, f.parts)
		if err != nil {
			return nil, Counts{}, err
		}
		fresh := true
		for _, sp := range ext {
			for b := sp.start; b < sp.end; b++ {
				if masked[sp.line][b] {
					fresh = false
					break
				}
			}
			if !fresh {
				break
			}
		}
		for _, sp := range ext {
			for b := sp.start; b < sp.end; b++ {
				masked[sp.line][b] = true
			}
		}
		if fresh {
			counts.Total++
			byKind[f.kind]++
		}
	}
	if counts.Total > 0 {
		counts.ByKind = byKind
	}
	out := make([]maskedLine, len(lines))
	for i, line := range lines {
		out[i] = maskLine(line, masked[i])
	}
	return out, counts, nil
}

// internalf builds an ErrInternal error naming the operation.
func internalf(op, reason string) error {
	return fmt.Errorf("%s: internal failure: %s: %w", op, reason, ErrInternal)
}

// extendParts validates every part and extends it over the combining marks
// after it. Cluster edges fail with ErrUnsupportedInput and the zero
// result; malformed spans fail with ErrInternal.
func extendParts(op string, lines []string, parts []span) ([]span, error) {
	ext := make([]span, len(parts))
	for i, sp := range parts {
		if sp.line < 0 || sp.line >= len(lines) {
			return nil, internalf(op, "span outside its lines")
		}
		line := lines[sp.line]
		n := len(line)
		if sp.start < 0 || sp.end > n || sp.start >= sp.end {
			return nil, internalf(op, "span outside its line")
		}
		if (sp.start > 0 && !utf8.RuneStart(line[sp.start])) ||
			(sp.end < n && !utf8.RuneStart(line[sp.end])) {
			return nil, internalf(op, "span off a rune boundary")
		}
		end, err := extendEnd(op, line, sp.start, sp.end)
		if err != nil {
			return nil, err
		}
		ext[i] = span{line: sp.line, start: sp.start, end: end}
	}
	return ext, nil
}

// extendEnd absorbs the combining marks after the span, then enforces the
// cluster edges: a cluster-forming rune inside, an extender right after,
// or a ZWJ right before fails closed.
func extendEnd(op, line string, start, end int) (int, error) {
	for _, r := range line[start:end] {
		if isClusterForming(r) {
			return 0, fmt.Errorf(
				"%s: unsupported input at bytes %d:%d: cluster in span: %w",
				op, start, end, ErrUnsupportedInput,
			)
		}
	}
	for end < len(line) {
		r, size := utf8.DecodeRuneInString(line[end:])
		if !isMark(r) {
			if isExtenderAfter(r) {
				return 0, fmt.Errorf(
					"%s: unsupported input at bytes %d:%d: extender after span: %w",
					op, start, end, ErrUnsupportedInput,
				)
			}
			break
		}
		if isClusterForming(r) {
			return 0, fmt.Errorf(
				"%s: unsupported input at bytes %d:%d: cluster in span: %w",
				op, start, end, ErrUnsupportedInput,
			)
		}
		end += size
	}
	if start > 0 {
		if r, _ := utf8.DecodeLastRuneInString(line[:start]); r == 0x200d {
			return 0, fmt.Errorf(
				"%s: unsupported input at bytes %d:%d: joiner before span: %w",
				op, start, end, ErrUnsupportedInput,
			)
		}
	}
	return end, nil
}

// isMark reports the combining marks an extended span absorbs: Mn and Me
// vanish into the mask with width 0. Cf (ZWJ, BOM) is cluster-forming and
// never absorbed: a joiner after a span fails the call instead.
func isMark(r rune) bool {
	return unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r)
}

// maskLine replaces every masked rune with its CellWidth in placeholders,
// copying every other byte as it is. Absorbed marks have width 0 and leave
// no placeholder behind. starts records where each input rune begins in
// the output runes.
func maskLine(line string, masked []bool) maskedLine {
	var sb strings.Builder
	sb.Grow(len(line) + 8)
	var starts []int
	out := 0
	for i, r := range line {
		size := utf8.RuneLen(r)
		hit := false
		for b := i; b < i+size; b++ {
			if masked[b] {
				hit = true
				break
			}
		}
		starts = append(starts, out)
		if !hit {
			sb.WriteRune(r)
			out++
			continue
		}
		for range runeWidth(r) {
			sb.WriteString(placeholder)
			out++
		}
	}
	return maskedLine{text: sb.String(), starts: starts}
}

// splitRows cuts masked logical lines back into screen rows. Row bounds
// are byte offsets in the input lines; the masked text is cut on rune
// counts through the starts map in one pass per line.
func splitRows(lines []string, masked []maskedLine, segs [][]rowSeg, nrows int) []string {
	rows := make([]string, nrows)
	for li, line := range lines {
		out := []rune(masked[li].text)
		starts := masked[li].starts
		at := func(n int) int {
			if n >= len(starts) {
				return len(out)
			}
			return starts[n]
		}
		n := 0
		off := 0
		for _, sg := range segs[li] {
			start := n
			for off < sg.end {
				_, size := utf8.DecodeRuneInString(line[off:])
				off += size
				n++
			}
			rows[sg.row] = string(out[at(start):at(n)])
		}
	}
	return rows
}

// joinRows groups screen rows into logical lines: consecutive rows join
// without separator while the last row of the group is soft-wrapped (Cols
// known, at least Cols cells wide, last rune not blank). It returns the
// line texts with the segments mapping each slice back to its row.
func joinRows(rows []string, cols int) ([]string, [][]rowSeg) {
	var lines []string
	var allSegs [][]rowSeg
	i := 0
	for i < len(rows) {
		j := i
		for cols > 0 && j+1 < len(rows) && isSoftWrapped(rows[j], cols) {
			j++
		}
		var sb strings.Builder
		var segs []rowSeg
		off := 0
		for k := i; k <= j; k++ {
			sb.WriteString(rows[k])
			segs = append(segs, rowSeg{row: k, start: off, end: off + len(rows[k])})
			off += len(rows[k])
		}
		lines = append(lines, sb.String())
		allSegs = append(allSegs, segs)
		i = j + 1
	}
	return lines, allSegs
}

// isSoftWrapped reports whether row continues on the next row: the width
// is known, the row fills it, and its last rune is not blank, so a padded
// row or a TUI frame never joins.
func isSoftWrapped(row string, cols int) bool {
	if row == "" || CellWidth(row) < cols {
		return false
	}
	last, _ := utf8.DecodeLastRuneInString(row)
	return !isBlankRune(last)
}
