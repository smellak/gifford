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
	var pathRules []*pathsRule
	for _, ru := range rs.rules {
		if pr, ok := ru.(*pathsRule); ok {
			pathRules = append(pathRules, pr)
			continue
		}
		findings = append(findings, ru.find(lines)...)
	}
	if len(pathRules) > 0 {
		covered := coveredRanges(lines, findings)
		for _, pr := range pathRules {
			found, _ := pr.findCovered(lines, covered)
			findings = append(findings, found...)
		}
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
	// skip marks the bytes every finding owns: each line byte is visited
	// once no matter how many spans cover it, so nested findings cost
	// near-constant time per span plus once per masked byte. The fresh
	// check below is the historical per-byte loop with its early break,
	// unchanged.
	masked := make([][]bool, len(lines))
	skip := make([]*coverage, len(lines))
	// info holds the per-line span data, built once when findings exist:
	// the ascii flag and the cell-width prefix sums behind every
	// zero-cell check below.
	var info []lineSpans
	if len(findings) > 0 {
		info = make([]lineSpans, len(lines))
		for i, line := range lines {
			plain := true
			for j := range len(line) {
				if line[j] >= utf8.RuneSelf {
					plain = false
					break
				}
			}
			if plain {
				info[i] = lineSpans{ascii: true, wpref: widthPrefix(line)}
				continue
			}
			info[i] = lineSpans{cpref: clusterPrefix(line), wpref: widthPrefix(line)}
		}
	}
	for i, line := range lines {
		masked[i] = make([]bool, len(line))
		skip[i] = newCoverage(len(line))
	}
	counts := Counts{}
	byKind := make(map[Kind]int64)
	for _, f := range findings {
		ext, err := extendParts(op, lines, info, f.parts)
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
			skip[sp.line].markEach(sp.start, sp.end, func(b int) {
				masked[sp.line][b] = true
			})
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

// coveredRanges returns, per line, the byte ranges masked by non-path
// findings, merged and sorted. Path boundaries yield to covered runes;
// kinds 1-6 are every kind but the path one, continuation parts included.
func coveredRanges(lines []string, findings []finding) [][][2]int {
	covered := make([][][2]int, len(lines))
	for _, f := range findings {
		if f.kind == KindSensitivePath {
			continue
		}
		for _, sp := range f.parts {
			if sp.line < 0 || sp.line >= len(lines) {
				continue
			}
			start, end := sp.start, sp.end
			if start < 0 {
				start = 0
			}
			if end > len(lines[sp.line]) {
				end = len(lines[sp.line])
			}
			if start >= end {
				continue
			}
			covered[sp.line] = append(covered[sp.line], [2]int{start, end})
		}
	}
	for li := range covered {
		covered[li] = mergeRanges(covered[li])
	}
	return covered
}

// mergeRanges sorts ranges by start, merging overlapping or adjacent ones.
func mergeRanges(ranges [][2]int) [][2]int {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	out := ranges[:0]
	for _, r := range ranges {
		if n := len(out); n > 0 && r[0] <= out[n-1][1] {
			if r[1] > out[n-1][1] {
				out[n-1][1] = r[1]
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// internalf builds an ErrInternal error naming the operation.
func internalf(op, reason string) error {
	return fmt.Errorf("%s: internal failure: %s: %w", op, reason, ErrInternal)
}

// lineSpans carries the per-line data extendParts needs: ascii reports
// the all-ASCII lines, while cpref counts the cluster-forming runes of a
// non-ASCII line, built once, and wpref holds the cell-width prefix sums,
// so the zero-cell check of any rune-aligned span is one subtraction.
// markEnds maps every byte offset to the end of the absorbable mark run
// starting there, built lazily on the first span that needs it; walked
// counts the runes the absorption examined past span ends and markBuilds
// the table builds, both for the deterministic regression test.
type lineSpans struct {
	ascii      bool
	cpref      []int
	wpref      []int
	markEnds   []int
	walked     int
	markBuilds int
}

// extendParts validates every part and extends it over the combining marks
// after it. Cluster edges fail with ErrUnsupportedInput and the zero
// result; malformed spans fail with ErrInternal.
func extendParts(op string, lines []string, info []lineSpans, parts []span) ([]span, error) {
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
		// The table lives in the call's per-line slot, by pointer: a
		// table built on a copy would be rebuilt for every span.
		spans := &lineSpans{}
		if sp.line < len(info) {
			spans = &info[sp.line]
		}
		end, err := extendEnd(op, line, spans, sp.start, sp.end)
		if err != nil {
			return nil, err
		}
		ext[i] = span{line: sp.line, start: sp.start, end: end}
	}
	return ext, nil
}

// extendEnd absorbs the combining marks after the span, then enforces the
// cluster edges: a cluster-forming rune inside, an extender right after,
// or a ZWJ right before fails closed. An all-ASCII line holds no
// cluster-forming rune, so the in-span scan is skipped; otherwise it is
// one prefix subtraction. A span ending before a mark run jumps over it
// with the line table; a run holding a cluster-forming rune keeps today's
// rune walk, unchanged, which fails closed at the same rune.
func extendEnd(op, line string, spans *lineSpans, start, end int) (int, error) {
	if clusterInSpan(line, *spans, start, end) {
		return 0, fmt.Errorf(
			"%s: unsupported input at bytes %d:%d: cluster in span: %w",
			op, start, end, ErrUnsupportedInput,
		)
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
		if mend, ok := markJump(line, spans, end); ok {
			spans.walked++
			end = mend
			continue
		}
		if isClusterForming(r) {
			return 0, fmt.Errorf(
				"%s: unsupported input at bytes %d:%d: cluster in span: %w",
				op, start, end, ErrUnsupportedInput,
			)
		}
		spans.walked++
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
	if end > start && spanWidth(line, spans.wpref, start, end) == 0 {
		return 0, fmt.Errorf(
			"%s: unsupported input at bytes %d:%d: zero-cell span: %w",
			op, start, end, ErrUnsupportedInput,
		)
	}
	return end, nil
}

// markJump moves end past the absorbable mark run starting there,
// building the line table on first use. It reports false when the run
// holds a cluster-forming rune or the line carries no cluster table: the
// caller then walks today's loop, which fails closed at the same rune.
// A jump lands on a non-mark, so the loop applies the extender check
// exactly as today.
func markJump(line string, spans *lineSpans, end int) (int, bool) {
	if len(spans.cpref) != len(line)+1 {
		return 0, false
	}
	if spans.markEnds == nil {
		spans.markEnds = markRunEnds(line)
		spans.markBuilds++
	}
	mend := spans.markEnds[end]
	if mend <= end || mend > len(line) {
		return 0, false
	}
	if spans.cpref[mend]-spans.cpref[end] > 0 {
		return 0, false
	}
	return mend, true
}

// markRunEnds maps every byte offset of line to the end of the run of
// absorbable marks (isMark: Mn, Me) starting there, or to the offset
// itself. One right-to-left pass: valid UTF-8 peels the same rune
// boundaries either way, and both callers reject invalid input before
// any rule runs. Continuation bytes map to themselves and are never
// queried; a query there behaves like today's loop.
func markRunEnds(line string) []int {
	ends := make([]int, len(line)+1)
	ends[len(line)] = len(line)
	runEnd := len(line)
	for i := len(line); i > 0; {
		r, size := utf8.DecodeLastRuneInString(line[:i])
		start := i - size
		for j := start + 1; j < i; j++ {
			ends[j] = j
		}
		if isMark(r) {
			ends[start] = runEnd
		} else {
			runEnd = start
			ends[start] = start
		}
		i = start
	}
	return ends
}

// clusterInSpan reports whether [start, end) holds a cluster-forming
// rune: never on an all-ASCII line, one prefix subtraction with the
// tables, today's rune scan without them.
func clusterInSpan(line string, spans lineSpans, start, end int) bool {
	if spans.ascii {
		return false
	}
	if len(spans.cpref) == len(line)+1 {
		return spans.cpref[end]-spans.cpref[start] > 0
	}
	for _, r := range line[start:end] {
		if isClusterForming(r) {
			return true
		}
	}
	return false
}

// clusterPrefix counts the cluster-forming runes of line: pref[i] is the
// count in line[:i] for rune-aligned i, so the count in any rune-aligned
// span is one subtraction. Continuation bytes carry their rune start's
// count and are never queried.
func clusterPrefix(line string) []int {
	pref := make([]int, len(line)+1)
	for i := 0; i < len(line); {
		r, size := utf8.DecodeRuneInString(line[i:])
		n := 0
		if isClusterForming(r) {
			n = 1
		}
		for k := 1; k < size; k++ {
			pref[i+k] = pref[i]
		}
		pref[i+size] = pref[i] + n
		i += size
	}
	return pref
}

// spanWidth returns the cell width of the rune-aligned line[start:end]:
// one subtraction on the line prefix sums, or the direct sum without
// them.
func spanWidth(line string, wpref []int, start, end int) int {
	if len(wpref) == len(line)+1 && 0 <= start && start <= end && end <= len(line) {
		return wpref[end] - wpref[start]
	}
	return CellWidth(line[start:end])
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
