package redact

import (
	"regexp"
	"sort"
)

// pemBegin matches a private key opening marker; pemEnd the closing one.
// Certificates, public keys and other labels never match.
const pemBegin = `-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?-----`

const pemEnd = `-----END (?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?-----`

// pemBase64Run matches one base64 run; pemBodyLine a full body line with
// optional frame padding; pemHeaderLine an encrypted-section header before
// the first body line.
const pemBase64Run = `[A-Za-z0-9+/=]+`

const pemBodyLine = `^[ \x{2500}-\x{259F}]*[A-Za-z0-9+/=]+[ \x{2500}-\x{259F}]*\r?$`

const pemHeaderLine = `^[ \x{2500}-\x{259F}]*[A-Za-z][A-Za-z0-9-]*: `

// pemMarker is one BEGIN or END occurrence.
type pemMarker struct {
	line    int
	start   int
	end     int
	isBegin bool
}

// pemRule masks private key blocks in their four forms, one substitution
// per block.
type pemRule struct {
	begin  *regexp.Regexp
	end    *regexp.Regexp
	base   *regexp.Regexp
	body   *regexp.Regexp
	header *regexp.Regexp
}

func (p *pemRule) find(lines []string) []finding {
	markers := p.scan(lines)
	claimed := make([][][2]int, len(lines))
	out := make([]finding, 0, len(markers))
	for i, m := range markers {
		if claimedAt(claimed[m.line], m.start, m.end) {
			continue
		}
		out = append(out, p.block(lines, markers, i, claimed))
	}
	return out
}

// claimedAt reports whether [start, end) overlaps a claimed range, so a
// block never reuses bytes an earlier block owns. After a block, scanning
// resumes at the cursor on the same line.
func claimedAt(ranges [][2]int, start, end int) bool {
	for _, r := range ranges {
		if start < r[1] && r[0] < end {
			return true
		}
	}
	return false
}

// claimRange marks [start, end) of one line owned by a block.
func claimRange(claimed [][][2]int, line, start, end int) {
	claimed[line] = append(claimed[line], [2]int{start, end})
}

// claimLine marks a whole line owned by a block.
func claimLine(claimed [][][2]int, lines []string, li int) {
	claimRange(claimed, li, 0, len(lines[li]))
}

// scan collects every marker in document order, byte order within a line.
func (p *pemRule) scan(lines []string) []pemMarker {
	markers := make([]pemMarker, 0, len(lines))
	for li, line := range lines {
		for _, m := range p.begin.FindAllStringIndex(line, -1) {
			markers = append(markers, pemMarker{line: li, start: m[0], end: m[1], isBegin: true})
		}
		for _, m := range p.end.FindAllStringIndex(line, -1) {
			markers = append(markers, pemMarker{line: li, start: m[0], end: m[1]})
		}
	}
	sort.SliceStable(markers, func(i, j int) bool {
		if markers[i].line != markers[j].line {
			return markers[i].line < markers[j].line
		}
		return markers[i].start < markers[j].start
	})
	return markers
}

// block masks the block starting at markers[i], marking every line it
// owns in used.
func (p *pemRule) block(lines []string, markers []pemMarker, i int, claimed [][][2]int) finding {
	m := markers[i]
	if !m.isBegin {
		return p.closeBlock(lines, m, claimed)
	}
	if end, ok := p.sameLineEnd(lines[m.line], m); ok {
		claimRange(claimed, m.line, m.start, end)
		return finding{kind: KindPrivateKey, parts: []span{{line: m.line, start: m.start, end: end}}}
	}
	if i+1 < len(markers) {
		if n := markers[i+1]; n.line != m.line && !n.isBegin {
			return p.enclosedBlock(lines, markers, i, i+1, claimed)
		}
	}
	return p.openBlock(lines, m, claimed)
}

// sameLineEnd reports the first END later on the marker's line.
func (p *pemRule) sameLineEnd(line string, m pemMarker) (int, bool) {
	loc := p.end.FindStringIndex(line[m.start+1:])
	if loc == nil {
		return 0, false
	}
	return m.start + 1 + loc[1], true
}

// enclosedBlock masks a BEGIN with an END on a later line and no BEGIN
// between: both markers and every base64 run on the lines between them,
// line numbers and header words included.
func (p *pemRule) enclosedBlock(lines []string, markers []pemMarker, bi, ei int, claimed [][][2]int) finding {
	b, e := markers[bi], markers[ei]
	parts := []span{{line: b.line, start: b.start, end: b.end}}
	for li := b.line + 1; li < e.line; li++ {
		claimLine(claimed, lines, li)
		for _, m := range p.base.FindAllStringIndex(lines[li], -1) {
			parts = append(parts, span{line: li, start: m[0], end: m[1]})
		}
	}
	claimRange(claimed, b.line, b.start, b.end)
	claimRange(claimed, e.line, e.start, e.end)
	return finding{kind: KindPrivateKey, parts: append(parts, span{line: e.line, start: e.start, end: e.end})}
}

// openBlock masks a BEGIN without a later END: the marker and the runs of
// following blank, header (before the first body line) and body lines. The
// first other line ends the block, so a TUI truncation note survives.
func (p *pemRule) openBlock(lines []string, m pemMarker, claimed [][][2]int) finding {
	parts := []span{{line: m.line, start: m.start, end: m.end}}
	claimRange(claimed, m.line, m.start, m.end)
	body := false
	for li := m.line + 1; li < len(lines); li++ {
		line := lines[li]
		switch {
		case isBlankLine(line):
			claimLine(claimed, lines, li)
		case !body && p.header.MatchString(line):
			claimLine(claimed, lines, li)
			parts = append(parts, p.runs(li, line)...)
		case p.body.MatchString(line):
			body = true
			claimLine(claimed, lines, li)
			parts = append(parts, p.runs(li, line)...)
		default:
			return finding{kind: KindPrivateKey, parts: parts}
		}
	}
	return finding{kind: KindPrivateKey, parts: parts}
}

// closeBlock masks an END with no BEGIN since the previous block: the
// marker, the runs before it on its line back to the previous block's
// last byte, and, only when no block stands before it on its line, the
// runs walking up over blank and body lines, never past the previous
// block. Runs at or before the previous block end are skipped, never
// reused: they stay masked by their own block, or visible when they were
// never its content, and borrowing them would poison this block with
// already-masked bytes or hide readable text.
func (p *pemRule) closeBlock(lines []string, m pemMarker, claimed [][][2]int) finding {
	parts := []span{{line: m.line, start: m.start, end: m.end}}
	claimRange(claimed, m.line, m.start, m.end)
	prevEnd := 0
	for _, r := range claimed[m.line] {
		if r[1] <= m.start && r[1] > prevEnd {
			prevEnd = r[1]
		}
	}
	for _, loc := range p.base.FindAllStringIndex(lines[m.line][:m.start], -1) {
		if loc[0] < prevEnd || claimedAt(claimed[m.line], loc[0], loc[1]) {
			continue
		}
		claimRange(claimed, m.line, loc[0], loc[1])
		parts = append(parts, span{line: m.line, start: loc[0], end: loc[1]})
	}
	if prevEnd == 0 {
		for li := m.line - 1; li >= 0 && len(claimed[li]) == 0; li-- {
			line := lines[li]
			if !isBlankLine(line) && !p.body.MatchString(line) {
				break
			}
			claimLine(claimed, lines, li)
			parts = append(parts, p.runs(li, line)...)
		}
	}
	return finding{kind: KindPrivateKey, parts: parts}
}

// runs returns every base64 run of one line as spans.
func (p *pemRule) runs(li int, line string) []span {
	locs := p.base.FindAllStringIndex(line, -1)
	out := make([]span, 0, len(locs))
	for _, m := range locs {
		out = append(out, span{line: li, start: m[0], end: m[1]})
	}
	return out
}

// isBlankLine reports lines with nothing but spaces, frames and carriage
// returns.
func isBlankLine(line string) bool {
	for _, r := range line {
		if r != ' ' && r != '\r' && !isFrameRune(r) {
			return false
		}
	}
	return true
}
