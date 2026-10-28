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
	used := make([]bool, len(lines))
	out := make([]finding, 0, len(markers))
	for i, m := range markers {
		if used[m.line] {
			continue
		}
		out = append(out, p.block(lines, markers, i, used))
	}
	return out
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
func (p *pemRule) block(lines []string, markers []pemMarker, i int, used []bool) finding {
	m := markers[i]
	if !m.isBegin {
		return p.closeBlock(lines, m, used)
	}
	if end, ok := p.sameLineEnd(lines[m.line], m); ok {
		used[m.line] = true
		return finding{kind: KindPrivateKey, parts: []span{{line: m.line, start: m.start, end: end}}}
	}
	if i+1 < len(markers) {
		if n := markers[i+1]; n.line != m.line && !n.isBegin {
			return p.enclosedBlock(lines, markers, i, i+1, used)
		}
	}
	return p.openBlock(lines, m, used)
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
func (p *pemRule) enclosedBlock(lines []string, markers []pemMarker, bi, ei int, used []bool) finding {
	b, e := markers[bi], markers[ei]
	parts := []span{{line: b.line, start: b.start, end: b.end}}
	for li := b.line + 1; li < e.line; li++ {
		used[li] = true
		for _, m := range p.base.FindAllStringIndex(lines[li], -1) {
			parts = append(parts, span{line: li, start: m[0], end: m[1]})
		}
	}
	used[b.line] = true
	used[e.line] = true
	return finding{kind: KindPrivateKey, parts: append(parts, span{line: e.line, start: e.start, end: e.end})}
}

// openBlock masks a BEGIN without a later END: the marker and the runs of
// following blank, header (before the first body line) and body lines. The
// first other line ends the block, so a TUI truncation note survives.
func (p *pemRule) openBlock(lines []string, m pemMarker, used []bool) finding {
	parts := []span{{line: m.line, start: m.start, end: m.end}}
	used[m.line] = true
	body := false
	for li := m.line + 1; li < len(lines); li++ {
		line := lines[li]
		switch {
		case isBlankLine(line):
			used[li] = true
		case !body && p.header.MatchString(line):
			used[li] = true
			parts = append(parts, p.runs(li, line)...)
		case p.body.MatchString(line):
			body = true
			used[li] = true
			parts = append(parts, p.runs(li, line)...)
		default:
			return finding{kind: KindPrivateKey, parts: parts}
		}
	}
	return finding{kind: KindPrivateKey, parts: parts}
}

// closeBlock masks an END with no BEGIN since the previous block: the
// marker, the runs before it on its line, and the runs walking up over
// blank and body lines, never past the previous block.
func (p *pemRule) closeBlock(lines []string, m pemMarker, used []bool) finding {
	parts := []span{{line: m.line, start: m.start, end: m.end}}
	used[m.line] = true
	for _, loc := range p.base.FindAllStringIndex(lines[m.line][:m.start], -1) {
		parts = append(parts, span{line: m.line, start: loc[0], end: loc[1]})
	}
	for li := m.line - 1; li >= 0 && !used[li]; li-- {
		line := lines[li]
		if !isBlankLine(line) && !p.body.MatchString(line) {
			break
		}
		used[li] = true
		parts = append(parts, p.runs(li, line)...)
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
