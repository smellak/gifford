package redact

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// pathsRule masks configured sensitive paths and anything below them. The
// entries arrive deduped and longest first, so nested paths mask from the
// outside in.
type pathsRule struct {
	entries []string
}

func (p *pathsRule) find(lines []string) []finding {
	return p.findCovered(lines, nil)
}

// findCovered locates every literal occurrence with its §6.5 boundaries,
// letting covered runes through: absent, covered, or outside letters,
// digits and _ . ~ - / before, and absent, covered, slash or outside
// letters, digits and _ . ~ - after. Covered means masked by a finding of
// kinds 1-6 or by an occurrence that counts, its extension included. The
// counted occurrences are the least fixed point over the literal ones on
// the original offsets: a worklist re-checks an occurrence only when a
// rune next to it becomes covered, and each occurrence changes at most
// once.
func (p *pathsRule) findCovered(lines []string, covered [][][2]int) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		var cov [][2]int
		if li < len(covered) {
			cov = covered[li]
		}
		out = append(out, p.coverLine(li, line, cov)...)
	}
	return out
}

// pathOcc is one literal occurrence with its bare extension.
type pathOcc struct {
	start, end int
	counted    bool
}

// coverLine counts the occurrences of one line to the fixed point,
// emitting one finding per counted occurrence in entry order.
func (p *pathsRule) coverLine(li int, line string, cov [][2]int) []finding {
	var occs []pathOcc
	for _, e := range p.entries {
		rest := line
		off := 0
		for {
			i := strings.Index(rest, e)
			if i < 0 {
				break
			}
			start := off + i
			end := start + len(e)
			rest = rest[i+len(e):]
			off = end
			if end < len(line) && line[end] == '/' {
				end = extendBare(line, end)
			}
			occs = append(occs, pathOcc{start: start, end: end})
		}
	}
	queue := make([]int, len(occs))
	for i := range occs {
		queue[i] = i
	}
	var out []finding
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		if occs[i].counted {
			continue
		}
		if !beforeOK(line, occs[i].start, cov) || !afterOK(line, occs[i].end, cov) {
			continue
		}
		occs[i].counted = true
		cov = append(cov, [2]int{occs[i].start, occs[i].end})
		out = append(out, finding{kind: KindSensitivePath, parts: []span{{line: li, start: occs[i].start, end: occs[i].end}}})
		for j := range occs {
			if j == i || occs[j].counted {
				continue
			}
			if nextTo(occs[j], occs[i].start, occs[i].end) {
				queue = append(queue, j)
			}
		}
	}
	return out
}

// nextTo reports whether occurrence o has a rune next to it inside the
// newly covered [start, end): the byte before its start or at its end.
func nextTo(o pathOcc, start, end int) bool {
	if o.start > 0 && o.start-1 >= start && o.start-1 < end {
		return true
	}
	return o.end >= start && o.end < end
}

// beforeOK checks the rune before an occurrence: absent, covered, or
// outside letters, digits and _ . ~ - /.
func beforeOK(line string, start int, cov [][2]int) bool {
	if start == 0 {
		return true
	}
	if coveredAt(cov, start-1) {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(line[:start])
	return !isPathRune(r, true)
}

// afterOK checks the rune after an occurrence: absent, covered, slash, or
// outside letters, digits and _ . ~ -.
func afterOK(line string, end int, cov [][2]int) bool {
	if end >= len(line) {
		return true
	}
	if coveredAt(cov, end) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(line[end:])
	if r == '/' {
		return true
	}
	return !isPathRune(r, false)
}

// coveredAt reports whether byte pos lies in a covered range.
func coveredAt(cov [][2]int, pos int) bool {
	for _, r := range cov {
		if pos >= r[0] && pos < r[1] {
			return true
		}
	}
	return false
}

// isPathRune reports the runes binding a path to its neighbors. The
// before set adds the slash, so a subpath never matches inside its
// parent's prefix.
func isPathRune(r rune, before bool) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	switch r {
	case '_', '.', '~', '-':
		return true
	case '/':
		return before
	default:
		return false
	}
}

// extendBare extends a match over following bare-value runes.
func extendBare(line string, end int) int {
	for end < len(line) {
		r, size := utf8.DecodeRuneInString(line[end:])
		if !isBareRune(r) {
			break
		}
		end += size
	}
	return end
}
