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
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for _, e := range p.entries {
			out = append(out, p.occurrences(li, line, e)...)
		}
	}
	return out
}

// occurrences locates every literal occurrence with its §6.5 boundaries:
// the rune before is absent or outside letters, digits and _ . ~ - /,
// and the rune after is absent, a slash or outside letters, digits and
// _ . ~ -. After a slash the match extends over bare-value runes.
func (p *pathsRule) occurrences(li int, line, entry string) []finding {
	out := make([]finding, 0, strings.Count(line, entry))
	rest := line
	off := 0
	for {
		i := strings.Index(rest, entry)
		if i < 0 {
			return out
		}
		start := off + i
		end := start + len(entry)
		rest = rest[i+len(entry):]
		off = end
		if !pathBeforeOK(line, start) || !pathAfterOK(line, end) {
			continue
		}
		if end < len(line) && line[end] == '/' {
			end = extendBare(line, end)
		}
		out = append(out, finding{kind: KindSensitivePath, parts: []span{{line: li, start: start, end: end}}})
	}
}

// pathBeforeOK checks the rune before a path occurrence.
func pathBeforeOK(line string, start int) bool {
	if start == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(line[:start])
	return !isPathRune(r, true)
}

// pathAfterOK checks the rune after a path occurrence.
func pathAfterOK(line string, end int) bool {
	if end >= len(line) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(line[end:])
	if r == '/' {
		return true
	}
	return !isPathRune(r, false)
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
