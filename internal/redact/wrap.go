package redact

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// contKinds are the rules the §6.6 wrap continuation applies to on
// screens: a TUI hard break inside a value joins the next line's head.
func contKinds(k Kind) bool {
	switch k {
	case KindAuthorization, KindAPIKey, KindJWT, KindSecretAssignment:
		return true
	default:
		return false
	}
}

// headPattern matches the usable head: token runes with up to three
// trailing equals signs.
const headPattern = `^[A-Za-z0-9+/_.~%-]+={0,3}`

// contRule wraps the value rules with the §6.6 continuation and prefix
// join for screens. Free text runs the inner rules without it.
type contRule struct {
	inner    []rule
	keys     *apiKeys
	jwt      *jwtRule
	head     *regexp.Regexp
	prefixes []string
}

func (c *contRule) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for _, r := range c.inner {
		out = append(out, r.find(lines)...)
	}
	out = c.continueFindings(lines, out)
	out = append(out, c.prefixJoins(lines)...)
	return c.continueFindings(lines, out)
}

// headRun splits a line's first non-blank run into its joinable head and
// the rest, reporting whether the run stands alone and the rest is empty
// for the repeat rule.
type headRun struct {
	start, end int
	onlyRun    bool
	restEmpty  bool
}

// usableHead returns the joinable head of the next line: its first
// non-blank run must start with a token head, hold only closers after it,
// and either stand alone or span 16 cells.
func usableHead(line string, head *regexp.Regexp) (headRun, bool) {
	i := 0
	for i < len(line) {
		r, size := utf8.DecodeRuneInString(line[i:])
		if !isBlankRune(r) {
			break
		}
		i += size
	}
	start := i
	for i < len(line) {
		r, size := utf8.DecodeRuneInString(line[i:])
		if isBlankRune(r) {
			break
		}
		i += size
	}
	run := line[start:i]
	loc := head.FindStringIndex(run)
	if loc == nil {
		return headRun{}, false
	}
	fore, rest := run[:loc[1]], run[loc[1]:]
	if !isCloserRun(rest) {
		return headRun{}, false
	}
	onlyRun := true
	for _, r := range line[i:] {
		if !isBlankRune(r) {
			onlyRun = false
			break
		}
	}
	if !onlyRun && CellWidth(fore) < 16 {
		return headRun{}, false
	}
	return headRun{start: start, end: start + len(fore), onlyRun: onlyRun, restEmpty: rest == ""}, true
}

// isCloserRun reports whether rest holds only closing runes.
func isCloserRun(rest string) bool {
	for _, r := range rest {
		if !strings.ContainsRune("\"'`),;]}>:", r) {
			return false
		}
	}
	return true
}

// continueFindings extends every kinds 3-6 finding ending at its line's
// last non-blank rune over the next line's usable head, masked and
// uncounted, repeating while the joined run stands alone with nothing
// after its head.
func (c *contRule) continueFindings(lines []string, findings []finding) []finding {
	for fi := range findings {
		f := &findings[fi]
		if !contKinds(f.kind) {
			continue
		}
		for {
			last := lastPart(f.parts)
			if last.line+1 >= len(lines) {
				break
			}
			if !endsAtLastNonBlank(lines[last.line], last.end) {
				break
			}
			head, ok := usableHead(lines[last.line+1], c.head)
			if !ok {
				break
			}
			f.parts = append(f.parts, span{line: last.line + 1, start: head.start, end: head.end})
			if !head.onlyRun || !head.restEmpty {
				break
			}
		}
	}
	return findings
}

// lastPart returns the part on the highest line, breaking ties by end.
func lastPart(parts []span) span {
	best := parts[0]
	for _, p := range parts[1:] {
		if p.line > best.line || (p.line == best.line && p.end > best.end) {
			best = p
		}
	}
	return best
}

// endsAtLastNonBlank reports whether end sits right past the line's last
// non-blank rune.
func endsAtLastNonBlank(line string, end int) bool {
	trimmed := line
	for len(trimmed) > 0 {
		r, size := utf8.DecodeLastRuneInString(trimmed)
		if !isBlankRune(r) {
			break
		}
		trimmed = trimmed[:len(trimmed)-size]
	}
	return len(trimmed) == end
}

// prefixJoins matches fragments cut by a TUI hard break: a logical line's
// last non-blank run ending in a literal prefix over token runes joins the
// next line's usable head against the api_key and jwt patterns, anchored
// at the fragment. A longer match is one finding over both parts.
func (c *contRule) prefixJoins(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li := 0; li+1 < len(lines); li++ {
		frag, ok := c.lastRunFragment(lines[li])
		if !ok {
			continue
		}
		head, ok := usableHead(lines[li+1], c.head)
		if !ok {
			continue
		}
		joined := frag.text + lines[li+1][head.start:head.end]
		if end, ok := c.matchFragment(frag, joined); ok && end > len(frag.text) {
			over := end - len(frag.text)
			out = append(out, finding{kind: frag.kind, parts: []span{
				{line: li, start: frag.start, end: frag.start + len(frag.text)},
				{line: li + 1, start: head.start, end: head.start + over},
			}})
		}
	}
	return out
}

// fragment is one joinable tail of a line.
type fragment struct {
	kind  Kind
	text  string
	start int
}

// lastRunFragment finds a literal prefix starting at a word boundary in
// the line's last non-blank run, running over token runes to the run's
// end, and not matching on its own.
func (c *contRule) lastRunFragment(line string) (fragment, bool) {
	end := len(line)
	for end > 0 {
		r, size := utf8.DecodeLastRuneInString(line[:end])
		if !isBlankRune(r) {
			break
		}
		end -= size
	}
	start := end
	for start > 0 {
		r, size := utf8.DecodeLastRuneInString(line[:start])
		if isBlankRune(r) {
			break
		}
		start -= size
	}
	run := line[start:end]
	for pos := 0; pos < len(run); {
		_, size := utf8.DecodeRuneInString(run[pos:])
		boundary := pos == 0 || !isWordRune(mustDecodeLastRune(run[:pos]))
		if boundary {
			for _, prefix := range c.prefixes {
				if !strings.HasPrefix(run[pos:], prefix) {
					continue
				}
				text := run[pos:]
				if !isTokenRun(text) {
					continue
				}
				kind := KindAPIKey
				if prefix == "eyJ" {
					kind = KindJWT
				}
				if c.selfMatches(kind, text) {
					continue
				}
				return fragment{kind: kind, text: text, start: start + pos}, true
			}
		}
		pos += size
	}
	return fragment{}, false
}

// mustDecodeLastRune decodes the last rune of a non-empty string.
func mustDecodeLastRune(s string) rune {
	r, _ := utf8.DecodeLastRuneInString(s)
	return r
}

// isWordRune reports word runes for fragment boundaries.
func isWordRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
		return true
	case r == '_':
		return true
	default:
		return false
	}
}

// isTokenRun reports runs over the fragment token class.
func isTokenRun(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
		case r == '_' || r == '.' || r == '-':
		default:
			return false
		}
	}
	return true
}

// selfMatches reports whether the fragment matches on its own, in which
// case the plain rules already own it.
func (c *contRule) selfMatches(kind Kind, text string) bool {
	if kind == KindJWT {
		_, ok := c.jwt.matchAt(text, 0)
		return ok
	}
	_, ok := c.keys.matchAt(text, 0)
	return ok
}

// matchFragment anchors the fragment's kind patterns at its start over
// the joined candidate.
func (c *contRule) matchFragment(frag fragment, joined string) (int, bool) {
	if frag.kind == KindJWT {
		return c.jwt.matchAt(joined, 0)
	}
	return c.keys.matchAt(joined, 0)
}
