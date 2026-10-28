package redact

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// authorizationHead matches one header name with its separator, capturing
// an optional quote right after the name. A quoted name (group 1 set)
// disables the enclosing-quote rule below.
const authorizationHead = `(?i)\b(?:proxy-)?authorization(["']?)[ \t]*[:=][ \t]*`

// authRule masks one header value: the scheme word stays visible, the
// credential runs to the value's end.
type authRule struct {
	head *regexp.Regexp
}

func (a *authRule) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for _, m := range a.head.FindAllStringSubmatchIndex(line, -1) {
			start, end := m[0], m[1]
			quoted := line[m[2]:m[3]]
			from, to, ok := authCredential(line, start, end, quoted)
			if !ok {
				continue
			}
			out = append(out, finding{kind: KindAuthorization, parts: []span{{line: li, start: from, end: to}}})
		}
	}
	return out
}

// authCredential locates the credential part after one head match,
// returning its byte range. It reports false when the value holds no
// credential: empty, or starting with the placeholder on a second pass.
func authCredential(line string, headStart, headEnd int, quoted string) (int, int, bool) {
	start, end := authValue(line, headStart, headEnd, quoted)
	if start >= end || strings.HasPrefix(line[start:end], string(Placeholder)) {
		return 0, 0, false
	}
	return start, end, true
}

// authValue finds the value after the head: a quoted value from its
// opening quote, an enclosed value from the quote before the name, or a
// bare value to the line end. It strips one visible scheme word.
func authValue(line string, headStart, headEnd int, quoted string) (int, int) {
	if headEnd < len(line) && (line[headEnd] == '"' || line[headEnd] == '\'') {
		end := closingQuote(line, headEnd, line[headEnd])
		return stripScheme(line, headEnd+1, end)
	}
	if quoted == "" && headStart > 0 && (line[headStart-1] == '"' || line[headStart-1] == '\'') {
		end := closingQuote(line, headEnd, line[headStart-1])
		return stripScheme(line, headEnd, end)
	}
	return stripScheme(line, headEnd, bareEnd(line, headEnd))
}

// stripScheme removes one visible scheme word and the spaces and tabs
// after it. Any other first word belongs to the credential.
func stripScheme(line string, start, end int) (int, int) {
	i := start
	for i < end {
		r, size := utf8.DecodeRuneInString(line[i:end])
		if !isSchemeRune(r) {
			break
		}
		i += size
	}
	word := line[start:i]
	if word == "" || !isScheme(word) {
		return start, end
	}
	if i < end && !isSchemeEnd(line, i) {
		return start, end
	}
	for i < end && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	return i, end
}

// isSchemeRune reports the runes a scheme word is read over.
func isSchemeRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
		return true
	case r == '.' || r == '_' || r == '-':
		return true
	default:
		return false
	}
}

// isSchemeEnd reports the runes ending a scheme word: a space, tab,
// quote, placeholder, control or frame rune, or the value end. Any other
// first word belongs to the credential.
func isSchemeEnd(line string, i int) bool {
	if i >= len(line) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(line[i:])
	switch {
	case r == ' ' || r == '\t' || r == '"' || r == '\'' || r == Placeholder:
		return true
	case unicode.IsControl(r) || isBlankRune(r):
		return true
	default:
		return false
	}
}

// isScheme reports the visible scheme words, case-insensitive.
func isScheme(word string) bool {
	switch strings.ToLower(word) {
	case "basic",
		"bearer",
		"digest",
		"token",
		"negotiate",
		"ntlm",
		"bot",
		"apikey",
		"oauth",
		"signature",
		"hoba",
		"mutual",
		"vapid",
		"dpop",
		"gnap",
		"privatetoken",
		"concealed",
		"scram-sha-1",
		"scram-sha-256",
		"aws4-hmac-sha256",
		"hawk":
		return true
	default:
		return false
	}
}

// bareEnd ends an unquoted value at the first control or frame rune or the
// line end, backing off trailing blanks. Spaces, commas, equals signs and
// inner quotes stay inside the credential.
func bareEnd(line string, start int) int {
	end := start
	for end < len(line) {
		r, size := utf8.DecodeRuneInString(line[end:])
		if unicode.IsControl(r) || isFrameRune(r) {
			break
		}
		end += size
	}
	return trimBlanks(line, start, end)
}

// trimBlanks backs end off trailing blank runes.
func trimBlanks(line string, start, end int) int {
	for end > start {
		r, size := utf8.DecodeLastRuneInString(line[start:end])
		if !isBlankRune(r) {
			break
		}
		end -= size
	}
	return end
}

// closingQuote ends a quoted value at its closing quote following the §6.4
// rules, or, when unterminated, at the last non-blank rune before the
// first control or frame rune or the line end. start points at the opening
// quote; the returned end is past the last value rune.
func closingQuote(line string, start int, q byte) int {
	i := start + 1
	for i < len(line) {
		c := line[i]
		if q == '"' && c == '"' {
			back := 0
			for j := i - 1; j > start && line[j] == '\\'; j-- {
				back++
			}
			if back%2 == 0 {
				return i
			}
			i++
			continue
		}
		if q == '\'' && c == '\'' {
			if i+1 < len(line) && line[i+1] == '\'' {
				i += 2
				continue
			}
			return i
		}
		r, size := utf8.DecodeRuneInString(line[i:])
		if unicode.IsControl(r) || isFrameRune(r) {
			return trimBlanks(line, start+1, i)
		}
		i += size
	}
	return trimBlanks(line, start+1, len(line))
}
