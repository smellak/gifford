package redact

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// assignmentCandidate matches a named key with its operator across the
// .env, YAML, JSON, TOML, header, query, cookie and flag forms.
const assignmentCandidate = `([A-Za-z_][A-Za-z0-9_.-]*)["']?[ \t]*[:=]`

// assignRule masks the value of a secret-named key.
type assignRule struct {
	cand *regexp.Regexp
}

func (a *assignRule) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for _, m := range a.cand.FindAllStringSubmatchIndex(line, -1) {
			name := line[m[2]:m[3]]
			op := m[1] - 1
			if op+1 < len(line) && line[op+1] == '=' {
				continue
			}
			if m[0] >= 3 && line[m[0]-3:m[0]] == "://" {
				continue
			}
			if !isSecretNamed(name) {
				continue
			}
			start, end, ok := assignValue(line, m[1])
			if !ok {
				continue
			}
			out = append(out, finding{kind: KindSecretAssignment, parts: []span{{line: li, start: start, end: end}}})
		}
	}
	return out
}

// assignValue locates the value after the operator, skipping spaces and
// tabs. It reports false for empty values, second-pass placeholders and
// the unquoted shapes the policy leaves readable.
func assignValue(line string, opEnd int) (int, int, bool) {
	i := opEnd
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	if i >= len(line) {
		return 0, 0, false
	}
	if line[i] == '"' || line[i] == '\'' {
		end := closingQuote(line, i, line[i])
		if r, _ := utf8.DecodeRuneInString(line[i+1:]); end <= i+1 || r == Placeholder {
			return 0, 0, false
		}
		return i + 1, end, true
	}
	r, _ := utf8.DecodeRuneInString(line[i:])
	if isBareRefusal(r) {
		return 0, 0, false
	}
	end := i
	for end < len(line) {
		r, size := utf8.DecodeRuneInString(line[end:])
		if !isBareRune(r) {
			break
		}
		end += size
	}
	run := line[i:end]
	if isBareStopList(run) {
		return 0, 0, false
	}
	if end < len(line) {
		if r, _ := utf8.DecodeRuneInString(line[end:]); r == '(' || r == '[' {
			return 0, 0, false
		}
	}
	return i, end, true
}

// isBareRefusal reports the runes voiding a bare value on sight: shell and
// template introductions, brackets, the comparison and pipe operators and
// the redacted-pass placeholder.
func isBareRefusal(r rune) bool {
	switch r {
	case '$', '%', '<', '{', '(', '[', '=', '&', '|', '>', '*', Placeholder:
		return true
	default:
		return false
	}
}

// isBareRune reports the runes a bare value runs over: anything but
// whitespace, controls, blanks, the placeholder and the quoting,
// separating and bracketing runes.
func isBareRune(r rune) bool {
	switch {
	case unicode.IsSpace(r) || unicode.IsControl(r) || isBlankRune(r) || r == Placeholder:
		return false
	case strings.ContainsRune("\"',;&|()[]{}<>", r):
		return false
	default:
		return true
	}
}

// isBareStopList reports the type and null words a bare value may never
// be: masking one would only hide a schema, never a secret.
func isBareStopList(run string) bool {
	switch run {
	case "string",
		"str",
		"String",
		"bytes",
		"number",
		"int",
		"bool",
		"boolean",
		"any",
		"unknown",
		"object",
		"null",
		"nil",
		"None",
		"undefined",
		"true",
		"false",
		"True",
		"False":
		return true
	default:
		return false
	}
}

// isSecretWord reports the words that alone name a secret.
func isSecretWord(up string) bool {
	switch up {
	case "TOKEN",
		"SECRET",
		"PASSWORD",
		"PASSWD",
		"PASS",
		"PASSPHRASE",
		"CREDENTIAL",
		"CREDENTIALS",
		"CREDS",
		"APIKEY",
		"AUTH",
		"COOKIE",
		"DSN":
		return true
	default:
		return false
	}
}

// isKeyIndex reports the words before KEY that name an index, not a
// secret.
func isKeyIndex(up string) bool {
	switch up {
	case "PRIMARY",
		"FOREIGN",
		"PUBLIC",
		"SORT",
		"PARTITION",
		"CACHE",
		"ROUTING",
		"IDEMPOTENCY",
		"LOOKUP",
		"MAP",
		"HASH",
		"SHORTCUT",
		"HOT":
		return true
	default:
		return false
	}
}

// isSecretNamed reports whether a key name holds secrets, splitting at
// separators and camel-case boundaries and upper-casing the words.
func isSecretNamed(name string) bool {
	words := make([]string, 0, 4)
	for _, part := range strings.FieldsFunc(name, isNameSeparator) {
		words = append(words, splitCamel(part)...)
	}
	for i, w := range words {
		up := strings.ToUpper(w)
		if isSecretWord(up) {
			return true
		}
		if (up == "PWD" || up == "KEY") && len(words) >= 2 {
			if up == "KEY" && i > 0 && isKeyIndex(strings.ToUpper(words[i-1])) {
				continue
			}
			return true
		}
	}
	return false
}

// isNameSeparator reports the separators a secret name splits at.
func isNameSeparator(r rune) bool {
	return r == '_' || r == '-' || r == '.'
}

// splitCamel splits one separator-free part at camel-case boundaries: a
// lower or digit before an upper, or an upper before an upper-then-lower
// as in APIKey.
func splitCamel(part string) []string {
	runes := []rune(part)
	words := make([]string, 0, len(runes))
	start := 0
	for i := 1; i < len(runes); i++ {
		prev, cur := runes[i-1], runes[i]
		boundary := isLowerOrDigit(prev) && isUpper(cur)
		if !boundary && isUpper(prev) && isUpper(cur) && i+1 < len(runes) && isLower(runes[i+1]) {
			boundary = true
		}
		if boundary {
			words = append(words, string(runes[start:i]))
			start = i
		}
	}
	return append(words, string(runes[start:]))
}

// isLowerOrDigit reports ASCII lower case and digits.
func isLowerOrDigit(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= '0' && r <= '9'
}

// isUpper reports ASCII upper case.
func isUpper(r rune) bool {
	return r >= 'A' && r <= 'Z'
}

// isLower reports ASCII lower case.
func isLower(r rune) bool {
	return r >= 'a' && r <= 'z'
}
