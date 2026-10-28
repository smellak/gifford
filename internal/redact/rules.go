package redact

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// connectionStringPattern matches scheme://userinfo@host, capturing the
// userinfo. The class keeps out whitespace, delimiters and the placeholder
// and frame runes, so a redacted pass never matches again; greediness takes
// the userinfo up to the last @.
const connectionStringPattern = `(?i)\b[a-z][a-z0-9+.-]*://([^\s/?#"'\x60\x{2022}\x{2500}-\x{259F}]*)@`

// connRule masks the userinfo of connection URLs holding a secret: a
// userinfo with a colon, or 16 runes and more.
type connRule struct {
	re *regexp.Regexp
}

func (c *connRule) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for _, m := range c.re.FindAllStringSubmatchIndex(line, -1) {
			user := line[m[2]:m[3]]
			if !strings.Contains(user, ":") && utf8.RuneCountInString(user) < 16 {
				continue
			}
			out = append(out, finding{kind: KindConnectionString, parts: []span{{line: li, start: m[2], end: m[3]}}})
		}
	}
	return out
}

// Vendor token patterns of §6.3, each masking its whole match. A match
// failing its check is skipped. Patterns are const strings, following
// §3.3; apiKeyPatternList builds the slice apiCheck indexes into.
//
//nolint:gosec // detection patterns of documented token shapes, not credentials.
const (
	apiKeyPatternAWS      = `\b(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16}\b`
	apiKeyPatternGitHub   = `\bgh[pousr]_[A-Za-z0-9]{36,}`
	apiKeyPatternFine     = `\bgithub_pat_[A-Za-z0-9_]{22,}`
	apiKeyPatternGitLab   = `\bglpat-[A-Za-z0-9_-]{20,}`
	apiKeyPatternSlack    = `\b(?:xox[abposr]|xapp)-[A-Za-z0-9-]{10,}`
	apiKeyPatternStripe   = `\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}`
	apiKeyPatternSK       = `\bsk-[A-Za-z0-9_-]{20,}`
	apiKeyPatternGoogle   = `\bAIza[A-Za-z0-9_-]{35}`
	apiKeyPatternHF       = `\bhf_[A-Za-z0-9]{34,}`
	apiKeyPatternNPM      = `\bnpm_[A-Za-z0-9]{36,}`
	apiKeyPatternPyPI     = `\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{50,}`
	apiKeyPatternSendGrid = `\bSG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}`
)

// apiKeyPatternList returns the twelve vendor token patterns in apiCheck
// index order.
func apiKeyPatternList() []string {
	return []string{
		apiKeyPatternAWS,
		apiKeyPatternGitHub,
		apiKeyPatternFine,
		apiKeyPatternGitLab,
		apiKeyPatternSlack,
		apiKeyPatternStripe,
		apiKeyPatternSK,
		apiKeyPatternGoogle,
		apiKeyPatternHF,
		apiKeyPatternNPM,
		apiKeyPatternPyPI,
		apiKeyPatternSendGrid,
	}
}

// apiLiteralPrefixes are the §6.3 literal prefixes for the §6.6 prefix
// join, plus eyJ for jwt.
func apiLiteralPrefixes() []string {
	return []string{
		"A3T", "AKIA", "ASIA", "ABIA", "ACCA",
		"ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_",
		"glpat-",
		"xoxa-", "xoxb-", "xoxp-", "xoxo-", "xoxs-", "xoxr-", "xapp-",
		"sk_live_", "sk_test_", "rk_live_", "rk_test_", "sk-",
		"AIza", "hf_", "npm_", "pypi-AgEIcHlwaS5vcmc", "SG.",
		"eyJ",
	}
}

// apiKeys masks whole vendor tokens.
type apiKeys struct {
	res      []*regexp.Regexp
	anchored []*regexp.Regexp
}

func (a *apiKeys) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for i, re := range a.res {
			for _, m := range re.FindAllStringIndex(line, -1) {
				if !apiCheck(i, line, m[0], m[1]) {
					continue
				}
				out = append(out, finding{kind: KindAPIKey, parts: []span{{line: li, start: m[0], end: m[1]}}})
			}
		}
	}
	return out
}

// matchAt anchors every api_key pattern at pos, returning the longest
// passing match for the §6.6 prefix join.
func (a *apiKeys) matchAt(s string, pos int) (int, bool) {
	best := -1
	for i, re := range a.anchored {
		loc := re.FindStringIndex(s[pos:])
		if loc == nil {
			continue
		}
		end := pos + loc[1]
		if !apiCheck(i, s, pos, end) {
			continue
		}
		if end > best {
			best = end
		}
	}
	return best, best >= 0
}

// apiCheck enforces the per-pattern checks by index into apiKeyPatternList():
// sk- keys need a digit and a letter after sk-, Google and SendGrid need a
// non-token rune right after the match.
func apiCheck(i int, line string, start, end int) bool {
	switch i {
	case 6:
		return hasDigitAndLetter(line[start+3 : end])
	case 7, 11:
		r, _ := utf8.DecodeRuneInString(line[end:])
		return r == utf8.RuneError || !isTokenRune(r)
	default:
		return true
	}
}

// hasDigitAndLetter reports whether s holds an ASCII digit and an ASCII
// letter.
func hasDigitAndLetter(s string) bool {
	digit, letter := false, false
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digit = true
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z':
			letter = true
		}
		if digit && letter {
			return true
		}
	}
	return false
}

// isTokenRune reports the Google/SendGrid token class.
func isTokenRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
		return true
	case r == '_' || r == '-':
		return true
	default:
		return false
	}
}

// jwtPattern matches a whole token with optional signature, so a head cut
// by a wrap still matches.
const jwtPattern = `\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]*)?`

// jwtRule masks whole JSON Web Tokens.
type jwtRule struct {
	re       *regexp.Regexp
	anchored *regexp.Regexp
}

func (j *jwtRule) find(lines []string) []finding {
	out := make([]finding, 0, len(lines))
	for li, line := range lines {
		for _, m := range j.re.FindAllStringIndex(line, -1) {
			out = append(out, finding{kind: KindJWT, parts: []span{{line: li, start: m[0], end: m[1]}}})
		}
	}
	return out
}

// matchAt anchors the jwt pattern at pos for the §6.6 prefix join.
func (j *jwtRule) matchAt(s string, pos int) (int, bool) {
	loc := j.anchored.FindStringIndex(s[pos:])
	if loc == nil {
		return -1, false
	}
	return pos + loc[1], true
}
