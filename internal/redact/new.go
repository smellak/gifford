package redact

import (
	"regexp"
	"sort"
)

// PolicyVersion is the redactor version every 2/3 result carries. It covers
// the rules, the placeholder, CellWidth and the wrap rules, not Options: a
// change to any of them bumps this version and the fixture in the same PR.
const PolicyVersion = "1"

// New builds the v1 redactor: it validates the options, drops duplicate
// paths and compiles every §6 pattern, so a bad constant fails every test.
// Patterns are const strings; word lists are switches.
func New(opts Options) (*Redactor, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	conn := &connRule{re: regexp.MustCompile(connectionStringPattern)}
	keys := &apiKeys{res: compileList(apiKeyPatterns), anchored: compileList(anchorAll(apiKeyPatterns))}
	jwt := &jwtRule{re: regexp.MustCompile(jwtPattern), anchored: regexp.MustCompile("^(?:" + jwtPattern + ")")}
	auth := &authRule{head: regexp.MustCompile(authorizationHead)}
	assign := &assignRule{cand: regexp.MustCompile(assignmentCandidate)}
	pem := &pemRule{
		begin:  regexp.MustCompile(pemBegin),
		end:    regexp.MustCompile(pemEnd),
		base:   regexp.MustCompile(pemBase64Run),
		body:   regexp.MustCompile(pemBodyLine),
		header: regexp.MustCompile(pemHeaderLine),
	}
	paths := &pathsRule{entries: dedupePaths(opts.SensitivePaths)}
	cont := &contRule{
		inner:    []rule{auth, keys, jwt, assign},
		keys:     keys,
		jwt:      jwt,
		head:     regexp.MustCompile(headPattern),
		prefixes: apiLiteralPrefixes(),
	}
	screen := &ruleSet{version: PolicyVersion, rules: []rule{pem, conn, cont, paths}}
	text := &ruleSet{version: PolicyVersion, rules: []rule{pem, conn, auth, keys, jwt, assign, paths}}
	return &Redactor{rules: screen, textRules: text}, nil
}

// compileList compiles every pattern of one rule.
func compileList(patterns []string) []*regexp.Regexp {
	res := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		res[i] = regexp.MustCompile(p)
	}
	return res
}

// anchorAll anchors every pattern at the match start for the §6.6 prefix
// join.
func anchorAll(patterns []string) []string {
	out := make([]string, len(patterns))
	for i, p := range patterns {
		out[i] = "^(?:" + p + ")"
	}
	return out
}

// dedupePaths drops duplicate entries, keeping the first occurrence, and
// sorts longest first so nested paths mask from the outside in.
func dedupePaths(entries []string) []string {
	seen := make(map[string]bool, len(entries))
	uniq := make([]string, 0, len(entries))
	for _, e := range entries {
		if !seen[e] {
			seen[e] = true
			uniq = append(uniq, e)
		}
	}
	sort.SliceStable(uniq, func(i, j int) bool { return len(uniq[i]) > len(uniq[j]) })
	return uniq
}
