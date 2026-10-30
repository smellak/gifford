package profiles

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	yaml "go.yaml.in/yaml/v3"

	profiledata "github.com/smellak/gifford/profiles"
)

// Supported harnesses, one profiles/<harness>.yaml each: the canonical
// protocol.Harness IDs, generic included.
var harnessNames = [...]string{"claude-code", "codex", "muse-code", "opencode", "antigravity", "generic"}

const (
	maxVersionOutputBytes = 4096
	maxVersionRunes       = 64
)

var versionToken = regexp.MustCompile(`[vV]?[0-9]+(?:\.[0-9]+)+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?`)

// ExtendedKeyboard names how a harness negotiates extended keyboard input.
type ExtendedKeyboard string

// Extended keyboard modes a profile can declare.
const (
	// ExtendedNone means plain xterm keys, no negotiation.
	ExtendedNone ExtendedKeyboard = "none"
	// ExtendedCSIU means CSI u (kitty-style) encoding.
	ExtendedCSIU ExtendedKeyboard = "csi_u"
	// ExtendedModifyOtherKeys means XTMODKEYS/modifyOtherKeys levels.
	ExtendedModifyOtherKeys ExtendedKeyboard = "modify_other_keys"
)

// Profile is one harness's keyboard behaviour: the byte sequences Gifford
// writes to drive its TUI, which modes it negotiates, and how to read the
// draft back from a screen line. Profiles with Measured false are xterm
// standard defaults, not keycap measurements.
type Profile struct {
	Harness          string
	Measured         bool
	VersionRange     string
	versionConds     []versionCond
	Submit           string
	InsertNewline    string
	ClearLine        string
	Cancel           string
	BracketedPaste   bool
	ExtendedKeyboard ExtendedKeyboard
	InputLinePattern string
	InputLine        *regexp.Regexp
}

// Draft extracts the draft text from a screen line using the input_line
// pattern; false means the line is not an input line. An unmeasured
// profile never yields draft evidence: without a keycap measurement the
// pattern is an xterm guess, so Draft fails closed until measured.
func (p Profile) Draft(line string) (string, bool) {
	if !p.Measured {
		return "", false
	}
	m := p.InputLine.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// Profiles holds the embedded harness profiles; it is read-only.
type Profiles struct {
	byName map[string]Profile
}

// ParseVersion returns the only bounded version token in harness output.
// Ambiguous or malformed output returns empty. Prerelease and build suffixes
// remain part of the value so they cannot silently select a stable profile.
// Both run and keycap use this boundary, matching the value keycap persists.
func ParseVersion(line string) string {
	if len(line) == 0 || len(line) > maxVersionOutputBytes {
		return ""
	}
	matches := versionToken.FindAllStringIndex(line, 2)
	if len(matches) != 1 {
		return ""
	}
	match := matches[0]
	if !versionTokenBoundary(line, match[0], match[1]) {
		return ""
	}
	version := line[match[0]:match[1]]
	if version[0] == 'v' || version[0] == 'V' {
		version = version[1:]
	}
	if utf8.RuneCountInString(version) > maxVersionRunes {
		return ""
	}
	return version
}

func versionTokenBoundary(line string, start, end int) bool {
	if start > 0 {
		before, _ := utf8.DecodeLastRuneInString(line[:start])
		if versionTokenRune(before) {
			return false
		}
	}
	if end < len(line) {
		after, _ := utf8.DecodeRuneInString(line[end:])
		if versionTokenRune(after) {
			return false
		}
	}
	return true
}

func versionTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("_+.-", r)
}

// profileYAML decodes one profile file; every key is a pointer so a missing
// key is visible.
type profileYAML struct {
	Harness          *string `yaml:"harness"`
	Measured         *bool   `yaml:"measured"`
	VersionRange     *string `yaml:"version_range"`
	Submit           *string `yaml:"submit"`
	InsertNewline    *string `yaml:"insert_newline"`
	ClearLine        *string `yaml:"clear_line"`
	Cancel           *string `yaml:"cancel"`
	BracketedPaste   *bool   `yaml:"bracketed_paste"`
	ExtendedKeyboard *string `yaml:"extended_keyboard"`
	InputLine        *string `yaml:"input_line"`
}

// Load decodes and validates every embedded profiles/*.yaml.
func Load() (Profiles, error) {
	byName := make(map[string]Profile, len(harnessNames))
	for _, name := range harnessNames {
		raw, err := profiledata.Files.ReadFile(name + ".yaml")
		if err != nil {
			return Profiles{}, fmt.Errorf("%w: %s.yaml: %w", ErrInvalidProfile, name, err)
		}
		profile, err := ParseFile(name, raw)
		if err != nil {
			return Profiles{}, err
		}
		byName[name] = profile
	}
	return Profiles{byName: byName}, nil
}

// Profile returns a copy of the named harness profile for a harness
// version; an unknown name returns ErrUnknownHarness, while a version
// outside the profile's version_range falls back to the generic profile.
// The "*" range matches every version, unknown included.
func (ps Profiles) Profile(name, version string) (Profile, error) {
	profile, ok := ps.byName[name]
	if !ok {
		return Profile{}, fmt.Errorf("%w: %s", ErrUnknownHarness, name)
	}
	if profile.matchesVersion(version) {
		return profile, nil
	}
	return ps.byName["generic"], nil
}

// ParseFile decodes one profile, names its file and key on any violation,
// and copies the values into a validated Profile. Exported so tests and the
// keycap writer vet candidate files with production rules.
func ParseFile(name string, raw []byte) (Profile, error) {
	fail := func(key string, err error) (Profile, error) {
		return Profile{}, fmt.Errorf("%w: %s.yaml: %s: %w", ErrInvalidProfile, name, key, err)
	}
	var decoded profileYAML
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&decoded); err != nil {
		return fail("document", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fail("document", fmt.Errorf("expected exactly one document"))
	}
	if decoded.Harness == nil {
		return fail("harness", fmt.Errorf("missing key"))
	}
	if *decoded.Harness != name {
		return fail("harness", fmt.Errorf("must match its file name"))
	}
	if decoded.Measured == nil {
		return fail("measured", fmt.Errorf("missing key"))
	}
	if decoded.VersionRange == nil || *decoded.VersionRange == "" {
		return fail("version_range", fmt.Errorf("missing key"))
	}
	conds, err := parseVersionRange(*decoded.VersionRange)
	if err != nil {
		return fail("version_range", err)
	}
	sequences := []struct {
		key   string
		value *string
	}{
		{"submit", decoded.Submit},
		{"insert_newline", decoded.InsertNewline},
		{"clear_line", decoded.ClearLine},
		{"cancel", decoded.Cancel},
	}
	keys := make(map[string]string, len(sequences))
	for _, field := range sequences {
		if field.value == nil || *field.value == "" {
			return fail(field.key, fmt.Errorf("missing key"))
		}
		if strings.ContainsRune(*field.value, 0) {
			return fail(field.key, fmt.Errorf("must not contain NUL"))
		}
		keys[field.key] = *field.value
	}
	if decoded.BracketedPaste == nil {
		return fail("bracketed_paste", fmt.Errorf("missing key"))
	}
	if decoded.ExtendedKeyboard == nil || *decoded.ExtendedKeyboard == "" {
		return fail("extended_keyboard", fmt.Errorf("missing key"))
	}
	var extended ExtendedKeyboard
	switch ExtendedKeyboard(*decoded.ExtendedKeyboard) {
	case ExtendedNone, ExtendedCSIU, ExtendedModifyOtherKeys:
		extended = ExtendedKeyboard(*decoded.ExtendedKeyboard)
	default:
		return fail("extended_keyboard", fmt.Errorf("must be none, csi_u or modify_other_keys"))
	}
	if decoded.InputLine == nil || *decoded.InputLine == "" {
		return fail("input_line", fmt.Errorf("missing key"))
	}
	pattern, err := regexp.Compile(*decoded.InputLine)
	if err != nil {
		return fail("input_line", err)
	}
	if pattern.NumSubexp() != 1 {
		return fail("input_line", fmt.Errorf("must capture exactly one group"))
	}
	return Profile{
		Harness:          name,
		Measured:         *decoded.Measured,
		VersionRange:     *decoded.VersionRange,
		versionConds:     conds,
		Submit:           keys["submit"],
		InsertNewline:    keys["insert_newline"],
		ClearLine:        keys["clear_line"],
		Cancel:           keys["cancel"],
		BracketedPaste:   *decoded.BracketedPaste,
		ExtendedKeyboard: extended,
		InputLinePattern: *decoded.InputLine,
		InputLine:        pattern,
	}, nil
}

// versionCond is one comma-separated condition of a version_range: an
// operator over a dotted version. The "*" range parses to no conditions
// and matches everything.
type versionCond struct {
	op  string
	ver []string
}

// parseVersionRange validates a version_range: "*" or comma-separated
// conditions like ">=1.2.0,<2.0.0". A bare version means equality.
func parseVersionRange(s string) ([]versionCond, error) {
	if s == "*" {
		return nil, nil
	}
	var conds []versionCond
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		cond := versionCond{}
		for _, op := range []string{">=", "<=", ">", "<", "="} {
			if rest, ok := strings.CutPrefix(part, op); ok {
				cond.op = op
				part = strings.TrimSpace(rest)
				break
			}
		}
		if cond.op == "" {
			cond.op = "="
		}
		if part == "" {
			return nil, fmt.Errorf("empty condition")
		}
		for _, seg := range strings.Split(part, ".") {
			if seg == "" {
				return nil, fmt.Errorf("empty version segment")
			}
		}
		cond.ver = strings.Split(part, ".")
		conds = append(conds, cond)
	}
	if len(conds) == 0 {
		return nil, fmt.Errorf("empty range")
	}
	return conds, nil
}

// matchesVersion reports whether version falls inside the profile's range.
// The "*" range matches every version, unknown included; any other range
// rejects an empty version.
func (p Profile) matchesVersion(version string) bool {
	if p.VersionRange == "*" {
		return true
	}
	if version == "" {
		return false
	}
	have := strings.Split(version, ".")
	for _, cond := range p.versionConds {
		if !evalCond(cond, have) {
			return false
		}
	}
	return true
}

func evalCond(cond versionCond, have []string) bool {
	cmp := compareVersions(have, cond.ver)
	switch cond.op {
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	default:
		return cmp == 0
	}
}

// compareVersions orders dotted versions segment by segment: numeric when
// both sides parse as integers, lexical otherwise; a missing segment
// counts as "0".
func compareVersions(a, b []string) int {
	n := max(len(a), len(b))
	for i := range n {
		as, bs := "0", "0"
		if i < len(a) {
			as = a[i]
		}
		if i < len(b) {
			bs = b[i]
		}
		if c := compareSegment(as, bs); c != 0 {
			return c
		}
	}
	return 0
}

func compareSegment(a, b string) int {
	ai, aerr := strconv.Atoi(a)
	bi, berr := strconv.Atoi(b)
	if aerr == nil && berr == nil {
		return cmp.Compare(ai, bi)
	}
	return strings.Compare(a, b)
}
