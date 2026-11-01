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
	maxVersionOutputBytes    = 4096
	maxVersionRunes          = 64
	maxInputPlaceholderRunes = 256
	maxExecutableNameBytes   = 255
	yamlStringTag            = "!!str"
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

// InputArea is what a profile declares of the harness's editable input
// area.
type InputArea string

// Input areas a profile can declare.
const (
	// InputAreaUnknown declares nothing: the area may take more screen
	// lines than the input line. A profile without input_area has it.
	InputAreaUnknown InputArea = "unknown"
	// InputAreaSingleLine declares the area is the input line alone, so a
	// draft never takes another screen line.
	InputAreaSingleLine InputArea = "single_line"
)

// InputBoundary is one row condition around the canonical empty editable area.
// Pattern and Negated are kept verbatim so keycap can preserve them. Positive
// expressions describe a whole row; a negated final expression identifies an
// existing row outside the box.
type InputBoundary struct {
	Pattern string
	Negated bool
	expr    *regexp.Regexp
}

// Matches reports whether line satisfies the boundary. A positive expression
// must cover the complete row; a negated expression succeeds only when it has
// no match anywhere in the row, which makes an imprecise negation fail closed.
func (b InputBoundary) Matches(line string) bool {
	if b.expr == nil {
		return false
	}
	match := b.expr.FindStringIndex(line)
	if b.Negated {
		return match == nil
	}
	return match != nil && match[0] == 0 && match[1] == len(line)
}

// InputBounds describes the canonical empty editable area relative to its
// cursor row. Each side runs from the nearest row out to a distinctive box
// border or an explicit final row outside the box. A nonempty draft does not
// need these bounds to be read.
type InputBounds struct {
	Above []InputBoundary
	Below []InputBoundary
}

// Configured reports whether either side was declared.
func (b InputBounds) Configured() bool { return len(b.Above) != 0 || len(b.Below) != 0 }

// Complete reports whether both sides of the empty editable area exist.
func (b InputBounds) Complete() bool { return len(b.Above) != 0 && len(b.Below) != 0 }

// Profile is one harness's keyboard behaviour: the byte sequences Gifford
// writes to drive its TUI, which modes it negotiates, and how to read the
// draft back from a screen line. Profiles with Measured false are xterm
// standard defaults, not keycap measurements.
type Profile struct {
	Harness      string
	Measured     bool
	VersionRange string
	versionConds []versionCond
	// AuxiliaryExecutable is the exact sibling executable that a measured
	// harness installation may keep alive. Empty means no auxiliary process.
	AuxiliaryExecutable string
	Submit              string
	InsertNewline       string
	ClearLine           string
	Cancel              string
	BracketedPaste      bool
	ExtendedKeyboard    ExtendedKeyboard
	InputLinePattern    string
	InputLine           *regexp.Regexp
	InputPlaceholder    string
	InputBounds         InputBounds
	InputArea           InputArea
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

// ParseVersion returns the unambiguous bounded version in harness output.
// Two tokens are accepted only for a closed "base (base-build)" form used by
// harnesses that print their stable and build versions together. Prerelease
// and build suffixes remain part of the value so they cannot silently select a
// stable profile. Both run and keycap use this boundary, matching the value
// keycap persists.
func ParseVersion(line string) string {
	if len(line) == 0 || len(line) > maxVersionOutputBytes {
		return ""
	}
	matches := versionToken.FindAllStringIndex(line, 3)
	if len(matches) == 0 || len(matches) > 2 {
		return ""
	}
	first, ok := boundedVersionToken(line, matches[0])
	if !ok {
		return ""
	}
	if len(matches) == 1 {
		return first
	}
	second, ok := boundedVersionToken(line, matches[1])
	if !ok || !parenthesizedVersionLayout(line, matches[0], matches[1]) ||
		!relatedParenthesizedVersion(first, second) {
		return ""
	}
	return second
}

func boundedVersionToken(line string, match []int) (string, bool) {
	if !versionTokenBoundary(line, match[0], match[1]) {
		return "", false
	}
	version := line[match[0]:match[1]]
	if version[0] == 'v' || version[0] == 'V' {
		version = version[1:]
	}
	if utf8.RuneCountInString(version) > maxVersionRunes {
		return "", false
	}
	return version, true
}

func parenthesizedVersionLayout(line string, first, second []int) bool {
	between := line[first[1]:second[0]]
	if len(between) == 0 || between[len(between)-1] != '(' ||
		second[1] >= len(line) || line[second[1]] != ')' {
		return false
	}
	for _, c := range between[:len(between)-1] {
		if c != ' ' && c != '\t' {
			return false
		}
	}
	return true
}

func relatedParenthesizedVersion(first, second string) bool {
	if first == second {
		return true
	}
	if !strings.HasPrefix(second, first) || len(second) == len(first) {
		return false
	}
	return second[len(first)] == '-' || second[len(first)] == '+'
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

// profileYAML decodes one profile file. Required keys are pointers so a
// missing key is visible; InputPlaceholder, InputBounds and InputArea use nodes to
// distinguish an absent optional field from an explicit null.
type profileYAML struct {
	Harness             *string   `yaml:"harness"`
	Measured            *bool     `yaml:"measured"`
	VersionRange        *string   `yaml:"version_range"`
	AuxiliaryExecutable yaml.Node `yaml:"auxiliary_executable"`
	Submit              *string   `yaml:"submit"`
	InsertNewline       *string   `yaml:"insert_newline"`
	ClearLine           *string   `yaml:"clear_line"`
	Cancel              *string   `yaml:"cancel"`
	BracketedPaste      *bool     `yaml:"bracketed_paste"`
	ExtendedKeyboard    *string   `yaml:"extended_keyboard"`
	InputLine           *string   `yaml:"input_line"`
	InputPlaceholder    yaml.Node `yaml:"input_placeholder"`
	InputBounds         yaml.Node `yaml:"input_bounds"`
	InputArea           yaml.Node `yaml:"input_area"`
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
	auxiliaryExecutable, err := parseAuxiliaryExecutable(
		decoded.AuxiliaryExecutable, *decoded.Measured, conds,
	)
	if err != nil {
		return fail("auxiliary_executable", err)
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
	inputPlaceholder, err := parseInputPlaceholder(decoded.InputPlaceholder, *decoded.Measured, conds)
	if err != nil {
		return fail("input_placeholder", err)
	}
	inputBounds, err := parseInputBounds(decoded.InputBounds, *decoded.Measured, conds)
	if err != nil {
		return fail("input_bounds", err)
	}
	inputArea, err := parseInputArea(decoded.InputArea, *decoded.Measured, keys["insert_newline"])
	if err != nil {
		return fail("input_area", err)
	}
	return Profile{
		Harness:             name,
		Measured:            *decoded.Measured,
		VersionRange:        *decoded.VersionRange,
		versionConds:        conds,
		AuxiliaryExecutable: auxiliaryExecutable,
		Submit:              keys["submit"],
		InsertNewline:       keys["insert_newline"],
		ClearLine:           keys["clear_line"],
		Cancel:              keys["cancel"],
		BracketedPaste:      *decoded.BracketedPaste,
		ExtendedKeyboard:    extended,
		InputLinePattern:    *decoded.InputLine,
		InputLine:           pattern,
		InputPlaceholder:    inputPlaceholder,
		InputBounds:         inputBounds,
		InputArea:           inputArea,
	}, nil
}

func parseAuxiliaryExecutable(
	node yaml.Node,
	measured bool,
	versionConds []versionCond,
) (string, error) {
	if node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode || node.Tag != yamlStringTag {
		return "", fmt.Errorf("must be a string executable name")
	}
	value := node.Value
	if value == "" {
		return "", fmt.Errorf("must not be empty")
	}
	if !measured {
		return "", fmt.Errorf("requires measured: true")
	}
	if len(versionConds) != 1 || versionConds[0].op != "=" {
		return "", fmt.Errorf("requires an exact version_range")
	}
	if !utf8.ValidString(value) || len(value) > maxExecutableNameBytes {
		return "", fmt.Errorf("must be a valid executable name of at most %d bytes", maxExecutableNameBytes)
	}
	if value == "." || value == ".." || strings.ContainsAny(value, `/\\`) ||
		strings.TrimSpace(value) != value {
		return "", fmt.Errorf("must be one executable name, not a path")
	}
	for _, r := range value {
		if r <= '\x1f' || r == '\x7f' {
			return "", fmt.Errorf("must not contain control characters")
		}
	}
	return value, nil
}

func parseInputBounds(node yaml.Node, measured bool, versionConds []versionCond) (InputBounds, error) {
	if node.Kind == 0 {
		return InputBounds{}, nil
	}
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return InputBounds{}, fmt.Errorf("must be a mapping with above and below")
	}
	if !measured {
		return InputBounds{}, fmt.Errorf("requires measured: true")
	}
	if len(versionConds) != 1 || versionConds[0].op != "=" {
		return InputBounds{}, fmt.Errorf("requires an exact version_range")
	}

	patterns := make(map[string][]inputBoundaryPattern, 2)
	for index := 0; index < len(node.Content); index += 2 {
		key, value := node.Content[index], node.Content[index+1]
		if key.Kind != yaml.ScalarNode || key.Tag != yamlStringTag ||
			(key.Value != "above" && key.Value != "below") {
			return InputBounds{}, fmt.Errorf("only above and below are allowed")
		}
		if _, duplicate := patterns[key.Value]; duplicate {
			return InputBounds{}, fmt.Errorf("duplicate %s", key.Value)
		}
		if value.Kind != yaml.SequenceNode || value.Tag != "!!seq" {
			return InputBounds{}, fmt.Errorf("%s: must be a list of expressions", key.Value)
		}
		side := make([]inputBoundaryPattern, len(value.Content))
		for expressionIndex, expression := range value.Content {
			boundary, err := parseInputBoundaryPattern(expression)
			if err != nil {
				return InputBounds{}, fmt.Errorf("%s[%d]: %w", key.Value, expressionIndex, err)
			}
			side[expressionIndex] = boundary
		}
		patterns[key.Value] = side
	}
	above, err := compileInputBoundaries("above", patterns["above"])
	if err != nil {
		return InputBounds{}, err
	}
	below, err := compileInputBoundaries("below", patterns["below"])
	if err != nil {
		return InputBounds{}, err
	}
	return InputBounds{Above: above, Below: below}, nil
}

type inputBoundaryPattern struct {
	pattern string
	negated bool
}

func parseInputBoundaryPattern(node *yaml.Node) (inputBoundaryPattern, error) {
	if node.Kind == yaml.ScalarNode && node.Tag == yamlStringTag {
		return inputBoundaryPattern{pattern: node.Value}, nil
	}
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content) != 2 {
		return inputBoundaryPattern{}, fmt.Errorf("must be a string expression or a single not expression")
	}
	key, value := node.Content[0], node.Content[1]
	if key.Kind != yaml.ScalarNode || key.Tag != yamlStringTag || key.Value != "not" {
		return inputBoundaryPattern{}, fmt.Errorf("only not is allowed in an expression mapping")
	}
	if value.Kind != yaml.ScalarNode || value.Tag != yamlStringTag {
		return inputBoundaryPattern{}, fmt.Errorf("not must contain a string expression")
	}
	return inputBoundaryPattern{pattern: value.Value, negated: true}, nil
}

func compileInputBoundaries(side string, patterns []inputBoundaryPattern) ([]InputBoundary, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("%s: must contain at least one expression", side)
	}
	boundaries := make([]InputBoundary, 0, len(patterns))
	for index, pattern := range patterns {
		if pattern.pattern == "" {
			return nil, fmt.Errorf("%s[%d]: must not be empty", side, index)
		}
		if pattern.negated && index != len(patterns)-1 {
			return nil, fmt.Errorf("%s[%d]: not is allowed only on the final outer boundary", side, index)
		}
		expression, err := regexp.Compile(pattern.pattern)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", side, index, err)
		}
		boundaries = append(boundaries, InputBoundary{
			Pattern: pattern.pattern,
			Negated: pattern.negated,
			expr:    expression,
		})
	}
	border := boundaries[len(boundaries)-1]
	if !border.Negated && (border.Matches("") || border.Matches("   ")) {
		return nil, fmt.Errorf("%s: final expression must identify a nonblank border", side)
	}
	return boundaries, nil
}

// parseInputArea reads the optional input_area, unknown when absent. Only a
// measurement shows the area is a single line, and a profile with a key
// that breaks the line inside the draft admits drafts of more lines. As
// insert_newline is a required key, no profile declares single_line yet.
// input_line is required of every profile.
func parseInputArea(node yaml.Node, measured bool, insertNewline string) (InputArea, error) {
	if node.Kind == 0 {
		return InputAreaUnknown, nil
	}
	if node.Kind != yaml.ScalarNode || node.Tag != yamlStringTag {
		return "", fmt.Errorf("must be single_line or unknown")
	}
	switch area := InputArea(node.Value); area {
	case InputAreaUnknown:
		return area, nil
	case InputAreaSingleLine:
		if !measured {
			return "", fmt.Errorf("single_line requires measured: true")
		}
		if insertNewline != "" {
			return "", fmt.Errorf("single_line requires no insert_newline")
		}
		return area, nil
	default:
		return "", fmt.Errorf("must be single_line or unknown")
	}
}

func parseInputPlaceholder(node yaml.Node, measured bool, versionConds []versionCond) (string, error) {
	if node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode || node.Tag != yamlStringTag {
		return "", fmt.Errorf("must be a string")
	}
	value := node.Value
	if value == "" {
		return "", fmt.Errorf("must not be empty")
	}
	if !measured {
		return "", fmt.Errorf("requires measured: true")
	}
	if len(versionConds) != 1 || versionConds[0].op != "=" {
		return "", fmt.Errorf("requires an exact version_range")
	}
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("must be valid UTF-8")
	}
	if utf8.RuneCountInString(value) > maxInputPlaceholderRunes {
		return "", fmt.Errorf("must be at most %d runes", maxInputPlaceholderRunes)
	}
	if strings.TrimSpace(value) != value {
		return "", fmt.Errorf("must not have leading or trailing space")
	}
	for _, r := range value {
		if r <= '\x1f' || r >= '\x7f' && r <= '\x9f' {
			return "", fmt.Errorf("must not contain control characters")
		}
	}
	return value, nil
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
