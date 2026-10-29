package profiles

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	profiledata "github.com/smellak/gifford/profiles"
)

// Supported harnesses, one profiles/<harness>.yaml each.
var harnessNames = [...]string{"claude", "codex", "muse", "opencode", "antigravity"}

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
// pattern; false means the line is not an input line.
func (p Profile) Draft(line string) (string, bool) {
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

// Profile returns a copy of the named harness profile; any other name
// returns ErrUnknownHarness.
func (ps Profiles) Profile(name string) (Profile, error) {
	profile, ok := ps.byName[name]
	if !ok {
		return Profile{}, fmt.Errorf("%w: %s", ErrUnknownHarness, name)
	}
	return profile, nil
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
