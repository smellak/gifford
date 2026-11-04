package setup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// readJSONObject reads a JSON object file. A missing or empty file is an
// empty object; anything that is not one JSON object is refused, so setup
// never rewrites a file it cannot read back exactly, such as JSON with
// comments.
func readJSONObject(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) || err == nil && len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, fmt.Errorf("%s is not a plain JSON object; merge it by hand", path)
	}
	if dec.More() {
		return nil, fmt.Errorf("%s holds more than one JSON value; merge it by hand", path)
	}
	return obj, nil
}

func writeJSONObject(path string, obj map[string]any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(obj); err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return writeFileAtomic(path, buf.Bytes(), fileMode(path))
}

// object returns obj[key] as an object, creating it when absent. An existing
// value of another type is refused rather than replaced.
func object(obj map[string]any, key, path string) (map[string]any, error) {
	value, ok := obj[key]
	if !ok {
		child := map[string]any{}
		obj[key] = child
		return child, nil
	}
	child, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: %q is not an object; merge it by hand", path, key)
	}
	return child, nil
}

func stringList(obj map[string]any, key, path string) ([]any, error) {
	value, ok := obj[key]
	if !ok {
		return nil, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: %q is not a list; merge it by hand", path, key)
	}
	return list, nil
}

// mergeHooks adds every hook group of canonical, a {"hooks": {...}} object,
// whose command is not already registered for that event. It reports whether
// anything was added.
func mergeHooks(settings map[string]any, canonical []byte, path string) (bool, error) {
	var want struct {
		Hooks map[string][]map[string]any `json:"hooks"`
	}
	if err := json.Unmarshal(canonical, &want); err != nil {
		return false, fmt.Errorf("decode embedded hooks: %w", err)
	}
	hooks, err := object(settings, "hooks", path)
	if err != nil {
		return false, err
	}
	changed := false
	for event, groups := range want.Hooks {
		existing, err := stringList(hooks, event, path+": hooks")
		if err != nil {
			return false, err
		}
		for _, group := range groups {
			if hasHookCommand(existing, groupCommands(group)) {
				continue
			}
			existing = append(existing, group)
			changed = true
		}
		hooks[event] = existing
	}
	return changed, nil
}

func groupCommands(group map[string]any) []string {
	var commands []string
	list, _ := group["hooks"].([]any)
	for _, item := range list {
		if hook, ok := item.(map[string]any); ok {
			if command, ok := hook["command"].(string); ok {
				commands = append(commands, command)
			}
		}
	}
	return commands
}

// hasHookCommand reports whether any existing group already runs one of
// commands.
func hasHookCommand(existing []any, commands []string) bool {
	for _, item := range existing {
		group, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for _, have := range groupCommands(group) {
			for _, want := range commands {
				if strings.TrimSpace(have) == want {
					return true
				}
			}
		}
	}
	return false
}

func hooksPresent(path string, canonical []byte) bool {
	settings, err := readJSONObject(path)
	if err != nil {
		return false
	}
	changed, err := mergeHooks(settings, canonical, path)
	return err == nil && !changed
}

// appendMissing adds each of values not already in obj[key].
func appendMissing(obj map[string]any, key, path string, values []string) (bool, error) {
	list, err := stringList(obj, key, path)
	if err != nil {
		return false, err
	}
	changed := false
	for _, value := range values {
		found := false
		for _, item := range list {
			if item == value {
				found = true
				break
			}
		}
		if !found {
			list = append(list, value)
			changed = true
		}
	}
	obj[key] = list
	return changed, nil
}

// readJSONCObject reads a JSON-with-comments object for inspection only:
// setup never writes such a file, because rewriting it would drop the
// user's comments. Comments and trailing commas are blanked out first.
func readJSONCObject(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	stripped := stripJSONC(raw)
	if len(bytes.TrimSpace(stripped)) == 0 {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(stripped))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil || dec.More() {
		return nil, fmt.Errorf("%s is not a JSON object with comments", path)
	}
	return obj, nil
}

// stripJSONC replaces // and /* */ comments and the commas that only
// precede a closing } or ] with spaces, leaving string contents untouched.
func stripJSONC(raw []byte) []byte {
	out := bytes.Clone(raw)
	blank := func(from, to int) {
		for i := from; i < to && i < len(out); i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}
	for i := 0; i < len(out); i++ {
		switch {
		case out[i] == '"':
			for i++; i < len(out) && out[i] != '"'; i++ {
				if out[i] == '\\' {
					i++
				}
			}
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '/':
			end := bytes.IndexByte(out[i:], '\n')
			if end < 0 {
				end = len(out) - i
			}
			blank(i, i+end)
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '*':
			end := bytes.Index(out[i+2:], []byte("*/"))
			if end < 0 {
				blank(i, len(out))
				return out
			}
			blank(i, i+2+end+2)
		}
	}
	// A comma followed only by blanks and a closing bracket is trailing.
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != ',' || inJSONString(out, i) {
			continue
		}
		j := i + 1
		for j < len(out) && (out[j] == ' ' || out[j] == '\t' || out[j] == '\n' || out[j] == '\r') {
			j++
		}
		if j < len(out) && (out[j] == '}' || out[j] == ']') {
			out[i] = ' '
		}
	}
	return out
}

// inJSONString reports whether offset at sits inside a string literal of
// comment-free JSON.
func inJSONString(raw []byte, at int) bool {
	in := false
	for i := 0; i < at; i++ {
		switch {
		case in && raw[i] == '\\':
			i++
		case raw[i] == '"':
			in = !in
		}
	}
	return in
}
