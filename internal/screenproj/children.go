package screenproj

import (
	"slices"
	"strings"
)

// MaxChildProcesses bounds a list of new child processes.
const MaxChildProcesses = 8

// maxChildName bounds a child process basename.
const maxChildName = 32

// ChildProcess reports whether entry follows the contract grammar of a
// new child process: an executable's basename of 1 to 32 ASCII letters,
// digits, '.', '_', '+' or '-', not starting with '-' or '.', optionally
// followed by one space and one verb from a closed set. Nothing else
// fits: no path, URL, flag, environment, free argument or text. The
// dataset window and the model input share it.
func ChildProcess(entry string) bool {
	name, verb, hasVerb := strings.Cut(entry, " ")
	return childName(name) && (!hasVerb || childVerb(verb))
}

// ModelChildProcesses is the list of new child processes the model sees:
// sorted and without duplicates, with nil, not observed, kept apart from
// [], observed none. A list with an entry outside ChildProcess, or with
// more than MaxChildProcesses entries, is unknown, nil, rather than send
// argv.
func ModelChildProcesses(list []string) []string {
	if list == nil {
		return nil
	}
	out := slices.Clone(list)
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) > MaxChildProcesses || slices.ContainsFunc(out, func(c string) bool { return !ChildProcess(c) }) {
		return nil
	}
	return out
}

// childName is a child process basename, which holds no slash,
// backslash, colon, space, control or non-ASCII byte, and cannot be a
// flag or a relative path.
func childName(s string) bool {
	if s == "" || len(s) > maxChildName || s[0] == '-' || s[0] == '.' {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '+', c == '-':
		default:
			return false
		}
	}
	return true
}

// childVerb reports a first argument from the closed set a child entry
// may carry. Any other argument could be a token, a name or a path.
func childVerb(arg string) bool {
	switch arg {
	case "build", "check", "compile", "exec", "fmt", "format", "generate", "install",
		"lint", "list", "run", "status", "test", "update", "vet":
		return true
	}
	return false
}
