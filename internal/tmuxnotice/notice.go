package tmuxnotice

import "strings"

const (
	// Prefix makes a notice a shell no-op if it ever reaches a shell prompt.
	Prefix    = ": "
	maxLength = 512
)

// Valid reports whether value starts with Prefix and then uses only the
// closed ASCII alphabet that the shell no-op cannot expand, quote, redirect,
// or chain. The complete value is limited to 512 characters.
func Valid(value string) bool {
	if len(value) <= len(Prefix) || len(value) > maxLength || !strings.HasPrefix(value, Prefix) {
		return false
	}
	for index := len(Prefix); index < len(value); index++ {
		char := value[index]
		if char != ' ' && char != '_' && char != '.' && char != ',' && char != ':' &&
			char != '[' && char != ']' && char != '-' &&
			(char < 'A' || char > 'Z') && (char < 'a' || char > 'z') &&
			(char < '0' || char > '9') {
			return false
		}
	}
	return true
}
