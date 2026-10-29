package screenproj

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// DomainMaskNone is the policy version of a projection without a domain
// mask: variant B of the A/B comparison.
const DomainMaskNone = "none"

// domainMaskPolicy prefixes the version of a domain mask; the list's
// fingerprint follows it.
const domainMaskPolicy = "domain-mask-v1:"

// MaskMarker replaces every masked name.
const MaskMarker = "[domain]"

// Mask list bounds.
const (
	maxMaskNames    = 4096
	maxMaskNameRune = 128
	maxMaskBytes    = 1 << 20
)

// ErrMaskList means a domain mask list is not valid. It never quotes the
// list.
var ErrMaskList = errors.New("invalid domain mask list")

// ParseMaskList reads a domain mask list: one name per line; blank lines
// and lines starting with # are skipped, and each name is trimmed. A
// list with no name is refused: NoMask says that. Opening the list is
// the caller's.
func ParseMaskList(r io.Reader) ([]string, error) {
	sc := bufio.NewScanner(io.LimitReader(r, maxMaskBytes+1))
	sc.Buffer(make([]byte, 0, 4096), maxMaskBytes+1)
	var names []string
	read, line := 0, 0
	for sc.Scan() {
		line++
		read += len(sc.Bytes()) + 1
		if read > maxMaskBytes {
			return nil, fmt.Errorf("%w: larger than 1 MiB", ErrMaskList)
		}
		name := strings.TrimSpace(sc.Text())
		if name == "" || strings.HasPrefix(name, "#") {
			continue
		}
		if !utf8.ValidString(name) || utf8.RuneCountInString(name) > maxMaskNameRune ||
			strings.ContainsFunc(name, unicode.IsControl) {
			return nil, fmt.Errorf("%w: line %d: not a name of 1-%d printable characters", ErrMaskList, line, maxMaskNameRune)
		}
		names = append(names, name)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: unreadable", ErrMaskList)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: no name", ErrMaskList)
	}
	if len(names) > maxMaskNames {
		return nil, fmt.Errorf("%w: more than %d names", ErrMaskList, maxMaskNames)
	}
	return names, nil
}

// Mask is the domain mask of a projection: NoMask, or a NewMask that
// replaces each listed name with MaskMarker, ignoring case, as a whole
// word only: a match next to a letter, digit or underscore is left
// alone. The zero Mask is no choice, and Policies refuses it.
type Mask struct {
	version string
	re      *regexp.Regexp
}

// NoMask masks nothing and names DomainMaskNone.
func NoMask() Mask { return Mask{version: DomainMaskNone} }

// NewMask builds the mask of a list from ParseMaskList. Its version
// names the list by the first 12 hex digits of the SHA-256 of the
// normalised list: lower-cased, sorted, one name per line.
func NewMask(names []string) (Mask, error) {
	if len(names) == 0 {
		return Mask{}, fmt.Errorf("%w: no name", ErrMaskList)
	}
	norm := make([]string, 0, len(names))
	for _, n := range names {
		if n == "" || !utf8.ValidString(n) || utf8.RuneCountInString(n) > maxMaskNameRune ||
			strings.ContainsFunc(n, unicode.IsControl) {
			return Mask{}, fmt.Errorf("%w: not a name of 1-%d printable characters", ErrMaskList, maxMaskNameRune)
		}
		norm = append(norm, strings.ToLower(n))
	}
	if len(norm) > maxMaskNames {
		return Mask{}, fmt.Errorf("%w: more than %d names", ErrMaskList, maxMaskNames)
	}
	slices.Sort(norm)
	norm = slices.Compact(norm)
	sum := sha256.Sum256([]byte(strings.Join(norm, "\n") + "\n"))
	// One pass, longest name first, so a name inside a longer one never
	// splits it and the marker is never masked again.
	alts := slices.Clone(norm)
	slices.SortStableFunc(alts, func(a, b string) int { return len(b) - len(a) })
	for i, n := range alts {
		alts[i] = regexp.QuoteMeta(n)
	}
	return Mask{
		version: domainMaskPolicy + hex.EncodeToString(sum[:])[:12],
		re:      regexp.MustCompile("(?i)(?:" + strings.Join(alts, "|") + ")"),
	}, nil
}

// Version is the policy version the mask names; "" for the zero Mask.
func (m Mask) Version() string { return m.version }

// apply masks every row.
func (m Mask) apply(rows []string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		if m.re != nil {
			r = maskWord(m.re, r)
		}
		out[i] = r
	}
	return out
}

// maskWord replaces the whole-word matches of re in s.
func maskWord(re *regexp.Regexp, s string) string {
	var b strings.Builder
	last := 0
	for _, loc := range re.FindAllStringIndex(s, -1) {
		before, _ := utf8.DecodeLastRuneInString(s[:loc[0]])
		after, _ := utf8.DecodeRuneInString(s[loc[1]:])
		if loc[0] > 0 && wordRune(before) || loc[1] < len(s) && wordRune(after) {
			continue
		}
		b.WriteString(s[last:loc[0]])
		b.WriteString(MaskMarker)
		last = loc[1]
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func wordRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }
