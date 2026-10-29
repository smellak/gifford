package dataset

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// Cursor is the terminal cursor's row at a window's anchor, as the
// replayed screen left it. Known is false when the source could not say
// where it was.
type Cursor struct {
	Row   int
	Known bool
}

// ErrNoCursor means the human-input exclusion was asked to clear the input
// block of a screen whose cursor is unknown. The build drops such windows
// before the pipeline, so in a run it is a stage failure.
var ErrNoCursor = errors.New("human input exclusion: cursor unknown and draft not closed")

// HumanInputPolicyV1 is the policy version HumanInputV1 names.
const HumanInputPolicyV1 = "human-input-v1"

// maxInputRowsAbove bounds the non-blank rows above the cursor that the
// input block takes in.
const maxInputRowsAbove = 8

// HumanInputV1 removes what a human may have typed. Unless the draft is
// known closed it blanks the input block: the cursor row, every row
// below it, and the non-blank rows right above it, at most
// maxInputRowsAbove. Rows are blanked, never dropped, so the screen keeps
// its size. With the draft closed it removes nothing.
type HumanInputV1 struct{}

// ExcludeHumanInput implements HumanInputExcluder.
func (HumanInputV1) ExcludeHumanInput(s redact.Screen, draft DraftState, cursor Cursor) (redact.Screen, string, error) {
	rows := slices.Clone(s.Rows)
	if draft.Closed() {
		return redact.Screen{Rows: rows, Cols: s.Cols}, HumanInputPolicyV1, nil
	}
	if !cursor.Known || cursor.Row < 0 || cursor.Row >= len(rows) {
		return redact.Screen{}, "", ErrNoCursor
	}
	for i := cursor.Row; i < len(rows); i++ {
		rows[i] = ""
	}
	for i, n := cursor.Row-1, 0; i >= 0 && n < maxInputRowsAbove && strings.TrimSpace(rows[i]) != ""; i, n = i-1, n+1 {
		rows[i] = ""
	}
	return redact.Screen{Rows: rows, Cols: s.Cols}, HumanInputPolicyV1, nil
}

// DomainMaskNone is the policy version of a build without a domain mask:
// variant B of the A/B comparison.
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
// list with no name is refused: --no-mask says that.
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

// DomainMask replaces each listed name with MaskMarker, ignoring case,
// as a whole word only: a match next to a letter, digit or underscore is
// left alone.
type DomainMask struct {
	version string
	re      *regexp.Regexp
}

// NewDomainMask builds the mask of a list from ParseMaskList. Its
// version names the list by the first 12 hex digits of the SHA-256 of
// the normalised list: lower-cased, sorted, one name per line.
func NewDomainMask(names []string) (*DomainMask, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: no name", ErrMaskList)
	}
	norm := make([]string, 0, len(names))
	for _, n := range names {
		norm = append(norm, strings.ToLower(n))
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
	return &DomainMask{
		version: domainMaskPolicy + hex.EncodeToString(sum[:])[:12],
		re:      regexp.MustCompile("(?i)(?:" + strings.Join(alts, "|") + ")"),
	}, nil
}

// Version is the policy version the mask names.
func (m *DomainMask) Version() string { return m.version }

// MaskDomain implements DomainMasker.
func (m *DomainMask) MaskDomain(s redact.Screen) (redact.Screen, string, error) {
	rows := make([]string, len(s.Rows))
	for i, r := range s.Rows {
		rows[i] = maskWord(m.re, r)
	}
	return redact.Screen{Rows: rows, Cols: s.Cols}, m.version, nil
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

// NoMask masks nothing and names DomainMaskNone.
type NoMask struct{}

// MaskDomain implements DomainMasker.
func (NoMask) MaskDomain(s redact.Screen) (redact.Screen, string, error) {
	return redact.Screen{Rows: slices.Clone(s.Rows), Cols: s.Cols}, DomainMaskNone, nil
}

// splitDomain separates split-v1's hash from every other use of the
// group digest.
const splitDomain = "gifford.dataset.split.v1"

// SplitV1 assigns a group's split from the group digest alone: the
// SHA-256 of the domain and the digest gives a number from 0 to 99; train
// below 70, validation from 70 to 84, test from 85. The same group always
// lands in the same split, in any run.
type SplitV1 struct{}

// Split implements Splitter.
func (SplitV1) Split(group protocol.Digest) (Split, error) {
	if !group.Valid() {
		return "", errors.New("split: not a group digest")
	}
	h := sha256.New()
	_, _ = io.WriteString(h, splitDomain)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, string(group))
	n := binary.BigEndian.Uint64(h.Sum(nil)[:8]) % 100
	switch {
	case n < 70:
		return SplitTrain, nil
	case n < 85:
		return SplitValidation, nil
	default:
		return SplitTest, nil
	}
}
