package redact

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// Placeholder replaces one masked cell. It is East Asian Ambiguous,
	// so it keeps width 1 under the v1 model like the ASCII it covers.
	Placeholder = '•'

	// MaxInputBytes bounds one call: the Text, or the sum of a Screen's rows.
	MaxInputBytes = 1 << 20
	// MaxRows bounds the rows of one Screen.
	MaxRows = 10000
	// MaxCols bounds the Cols of one Screen.
	MaxCols = 10000
	// MaxSensitivePaths bounds Options.SensitivePaths.
	MaxSensitivePaths = 256
)

// Screen is one reconstructed terminal screen: Rows hold printable text,
// one string per terminal row, and Cols holds the terminal width. Cols 0
// means unknown: rows never join into logical lines.
type Screen struct {
	Rows []string
	Cols int
}

// Counts holds the substitutions of one result: Total overall and per
// kind. ByKind is nil when Total is 0. Counts never hold content.
type Counts struct {
	Total  int64          `json:"total"`
	ByKind map[Kind]int64 `json:"by_kind,omitempty"`
}

// Add returns the sum in a new map; both operands keep their maps.
func (c Counts) Add(o Counts) Counts {
	total := c.Total + o.Total
	if total == 0 {
		return Counts{}
	}
	merged := make(map[Kind]int64, len(c.ByKind)+len(o.ByKind))
	for k, v := range c.ByKind {
		merged[k] += v
	}
	for k, v := range o.ByKind {
		merged[k] += v
	}
	for k, v := range merged {
		if v == 0 {
			delete(merged, k)
		}
	}
	return Counts{Total: total, ByKind: merged}
}

// ScreenResult is the redacted screen: Rows has the input's row count with
// each row at its input CellWidth, PolicyVersion names the rule bundle,
// and Counts holds the substitutions. Any error returns the zero result.
type ScreenResult struct {
	Rows          []string
	PolicyVersion string
	Counts        Counts
}

// TextResult is the redacted text: Text has the input's lines, each at its
// input CellWidth. Any error returns the zero result.
type TextResult struct {
	Text          string
	PolicyVersion string
	Counts        Counts
}

// Redactor masks secrets in screens and free text. The zero value is
// usable: it validates its input, then returns ErrInternal, since the rule
// bundle arrives with New. A Redactor is immutable and safe for concurrent
// use.
type Redactor struct {
	rules *ruleSet
}

// RedactScreen masks the secrets of s, joining soft-wrapped rows into
// logical lines first. Consumers define their own two-method interface.
func (r *Redactor) RedactScreen(s Screen) (out ScreenResult, err error) {
	defer func() {
		if recover() != nil {
			out = ScreenResult{}
			err = fmt.Errorf("redact screen: internal failure: %w", ErrInternal)
		}
	}()
	widths, err := checkScreen(s)
	if err != nil {
		return ScreenResult{}, err
	}
	rs := r.bundle()
	if rs == nil || len(rs.rules) == 0 {
		return ScreenResult{}, fmt.Errorf("redact screen: internal failure: no rules: %w", ErrInternal)
	}
	lines, segs := joinRows(s.Rows, s.Cols)
	masked, counts, err := run(lines, rs, "redact screen")
	if err != nil {
		return ScreenResult{}, err
	}
	rows := splitRows(lines, masked, segs, len(s.Rows))
	for i, row := range rows {
		if CellWidth(row) != widths[i] {
			return ScreenResult{}, fmt.Errorf(
				"redact screen: internal failure at row %d: width changed: %w", i, ErrInternal,
			)
		}
	}
	return ScreenResult{Rows: rows, PolicyVersion: rs.version, Counts: counts}, nil
}

// RedactText masks the secrets of s, one logical line per newline-separated
// line. Tab, carriage return and newline survive validation; any other
// control rune fails the call.
func (r *Redactor) RedactText(s string) (out TextResult, err error) {
	defer func() {
		if recover() != nil {
			out = TextResult{}
			err = fmt.Errorf("redact text: internal failure: %w", ErrInternal)
		}
	}()
	widths, err := checkText(s)
	if err != nil {
		return TextResult{}, err
	}
	rs := r.bundle()
	if rs == nil || len(rs.rules) == 0 {
		return TextResult{}, fmt.Errorf("redact text: internal failure: no rules: %w", ErrInternal)
	}
	lines := strings.Split(s, "\n")
	masked, counts, err := run(lines, rs, "redact text")
	if err != nil {
		return TextResult{}, err
	}
	texts := make([]string, len(masked))
	for i, line := range masked {
		texts[i] = line.text
		if CellWidth(line.text) != widths[i] {
			return TextResult{}, fmt.Errorf(
				"redact text: internal failure at line %d: width changed: %w", i, ErrInternal,
			)
		}
	}
	return TextResult{Text: strings.Join(texts, "\n"), PolicyVersion: rs.version, Counts: counts}, nil
}

// bundle returns the rule bundle, tolerating a nil receiver so a nil
// Redactor behaves like the zero value.
func (r *Redactor) bundle() *ruleSet {
	if r == nil {
		return nil
	}
	return r.rules
}

// checkScreen validates a screen, returning the input row widths for the
// output width guard.
func checkScreen(s Screen) ([]int, error) {
	if len(s.Rows) > MaxRows {
		return nil, fmt.Errorf(
			"redact screen: input too large: %d rows (max %d): %w",
			len(s.Rows), MaxRows, ErrInputTooLarge,
		)
	}
	total := 0
	for _, row := range s.Rows {
		total += len(row)
	}
	if total > MaxInputBytes {
		return nil, fmt.Errorf(
			"redact screen: input too large: %d bytes (max %d): %w",
			total, MaxInputBytes, ErrInputTooLarge,
		)
	}
	if s.Cols < 0 || s.Cols > MaxCols {
		return nil, fmt.Errorf(
			"redact screen: invalid input: cols %d out of range 0..%d: %w",
			s.Cols, MaxCols, ErrInvalidInput,
		)
	}
	widths := make([]int, len(s.Rows))
	for i, row := range s.Rows {
		if !utf8.ValidString(row) {
			return nil, fmt.Errorf(
				"redact screen: invalid input at row %d: not UTF-8: %w", i, ErrInvalidInput,
			)
		}
		if strings.IndexFunc(row, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf(
				"redact screen: invalid input at row %d: control rune: %w", i, ErrInvalidInput,
			)
		}
		widths[i] = CellWidth(row)
	}
	return widths, nil
}

// checkText validates free text, returning the input line widths for the
// output width guard.
func checkText(s string) ([]int, error) {
	if len(s) > MaxInputBytes {
		return nil, fmt.Errorf(
			"redact text: input too large: %d bytes (max %d): %w",
			len(s), MaxInputBytes, ErrInputTooLarge,
		)
	}
	if i := firstInvalidByte(s); i >= 0 {
		return nil, fmt.Errorf(
			"redact text: invalid input at byte %d: not UTF-8: %w", i, ErrInvalidInput,
		)
	}
	for i, c := range s {
		if unicode.IsControl(c) && c != '\t' && c != '\r' && c != '\n' {
			return nil, fmt.Errorf(
				"redact text: invalid input at byte %d: control rune: %w", i, ErrInvalidInput,
			)
		}
	}
	lines := strings.Split(s, "\n")
	widths := make([]int, len(lines))
	for i, line := range lines {
		widths[i] = CellWidth(line)
	}
	return widths, nil
}

// firstInvalidByte returns the offset of the first invalid UTF-8 byte, or
// -1 when s is valid.
func firstInvalidByte(s string) int {
	i := 0
	for i < len(s) {
		if s[i] < utf8.RuneSelf {
			i++
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		if size < 2 {
			return i
		}
		i += size
	}
	return -1
}
