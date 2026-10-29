// Package screenproj is the one projection of a terminal screen into the
// screen_lines the model sees, used alike to build the training dataset
// (gifford dataset build) and to serve the Analyst: for the same screen,
// input guard reading and policies, both give the same lines.
//
// Project composes, in this order and with no other way to do it: the
// human-input exclusion (human-input-v1), the cleaning of trailing blanks
// and of controls that show nothing, the redactor over the whole screen
// with its width, the domain mask or none, and the model cut (MaxLines,
// MaxRowRunes).
//
// Invariants: It is pure: no disk, network or clock, and it imports only
// the standard library, internal/protocol and internal/redact. A screen
// whose draft is not known closed never leaves it with a row of the
// draft: until the input block's exact bounds are known, the whole screen
// is blanked, and a cursor that is unknown or hidden fails closed. The
// cleaning never hides a secret from the redactor: a secret comes out
// redacted, or nothing comes out. Its errors name a stage, never screen
// content.
//
// It also holds the contract grammar of a child process entry, which the
// model input shares with the dataset window.
package screenproj

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// The model cut: at most MaxLines rows of the screen, the last ones, each
// of at most MaxRowRunes runes. The gateway applies the same cut to
// ANALYZE_AGENT_STATE, so a projected screen passes it unchanged.
const (
	MaxLines    = 40
	MaxRowRunes = 512
)

// HumanInputPolicyV1 is the version of the human-input exclusion.
const HumanInputPolicyV1 = "human-input-v1"

// Errors of Project. Each is wrapped with a fixed reason, never content.
var (
	// ErrNoCursor means the draft is not known closed and the cursor is
	// unknown or hidden, so nothing says the draft is off the screen.
	ErrNoCursor = errors.New("screen projection: draft not closed and cursor unknown or hidden")
	// ErrInvalidInput means the screen itself is out of bounds, or a row
	// is not rendered text the cleaning can keep a secret whole in.
	ErrInvalidInput = errors.New("screen projection: invalid screen")
	// ErrInvalidPolicies means the policies are missing or out of range.
	ErrInvalidPolicies = errors.New("screen projection: invalid policies")
	// ErrRedaction means the redactor failed or broke its contract.
	ErrRedaction = errors.New("screen projection: redaction failed")
)

// Cursor is the terminal cursor's row on the screen, as captured. Known
// is false when the source could not say where it was. Visible is
// whether the terminal showed it: a TUI that draws its own cursor hides
// the terminal's, which then stays where the TUI last wrote, so a hidden
// cursor does not locate the draft. The zero Cursor locates nothing.
type Cursor struct {
	Row     int
	Known   bool
	Visible bool
}

// locates reports whether the cursor is known and shown on the screen.
func (c Cursor) locates(rows int) bool {
	return c.Known && c.Visible && c.Row >= 0 && c.Row < rows
}

// Input is one captured screen: its rows top to bottom, its width in
// cells, the cursor and the input guard's draft state when it was
// captured. Only protocol.DraftStateClosed counts as closed.
type Input struct {
	Rows   []string
	Cols   int
	Cursor Cursor
	Draft  protocol.DraftState
}

// Redactor masks the secrets of a screen. *redact.Redactor satisfies it.
type Redactor interface {
	RedactScreen(s redact.Screen) (redact.ScreenResult, error)
}

// Policies are what a projection runs under: N, the redactor and the
// domain mask. Both callers take them from the same configuration.
type Policies struct {
	// Lines is N, how many of the screen's last rows the model sees,
	// 1 to MaxLines.
	Lines    int
	Redactor Redactor
	// Mask is NoMask() or a NewMask; the zero Mask is refused.
	Mask Mask
}

// Validate checks the policies on their own.
func (p Policies) Validate() error {
	switch {
	case p.Lines < 1 || p.Lines > MaxLines:
		return fmt.Errorf("%w: lines must be 1-%d", ErrInvalidPolicies, MaxLines)
	case p.Redactor == nil:
		return fmt.Errorf("%w: no redactor", ErrInvalidPolicies)
	case p.Mask.version == "":
		return fmt.Errorf("%w: no domain mask chosen", ErrInvalidPolicies)
	}
	return nil
}

// Versions are the policy versions a projection ran.
type Versions struct {
	HumanInput string
	Redaction  string
	DomainMask string
}

// Projection is the model's view of one screen.
type Projection struct {
	// Lines are the screen_lines, never nil: an empty screen is [].
	Lines []string
	// N is the Lines policy the cut ran with.
	N          int
	Versions   Versions
	Redactions int64
}

// Project projects one screen under p. It excludes the human input (the
// whole screen unless the draft is known closed, with the cursor known
// and shown), cleans every row, redacts the whole screen with its width,
// masks the domain names, and only then cuts: trailing empty rows go,
// then the last N rows are kept, each bounded to MaxRowRunes. Cleaning
// comes before the redactor, which refuses control runes, and so that no
// control inside a secret keeps it from the redactor. Any failure gives
// no lines.
func Project(in Input, p Policies) (Projection, error) {
	if err := p.Validate(); err != nil {
		return Projection{}, err
	}
	if in.Cols < 1 || in.Cols > redact.MaxCols {
		return Projection{}, fmt.Errorf("%w: width out of range", ErrInvalidInput)
	}
	if len(in.Rows) > redact.MaxRows {
		return Projection{}, fmt.Errorf("%w: too many rows", ErrInvalidInput)
	}
	rows := slices.Clone(in.Rows)
	// 1. The human input, human-input-v1.
	if in.Draft != protocol.DraftStateClosed {
		if !in.Cursor.locates(len(rows)) {
			return Projection{}, ErrNoCursor
		}
		for i := range rows {
			rows[i] = ""
		}
	}
	// 2. Cleaning.
	for i, r := range rows {
		c, ok := clean(r)
		if !ok {
			return Projection{}, fmt.Errorf("%w: a row is not rendered text", ErrInvalidInput)
		}
		rows[i] = c
	}
	// 3. The redactor, over the whole screen.
	res, err := p.Redactor.RedactScreen(redact.Screen{Rows: rows, Cols: in.Cols})
	if err != nil {
		// The redactor's own error may describe the screen.
		return Projection{}, fmt.Errorf("%w: the redactor refused the screen", ErrRedaction)
	}
	if len(res.Rows) != len(rows) || res.PolicyVersion == "" || res.Counts.Total < 0 {
		return Projection{}, fmt.Errorf("%w: the redactor broke its contract", ErrRedaction)
	}
	// 4. The domain mask.
	masked := p.Mask.apply(res.Rows)
	// 5. The model cut.
	return Projection{
		Lines:      cut(masked, p.Lines),
		N:          p.Lines,
		Versions:   Versions{HumanInput: HumanInputPolicyV1, Redaction: res.PolicyVersion, DomainMask: p.Mask.version},
		Redactions: res.Counts.Total,
	}, nil
}

// clean is a row as the model reads it: its trailing blanks go, and so do
// the controls that show nothing, such as a bell or NUL, so that one
// inside a secret joins it again for the redactor. Nothing else is
// dropped or replaced. A row that is not valid UTF-8, or that holds a
// control with an effect on the screen, is refused (ok false): cleaned,
// it could hide a secret from the redactor.
func clean(r string) (string, bool) {
	if !utf8.ValidString(r) {
		return "", false
	}
	r = strings.TrimRightFunc(r, unicode.IsSpace)
	if !strings.ContainsFunc(r, unicode.IsControl) {
		return r, true
	}
	var b strings.Builder
	for _, c := range r {
		switch {
		case !unicode.IsControl(c):
			b.WriteRune(c)
		case onScreen(c):
			return "", false
		}
	}
	return strings.TrimRightFunc(b.String(), unicode.IsSpace), true
}

// onScreen reports a control with an effect on the screen: it moves the
// cursor, so later text overwrites or leaves cells (backspace, tab, line
// feed, vertical tab, form feed, carriage return), starts an escape
// sequence whose printable parameters would stay inside the row (escape),
// or is a C1 control.
func onScreen(c rune) bool {
	switch c {
	case '\b', '\t', '\n', '\v', '\f', '\r', 0x1b:
		return true
	}
	return c >= 0x80 && c <= 0x9f
}

// cut is the model cut, the same as the gateway's: trailing empty rows
// go, then the last n rows are kept, each bounded to MaxRowRunes.
func cut(rows []string, n int) []string {
	for len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}
	rows = rows[max(0, len(rows)-n):]
	out := make([]string, len(rows))
	for i, r := range rows {
		if rs := []rune(r); len(rs) > MaxRowRunes {
			r = string(rs[:MaxRowRunes])
		}
		out[i] = r
	}
	return out
}
