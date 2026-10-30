package capture

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/vt"
)

// Format names the format of an imported capture.
type Format string

const (
	// FormatAsciicastV2 is an asciicast v2 file, the only format with an
	// import so far.
	FormatAsciicastV2 Format = "asciicast-v2"
	// FormatAsciicastV3 is an asciicast v3 file: planned, not imported yet.
	FormatAsciicastV3 Format = "asciicast-v3"
	// FormatDay0 is a day-0 capture directory, the panes observed before
	// the binary existed: planned, not imported yet.
	FormatDay0 Format = "day0"
)

// Known reports a format the import knows of, imported yet or not.
func (f Format) Known() bool {
	switch f {
	case FormatAsciicastV2, FormatAsciicastV3, FormatDay0:
		return true
	default:
		return false
	}
}

const (
	// MaxCastBytes bounds an imported cast. A cast holds output as
	// JSON-escaped text, so 1 GiB covers days of a busy terminal, while
	// the import, which streams the file and holds one line at a time,
	// never writes more to the recordings dir than about that.
	MaxCastBytes = 1 << 30
	// maxCastLineBytes bounds one line of a cast, its newline included.
	// asciinema writes one
	// event per read from the terminal, a few KiB at most, so even fully
	// escaped an event stays far below 4 MiB, and no more than one line
	// is ever in memory.
	maxCastLineBytes = 4 << 20
	// maxCastSeconds bounds an event's time: its nanoseconds must fit
	// the recording's monotonic clock.
	maxCastSeconds = 1 << 32
	// castVersion is the asciicast version this reader reads.
	castVersion = 2
)

// maxWall is the last wall time a recording can carry: RFC 3339 has four
// year digits.
var maxWall = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// The event codes of asciicast v2: output, input, marker and resize.
const (
	codeOutput = "o"
	codeInput  = "i"
	codeMarker = "m"
	codeResize = "r"
)

// ErrBadCast means a cast is not one an import can take whole. Its
// messages name a line and a reason, never what the line holds.
var ErrBadCast = errors.New("capture: not an asciicast v2 file an import can take")

// castHeader is all of an asciicast v2 header an import reads. Every
// other field, title and env included, is never read into anything.
type castHeader struct {
	Version   *int64 `json:"version"`
	Width     *int64 `json:"width"`
	Height    *int64 `json:"height"`
	Timestamp *int64 `json:"timestamp"`
}

// castEvent is one event of a cast. Data is set only for output and
// resize: typed input and markers are counted, never decoded.
type castEvent struct {
	at   int64
	code string
	data string
}

// castReader reads an asciicast v2 stream, one line at a time.
type castReader struct {
	sc    *bufio.Scanner
	line  int
	start time.Time
	last  int64
}

func newCastReader(r io.Reader) *castReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxCastLineBytes)
	return &castReader{sc: sc}
}

func (c *castReader) bad(what string) error {
	return fmt.Errorf("%w: line %d: %s", ErrBadCast, c.line, what)
}

// scan reads the next line, false at the end of the stream.
func (c *castReader) scan() (bool, error) {
	if c.sc.Scan() {
		c.line++
		return true, nil
	}
	err := c.sc.Err()
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, bufio.ErrTooLong):
		c.line++
		return false, c.bad(fmt.Sprintf("longer than %d bytes", maxCastLineBytes))
	default:
		return false, fmt.Errorf("capture: read cast: %w", err)
	}
}

// header reads the first line: version 2, a size the screen can take and
// the wall time the recording started at, which is never made up.
func (c *castReader) header() (cols, rows int, err error) {
	ok, err := c.scan()
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("%w: no header", ErrBadCast)
	}
	var h castHeader
	if !json.Valid(c.sc.Bytes()) || json.Unmarshal(c.sc.Bytes(), &h) != nil {
		return 0, 0, c.bad("header is not a JSON object")
	}
	switch {
	case h.Version == nil || *h.Version != castVersion:
		return 0, 0, c.bad("not asciicast version 2")
	case h.Width == nil || h.Height == nil || !screenFits(*h.Width, *h.Height):
		return 0, 0, c.bad("no width and height a screen can take")
	case h.Timestamp == nil:
		return 0, 0, c.bad("no timestamp: the wall clock is not made up")
	case *h.Timestamp < 1 || *h.Timestamp > maxWall.Unix():
		return 0, 0, c.bad("timestamp out of range")
	}
	c.start = time.Unix(*h.Timestamp, 0).UTC()
	return int(*h.Width), int(*h.Height), nil
}

// next reads one event, io.EOF after the last. Its time never goes back.
func (c *castReader) next() (castEvent, error) {
	ok, err := c.scan()
	if err != nil {
		return castEvent{}, err
	}
	if !ok {
		return castEvent{}, io.EOF
	}
	line := c.sc.Bytes()
	if !json.Valid(line) {
		return castEvent{}, c.bad("not JSON")
	}
	var parts []json.RawMessage
	if json.Unmarshal(line, &parts) != nil || len(parts) != 3 {
		return castEvent{}, c.bad("not an event of time, code and data")
	}
	var at *float64
	if json.Unmarshal(parts[0], &at) != nil || at == nil {
		return castEvent{}, c.bad("malformed time")
	}
	if *at < 0 || *at > maxCastSeconds {
		return castEvent{}, c.bad("time out of range")
	}
	ns := int64(math.Round(*at * float64(time.Second)))
	if ns < c.last {
		return castEvent{}, c.bad("time goes back")
	}
	if c.start.Add(time.Duration(ns)).After(maxWall) {
		return castEvent{}, c.bad("time out of range")
	}
	c.last = ns
	var code *string
	if json.Unmarshal(parts[1], &code) != nil || code == nil {
		return castEvent{}, c.bad("malformed event code")
	}
	ev := castEvent{at: ns, code: *code}
	switch ev.code {
	case codeOutput, codeResize:
		var data *string
		if json.Unmarshal(parts[2], &data) != nil || data == nil {
			return castEvent{}, c.bad("malformed event data")
		}
		ev.data = *data
	case codeInput, codeMarker:
		// The line is valid JSON, so data opening with a quote is a
		// whole string: it is checked without being decoded.
		if len(parts[2]) == 0 || parts[2][0] != '"' {
			return castEvent{}, c.bad("malformed event data")
		}
	default:
		return castEvent{}, c.bad("unknown event code")
	}
	return ev, nil
}

// screenFits reports a size the screen emulator takes.
func screenFits(cols, rows int64) bool {
	return cols >= 1 && rows >= 1 && cols <= vt.MaxCols && rows <= vt.MaxRows && cols*rows <= vt.MaxCells
}

// parseSize reads a resize's COLSxROWS: two decimal numbers, nothing
// else, that the screen takes.
func parseSize(s string) (cols, rows int, ok bool) {
	c, r, found := strings.Cut(s, "x")
	if !found || !digits(c) || !digits(r) {
		return 0, 0, false
	}
	cols, errC := strconv.Atoi(c)
	rows, errR := strconv.Atoi(r)
	if errC != nil || errR != nil || !screenFits(int64(cols), int64(rows)) {
		return 0, 0, false
	}
	return cols, rows, true
}

// digits reports one to five ASCII digits: no sign, space or other
// number form.
func digits(s string) bool {
	if len(s) < 1 || len(s) > 5 {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
