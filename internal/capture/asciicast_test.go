package capture

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readCast reads a whole cast, header and events.
func readCast(body string) ([]castEvent, error) {
	cr := newCastReader(strings.NewReader(body))
	if _, _, err := cr.header(); err != nil {
		return nil, err
	}
	var out []castEvent
	for {
		ev, err := cr.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

func TestCastReadsOnlyWhatAnImportUses(t *testing.T) {
	t.Parallel()
	cr := newCastReader(strings.NewReader(fixtureHeader() + "\n" + strings.Join(fixtureEvents(), "\n")))
	cols, rows, err := cr.header()
	require.NoError(t, err)
	require.Equal(t, [2]int{80, 24}, [2]int{cols, rows})
	require.Equal(t, time.Date(2025, 10, 30, 12, 0, 0, 0, time.UTC), cr.start)
	var got []castEvent
	for {
		ev, err := cr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, ev)
	}
	// Typed input and markers keep no data: they are never decoded.
	require.Equal(t, []castEvent{
		{at: 250 * int64(time.Millisecond), code: codeOutput, data: "fixture build ok\r\n"},
		{at: 500 * int64(time.Millisecond), code: codeInput},
		{at: 750 * int64(time.Millisecond), code: codeOutput, data: "$ "},
		{at: 1500 * int64(time.Millisecond), code: codeResize, data: "100x30"},
		{at: 2000 * int64(time.Millisecond), code: codeMarker},
		{at: 2500 * int64(time.Millisecond), code: codeOutput, data: "\x1b[1mdone\x1b[0m\r\n$ "},
		{at: 3250 * int64(time.Millisecond), code: codeInput},
	}, got)
}

func TestCastHeaderIsRefused(t *testing.T) {
	t.Parallel()
	for name, header := range map[string]string{
		"no line":           "",
		"not JSON":          `{"version": 2,`,
		"an array":          `[2, 80, 24]`,
		"no version":        `{"width": 80, "height": 24, "timestamp": 1}`,
		"version 3":         `{"version": 3, "term": {"cols": 80, "rows": 24}, "timestamp": 1}`,
		"no width":          `{"version": 2, "height": 24, "timestamp": 1}`,
		"zero height":       `{"version": 2, "width": 80, "height": 0, "timestamp": 1}`,
		"too wide":          `{"version": 2, "width": 10001, "height": 24, "timestamp": 1}`,
		"too many cells":    `{"version": 2, "width": 1000, "height": 1000, "timestamp": 1}`,
		"no timestamp":      `{"version": 2, "width": 80, "height": 24}`,
		"null timestamp":    `{"version": 2, "width": 80, "height": 24, "timestamp": null}`,
		"zero timestamp":    `{"version": 2, "width": 80, "height": 24, "timestamp": 0}`,
		"timestamp a float": `{"version": 2, "width": 80, "height": 24, "timestamp": 1.5}`,
		"after year 9999":   `{"version": 2, "width": 80, "height": 24, "timestamp": 253402300800}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := readCast(header)
			require.ErrorIs(t, err, ErrBadCast)
		})
	}
}

func TestCastEventIsRefused(t *testing.T) {
	t.Parallel()
	for name, events := range map[string][]string{
		"not JSON":          {`[0.1, "o", "x"`},
		"blank line":        {`[0.1, "o", "x"]`, ``, `[0.2, "o", "y"]`},
		"an object":         {`{"t": 0.1, "code": "o"}`},
		"two parts":         {`[0.1, "o"]`},
		"four parts":        {`[0.1, "o", "x", "y"]`},
		"time a string":     {`["0.1", "o", "x"]`},
		"null time":         {`[null, "o", "x"]`},
		"negative time":     {`[-0.1, "o", "x"]`},
		"time out of range": {`[1e10, "o", "x"]`},
		"time goes back":    {`[0.5, "o", "x"]`, `[0.25, "o", "y"]`},
		"unknown code":      {`[0.1, "x", "y"]`},
		"null code":         {`[0.1, null, "y"]`},
		"output not text":   {`[0.1, "o", 7]`},
		"input not text":    {`[0.1, "i", ["k"]]`},
		"marker not text":   {`[0.1, "m", null]`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := readCast(fixtureHeader() + "\n" + strings.Join(events, "\n"))
			require.ErrorIs(t, err, ErrBadCast)
			require.NotContains(t, err.Error(), `"x"`, "a reason, never the line")
		})
	}
}

func TestCastTimesMayRepeat(t *testing.T) {
	t.Parallel()
	evs, err := readCast(fixtureHeader() + "\n" + `[0, "o", "a"]` + "\n" + `[0, "o", "b"]` + "\n" + `[0.000000001, "o", "c"]`)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0, 1}, []int64{evs[0].at, evs[1].at, evs[2].at})
}

func TestCastLineIsBounded(t *testing.T) {
	t.Parallel()
	long := fmt.Sprintf(`[0.1, "o", "%s"]`, strings.Repeat("a", maxCastLineBytes))
	_, err := readCast(fixtureHeader() + "\n" + long)
	require.ErrorIs(t, err, ErrBadCast)
	require.Contains(t, err.Error(), "line 2: longer than")
	// A line and its newline within the bound read.
	fits := fmt.Sprintf(`[0.1, "o", "%s"]`, strings.Repeat("a", maxCastLineBytes-1-len(`[0.1, "o", ""]`)))
	_, err = readCast(fixtureHeader() + "\n" + fits)
	require.NoError(t, err)
}

func TestParseSize(t *testing.T) {
	t.Parallel()
	// Leading zeros still spell a decimal number.
	for in, want := range map[string][2]int{"100x30": {100, 30}, "1x1": {1, 1}, "10000x25": {10000, 25}, "0100x30": {100, 30}} {
		cols, rows, ok := parseSize(in)
		require.True(t, ok, in)
		require.Equal(t, want, [2]int{cols, rows}, in)
	}
	for _, in := range []string{"", "100", "x30", "100x", "0x30", "100x0", "-1x30", "+1x30", "1 x30", "1x30 ", "1x30x2", "10001x1", "1x10001", "999999x1", "1000x1000", "1e2x30"} {
		_, _, ok := parseSize(in)
		require.False(t, ok, in)
	}
}
