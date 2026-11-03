package tmuxhelper

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeFrameRoundTripsEveryExitCode(t *testing.T) {
	t.Parallel()

	for code := ExitOK; code <= ExitBinding; code++ {
		t.Run(codeName(code), func(t *testing.T) {
			t.Parallel()
			var want []byte
			if code == ExitOK {
				want = []byte{0, 1, 2, 0xff}
			}
			var frame bytes.Buffer
			require.NoError(t, writeFrame(&frame, code, want))
			gotCode, got, err := DecodeFrame(frame.Bytes())
			require.NoError(t, err)
			require.Equal(t, code, gotCode)
			require.Equal(t, want, got)
		})
	}
}

func TestMaxFrameBytesIsExactAndDecodable(t *testing.T) {
	t.Parallel()

	want := bytes.Repeat([]byte{0xff}, maxCaptureBytes)
	var frame bytes.Buffer
	require.NoError(t, writeFrame(&frame, ExitOK, want))
	require.Len(t, frame.Bytes(), MaxFrameBytes)
	code, got, err := DecodeFrame(frame.Bytes())
	require.NoError(t, err)
	require.Equal(t, ExitOK, code)
	require.Equal(t, want, got)

	require.Error(t, writeFrame(&bytes.Buffer{}, ExitOK, append(want, 0)))
}

func TestDecodeFrameAcceptsEmptySuccess(t *testing.T) {
	t.Parallel()

	code, body, err := DecodeFrame([]byte("GTMX/1 0 0\n\n"))
	require.NoError(t, err)
	require.Equal(t, ExitOK, code)
	require.Nil(t, body)
}

func TestDecodeFrameRejectsEveryNonCanonicalBoundary(t *testing.T) {
	t.Parallel()

	bad := map[string][]byte{
		"empty":                    nil,
		"frame above bound":        bytes.Repeat([]byte{'x'}, MaxFrameBytes+1),
		"leading byte":             []byte("xGTMX/1 0 0\n\n"),
		"wrong version":            []byte("GTMX/2 0 0\n\n"),
		"extra header space":       []byte("GTMX/1  0 0\n\n"),
		"leading zero code":        []byte("GTMX/1 00 0\n\n"),
		"negative code":            []byte("GTMX/1 -1 0\n\n"),
		"unknown code":             []byte("GTMX/1 8 0\n\n"),
		"leading zero length":      []byte("GTMX/1 0 01\nAA==\n"),
		"length above body bound":  []byte("GTMX/1 0 1048577\n\n"),
		"encoded length mismatch":  []byte("GTMX/1 0 1\n\n"),
		"decoded length mismatch":  []byte("GTMX/1 0 2\nAA==\n"),
		"invalid base64":           []byte("GTMX/1 0 1\n!!!!\n"),
		"noncanonical base64 bits": []byte("GTMX/1 0 1\nAB==\n"),
		"body on failure":          []byte("GTMX/1 7 1\nAA==\n"),
		"missing final newline":    []byte("GTMX/1 0 0\n"),
		"extra line":               []byte("GTMX/1 0 0\n\n\n"),
		"trailing bytes":           []byte("GTMX/1 0 0\n\nx"),
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			code, body, err := DecodeFrame(raw)
			require.ErrorIs(t, err, ErrInvalidFrame)
			require.Equal(t, ExitEnvironment, code)
			require.Empty(t, body)
		})
	}
}

func TestDecodeFrameReturnsIndependentBody(t *testing.T) {
	t.Parallel()

	raw := []byte("GTMX/1 0 1\nAA==\n")
	_, body, err := DecodeFrame(raw)
	require.NoError(t, err)
	body[0] = 1
	require.Equal(t, []byte("GTMX/1 0 1\nAA==\n"), raw)
}

func TestInvalidFrameSentinelDoesNotContainInput(t *testing.T) {
	t.Parallel()

	_, _, err := DecodeFrame([]byte("private pane bytes"))
	require.ErrorIs(t, err, ErrInvalidFrame)
	require.Equal(t, "gifford tmux: invalid frame", err.Error())
}

func codeName(code ExitCode) string {
	return []string{"ok", "tmux", "grammar", "environment", "timeout", "output-limit", "revision", "binding"}[code]
}

func FuzzDecodeFrameCanonical(f *testing.F) {
	f.Add([]byte("GTMX/1 0 0\n\n"))
	f.Add([]byte("GTMX/1 0 1\nAA==\n"))
	f.Add([]byte("GTMX/1 7 0\n\n"))
	f.Add([]byte("not a frame"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		code, body, err := DecodeFrame(raw)
		if err != nil {
			require.ErrorIs(t, err, ErrInvalidFrame)
			return
		}
		var canonical bytes.Buffer
		require.NoError(t, writeFrame(&canonical, code, body))
		require.Equal(t, raw, canonical.Bytes())
	})
}
