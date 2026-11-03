package tmuxattach

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/tmuxhelper"
)

const (
	helperBenchRevision = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	helperBenchBank     = "s02"
	helperBenchPane     = "%3"
)

// The helper request grammar is the closed seven-verb form measured on
// the helper branch: "<verb> <sha256-manifest> <bank> [pane]". Only the
// pane verbs carry a pane; anything else is a grammar error.
func TestHelperArgvVerbs(t *testing.T) {
	for verb, needsPane := range helperVerbs {
		pane := ""
		if needsPane {
			pane = helperBenchPane
		}
		argv, err := helperArgv(helperBenchRevision, helperBenchBank, verb, pane)
		require.NoError(t, err, "verb %s", verb)
		require.Equal(t, verb, argv[0])
		require.Equal(t, helperBenchRevision, argv[1])
		require.Equal(t, helperBenchBank, argv[2])
		if needsPane {
			require.Equal(t, []string{verb, helperBenchRevision, helperBenchBank, pane}, argv)
		} else {
			require.Len(t, argv, 3)
		}
	}
	require.Len(t, helperVerbs, 7, "closed seven-verb grammar")
}

func TestHelperArgvRejects(t *testing.T) {
	_, err := helperArgv(helperBenchRevision, helperBenchBank, "shell", helperBenchPane)
	require.Error(t, err)
	_, err = helperArgv("short", helperBenchBank, "capture", helperBenchPane)
	require.Error(t, err)
	_, err = helperArgv(helperBenchRevision, "S02!", "capture", helperBenchPane)
	require.Error(t, err)
	_, err = helperArgv(helperBenchRevision, helperBenchBank, "capture", "")
	require.Error(t, err)
	_, err = helperArgv(helperBenchRevision, helperBenchBank, "capture", "pane3")
	require.Error(t, err)
	_, err = helperArgv(helperBenchRevision, helperBenchBank, "panes", helperBenchPane)
	require.Error(t, err)
	_, err = helperArgv(helperBenchRevision, helperBenchBank, "panes", "")
	require.NoError(t, err)
	_, err = helperArgv(helperBenchRevision, helperBenchBank, "clients", "")
	require.NoError(t, err)
}

// A client without revision and bank refuses every call: that is the
// closed default for instances without a bank, before any execution.
func TestHelperClientClosedDefault(t *testing.T) {
	ctx := context.Background()
	_, _, err := helperClient{}.call(ctx, "panes", "")
	require.Error(t, err)
	_, _, err = helperClient{revision: helperBenchRevision}.call(ctx, "panes", "")
	require.Error(t, err)
	_, _, err = helperClient{bank: helperBenchBank}.call(ctx, "panes", "")
	require.Error(t, err)
	_, _, err = helperClient{revision: "short", bank: helperBenchBank}.call(ctx, "panes", "")
	require.Error(t, err)
	configured := helperClient{revision: helperBenchRevision, bank: helperBenchBank}
	_, _, err = configured.call(ctx, "shell", "")
	require.Error(t, err, "unknown verb fails before execution")
}

func helperFrameFor(code tmuxhelper.ExitCode, body string) string {
	raw := []byte(body)
	return "GTMX/1 " + itoa(int(code)) + " " + itoa(len(raw)) + "\n" +
		base64.StdEncoding.EncodeToString(raw) + "\n"
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := []byte{}
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}

// The GTMX/1 frame carries the helper exit code and the exact body
// length; truncated or malformed output never decodes.
func TestDecodeHelperFrame(t *testing.T) {
	for code := tmuxhelper.ExitOK; code <= tmuxhelper.ExitBinding; code++ {
		got, body, err := decodeHelperFrame([]byte(helperFrameFor(code, "a\tb")))
		require.NoError(t, err, "code %d", code)
		require.Equal(t, code, got)
		require.Equal(t, []byte("a\tb"), body)
	}
	got, body, err := decodeHelperFrame([]byte(helperFrameFor(tmuxhelper.ExitOK, "")))
	require.NoError(t, err)
	require.Equal(t, tmuxhelper.ExitOK, got)
	require.Empty(t, body)
}

func TestDecodeHelperFrameRejects(t *testing.T) {
	_, _, err := decodeHelperFrame(nil)
	require.Error(t, err)
	_, _, err = decodeHelperFrame([]byte("GTMX/1 0 3\n"))
	require.Error(t, err, "missing body line")
	full := helperFrameFor(tmuxhelper.ExitOK, "abc")
	_, _, err = decodeHelperFrame([]byte(full[:len(full)-2]))
	require.Error(t, err, "truncated")
	_, _, err = decodeHelperFrame([]byte("GTMX/2 0 0\n\n"))
	require.Error(t, err, "version")
	_, _, err = decodeHelperFrame([]byte("GTMX/1 9 0\n\n"))
	require.Error(t, err, "code range")
	_, _, err = decodeHelperFrame([]byte("GTMX/1 x 0\n\n"))
	require.Error(t, err, "code shape")
	_, _, err = decodeHelperFrame([]byte("GTMX/1 0 3\n!!!\n"))
	require.Error(t, err, "base64")
	_, _, err = decodeHelperFrame([]byte("GTMX/1 0 4\n" + base64.StdEncoding.EncodeToString([]byte("abc")) + "\n"))
	require.Error(t, err, "size mismatch")
	_, _, err = decodeHelperFrame([]byte("GTMX/1 0 0\n\ntrailing\n"))
	require.Error(t, err, "trailing data")
}
