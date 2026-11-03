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

// A client without revision refuses every call, and a server without a
// bank refuses every call: that is the closed default for instances
// without a bank, before any execution.
func TestHelperClientClosedDefault(t *testing.T) {
	ctx := context.Background()
	plain := Server{OwnerUser: "owner", OwnerUID: 1000, Socket: "/tmp/tmux-1000/default"}
	_, _, err := helperClient{}.call(ctx, plain, "panes", "")
	require.Error(t, err, "no revision")
	_, _, err = helperClient{revision: "short"}.call(ctx, plain, "panes", "")
	require.Error(t, err, "bad revision")
	_, _, err = helperClient{revision: helperBenchRevision}.call(ctx, plain, "panes", "")
	require.Error(t, err, "no bank")
	banked := Server{OwnerUser: "owner", OwnerUID: 1000, Socket: "/tmp/tmux-1000/default", Bank: helperBenchBank}
	configured := helperClient{revision: helperBenchRevision}
	_, _, err = configured.call(ctx, banked, "shell", "")
	require.Error(t, err, "unknown verb fails before execution")
	_, _, err = configured.call(ctx, banked, "capture", "")
	require.Error(t, err, "pane verb without pane fails before execution")
}

func TestJoinHelperResult(t *testing.T) {
	okFrame := []byte(helperFrameFor(tmuxhelper.ExitOK, "a\tb"))
	code, body, err := joinHelperResult(0, okFrame)
	require.NoError(t, err)
	require.Equal(t, tmuxhelper.ExitOK, code)
	require.Equal(t, []byte("a\tb"), body)
	timeoutFrame := []byte(helperFrameFor(tmuxhelper.ExitTimeout, ""))
	code, _, err = joinHelperResult(4, timeoutFrame)
	require.NoError(t, err)
	require.Equal(t, tmuxhelper.ExitTimeout, code)
	_, _, err = joinHelperResult(3, timeoutFrame)
	require.Error(t, err, "exit/frame mismatch")
	_, _, err = joinHelperResult(4, okFrame)
	require.Error(t, err, "frame code must equal the exit")
	_, _, err = joinHelperResult(0, okFrame[:len(okFrame)-2])
	require.Error(t, err, "truncated")
	_, _, err = joinHelperResult(0, []byte("GTMX/1 9 0\n\n"))
	require.Error(t, err, "code range")
	_, _, err = joinHelperResult(0, nil)
	require.Error(t, err, "empty")
	_, _, err = joinHelperResult(0, make([]byte, tmuxhelper.MaxFrameBytes+1))
	require.Error(t, err, "oversize")
	require.Equal(t, 1398122, tmuxhelper.MaxFrameBytes, "frozen bound owned by tmuxhelper: 1MiB body to 1398104 b64 plus header")
	exact := []byte("GTMX/1 0 3\n" + base64.StdEncoding.EncodeToString([]byte("abc")) + "\n")
	_, _, err = joinHelperResult(0, exact)
	require.NoError(t, err, "exact frame decodes")
	_, _, err = joinHelperResult(4, []byte(helperFrameFor(tmuxhelper.ExitTimeout, "x")))
	require.Error(t, err, "non-OK frame must carry an empty body")
	proc, ok := exitCodeOf(nil)
	require.True(t, ok)
	require.Equal(t, 0, proc)
}

func TestCappedStdout(t *testing.T) {
	var small cappedStdout
	written, err := small.Write([]byte("GTMX/1 0 0\n\n"))
	require.NoError(t, err)
	require.Equal(t, len("GTMX/1 0 0\n\n"), written)
	require.False(t, small.overflow)

	var exact cappedStdout
	huge := make([]byte, tmuxhelper.MaxFrameBytes+100)
	for i := range huge {
		huge[i] = 'A'
	}
	written, err = exact.Write(huge)
	require.NoError(t, err)
	require.Equal(t, len(huge), written, "dropped bytes still report consumed")
	require.True(t, exact.overflow)
	require.Equal(t, tmuxhelper.MaxFrameBytes+1, exact.buf.Len())

	var split cappedStdout
	_, _ = split.Write(huge[:tmuxhelper.MaxFrameBytes])
	require.False(t, split.overflow)
	_, _ = split.Write([]byte("AB"))
	require.True(t, split.overflow, "one byte past the bound trips overflow")
	require.Equal(t, tmuxhelper.MaxFrameBytes+1, split.buf.Len())
}

func TestHelperErrorMapping(t *testing.T) {
	require.NoError(t, helperError(tmuxhelper.ExitOK))
	require.Equal(t, ErrCommandTimeout, helperError(tmuxhelper.ExitTimeout))
	require.Equal(t, ErrCaptureTooLarge, helperError(tmuxhelper.ExitOutputLimit))
	for _, code := range []tmuxhelper.ExitCode{
		tmuxhelper.ExitTmux, tmuxhelper.ExitGrammar, tmuxhelper.ExitEnvironment,
		tmuxhelper.ExitRevision, tmuxhelper.ExitBinding,
	} {
		require.Equal(t, ErrCommand, helperError(code), "code %d", code)
	}
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

// The node decodes with the owner decoder: canonical frames only, an
// empty body for every non-OK code, and ErrInvalidFrame for the rest.
// The owner suite owns the decoder matrix; here we pin the contract
// points the transport relies on.
func TestDecodeHelperFrame(t *testing.T) {
	got, body, err := tmuxhelper.DecodeFrame([]byte(helperFrameFor(tmuxhelper.ExitOK, "a\tb")))
	require.NoError(t, err)
	require.Equal(t, tmuxhelper.ExitOK, got)
	require.Equal(t, []byte("a\tb"), body)
	for code := tmuxhelper.ExitTmux; code <= tmuxhelper.ExitBinding; code++ {
		got, body, err := tmuxhelper.DecodeFrame([]byte(helperFrameFor(code, "")))
		require.NoError(t, err, "code %d", code)
		require.Equal(t, code, got)
		require.Empty(t, body)
	}
}

func TestDecodeHelperFrameRejects(t *testing.T) {
	cases := map[string]string{
		"missing body line": "GTMX/1 0 3\n",
		"version":           "GTMX/2 0 0\n\n",
		"code range":        "GTMX/1 9 0\n\n",
		"code shape":        "GTMX/1 x 0\n\n",
		"non-canonical num": "GTMX/1 00 0\n\n",
		"base64":            "GTMX/1 0 3\n!!!\n",
		"size mismatch":     "GTMX/1 0 4\n" + base64.StdEncoding.EncodeToString([]byte("abc")) + "\n",
		"trailing data":     "GTMX/1 0 0\n\ntrailing\n",
		"body on error":     helperFrameFor(tmuxhelper.ExitTimeout, "x"),
	}
	for name, raw := range cases {
		_, _, err := tmuxhelper.DecodeFrame([]byte(raw))
		require.ErrorIs(t, err, tmuxhelper.ErrInvalidFrame, name)
	}
	full := helperFrameFor(tmuxhelper.ExitOK, "abc")
	_, _, err := tmuxhelper.DecodeFrame([]byte(full[:len(full)-2]))
	require.ErrorIs(t, err, tmuxhelper.ErrInvalidFrame, "truncated")
	_, _, err = tmuxhelper.DecodeFrame(nil)
	require.ErrorIs(t, err, tmuxhelper.ErrInvalidFrame, "empty")
	_, _, err = joinHelperResult(0, []byte("GTMX/1 x 0\n\n"))
	require.ErrorIs(t, err, tmuxhelper.ErrInvalidFrame, "transport surfaces the owner error")
}
