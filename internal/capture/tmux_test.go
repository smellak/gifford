package capture

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testNonce = "0123456789abcdef0123456789abcdef"

var _ tmuxRunner = (*runner)(nil)

const (
	testSocket  = "/tmp/tmux.sock"
	tmuxDisplay = "display-message"
)

// fakeExec scripts tmux answers and records argv.
type fakeExec struct {
	calls []execCall
	fn    func(argv []string) execResult
}

type execCall struct {
	argv     []string
	keepTMUX bool
	cap      int
}

func (f *fakeExec) run(_ context.Context, argv []string, keepTMUX bool, stdoutCap int) execResult {
	f.calls = append(f.calls, execCall{argv: append([]string(nil), argv...), keepTMUX: keepTMUX, cap: stdoutCap})
	return f.fn(argv)
}

func okOut(s string) execResult {
	return execResult{stdout: []byte(s)}
}

func testServer() serverFacts {
	return serverFacts{pid: 1234, startTime: 1793231354, socket: testSocket}
}

func testSlot() paneSlot {
	return paneSlot{sessionID: "$0", windowIndex: 3, paneIndex: 0}
}

func testReq() openRequest {
	return openRequest{
		target:      "%0",
		mode:        modeNormal,
		server:      testServer(),
		slot:        testSlot(),
		nonce:       testNonce,
		argv:        []string{"/bin/gifford", "capture", "ingest", "--session", "cap-AAAA", "--start-sequence", "2", "--attempt", testNonce},
		snapshotMax: 67108864,
	}
}

func TestRunnerArgv(t *testing.T) {
	t.Parallel()
	newTestRunner := func(fn func([]string) execResult) (*runner, *fakeExec) {
		fx := &fakeExec{fn: fn}
		r := newRunner(testSocket)
		r.exec = fx.run
		return r, fx
	}

	t.Run("server facts", func(t *testing.T) {
		t.Parallel()
		r, fx := newTestRunner(func([]string) execResult {
			return okOut("1234\t1793231354\t/tmp/tmux.sock\n")
		})
		got, err := r.server(context.Background())
		require.NoError(t, err)
		require.Equal(t, testServer(), got)
		require.Equal(t, []string{"-S", testSocket, tmuxDisplay, "-p", "#{pid}\t#{start_time}\t#{socket_path}"}, fx.calls[0].argv)
		require.False(t, fx.calls[0].keepTMUX)
	})

	t.Run("pane and panes", func(t *testing.T) {
		t.Parallel()
		r, fx := newTestRunner(func([]string) execResult {
			return okOut("%0\t$0\tshell\t3\t0\t100\t24\t0\t0\n")
		})
		got, err := r.pane(context.Background(), "%0")
		require.NoError(t, err)
		require.Equal(t, "%0", got.id)
		require.Equal(t, []string{"-S", testSocket, tmuxDisplay, "-p", "-t", "%0", paneFormat}, fx.calls[0].argv)
		rows, err := r.panes(context.Background())
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, []string{"-S", testSocket, "list-panes", "-a", "-F", paneFormat}, fx.calls[1].argv)
	})

	t.Run("discovery keeps tmux and skips -S", func(t *testing.T) {
		t.Parallel()
		r, fx := newTestRunner(func([]string) execResult { return okOut("/tmp/other.sock\n") })
		sock, err := r.discoverSocket(context.Background())
		require.NoError(t, err)
		require.Equal(t, "/tmp/other.sock", sock)
		require.Equal(t, []string{tmuxDisplay, "-p", "#{socket_path}"}, fx.calls[0].argv)
		require.True(t, fx.calls[0].keepTMUX)
	})

	t.Run("open argv and pipe word round trip", func(t *testing.T) {
		t.Parallel()
		req := testReq()
		r, fx := newTestRunner(func([]string) execResult {
			return okOut("gifford-pre-" + testNonce + " 0\ngifford-opened-" + testNonce + "\nrest")
		})
		out, err := r.open(context.Background(), req)
		require.NoError(t, err)
		require.True(t, out.opened)
		argv := fx.calls[0].argv
		require.Equal(t, "-S", argv[0])
		require.Equal(t, []string{"if-shell", "-F", "-t", "%0"}, argv[2:6])
		require.NotContains(t, argv, "-b")
		branch := argv[6+1]
		got := decodePipeWord(t, branch)
		require.Equal(t, req.argv, got)
		foundPipe := false
		for _, w := range argv {
			require.NotEqual(t, "-I", w)
			require.NotEqual(t, "-o", w)
			if strings.Contains(w, "pipe-pane -O") {
				foundPipe = true
			}
		}
		require.True(t, foundPipe)
	})
}

// decodePipeWord inverts the open branch's streamed command: tmux
// unquote, halve '#' and '%', then shell unquote.
func decodePipeWord(t *testing.T, branch string) []string {
	t.Helper()
	i := strings.Index(branch, "pipe-pane -O -t")
	require.NotEqual(t, -1, i)
	word := strings.TrimSpace(branch[i+len("pipe-pane -O -t"):])
	end := strings.Index(word, " ")
	target, rest := word, ""
	if end >= 0 {
		target, rest = word[:end], strings.TrimSpace(word[end+1:])
	}
	// The open target goes bare: pipe-pane -O and capture-pane
	// reject the angle-bracket form against a real server.
	require.Regexp(t, `^%[0-9]+$`, target)
	unquoted := tmuxUnquote(t, quotedWord(t, rest))
	halved := strings.NewReplacer("##", "#", "%%", "%").Replace(unquoted)
	return shellSplit(t, halved)
}

// quotedWord extracts the first tmux-quoted word of s, honouring the
// quoted-quote escape the builders emit.
func quotedWord(t *testing.T, s string) string {
	t.Helper()
	require.True(t, len(s) > 0 && s[0] == '\'')
	i := 1
	for i < len(s) {
		if s[i] == '\'' {
			if len(s[i:]) >= 4 && s[i+1] == '\\' && s[i+2] == '\'' && s[i+3] == '\'' {
				i += 4
				continue
			}
			return s[:i+1]
		}
		i++
	}
	require.Fail(t, "unterminated quoted word")
	return ""
}

func tmuxUnquote(t *testing.T, s string) string {
	t.Helper()
	require.True(t, len(s) >= 2 && strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'"))
	return strings.ReplaceAll(s[1:len(s)-1], "'\\''", "'")
}

func shellSplit(t *testing.T, cmd string) []string {
	t.Helper()
	require.True(t, strings.HasPrefix(cmd, "exec "))
	rest := cmd[len("exec "):]
	var out []string
	for len(rest) > 0 {
		require.Equal(t, byte('\''), rest[0])
		var sb strings.Builder
		i := 1
		for {
			require.Less(t, i, len(rest))
			if rest[i] == '\'' {
				if i+3 < len(rest) && rest[i+1] == '\\' && rest[i+2] == '\'' && rest[i+3] == '\'' {
					sb.WriteByte('\'')
					i += 4
					continue
				}
				i++
				break
			}
			sb.WriteByte(rest[i])
			i++
		}
		out = append(out, sb.String())
		rest = strings.TrimPrefix(rest[i:], " ")
	}
	return out
}

func TestGuardBuilder(t *testing.T) {
	t.Parallel()
	got := guard(testServer(), testSlot(), modeNormal)
	require.Equal(t, "#{&&:#{==:#{pid},1234},#{&&:#{==:#{start_time},1793231354},#{&&:#{==:#{session_id},$0},#{&&:#{==:#{window_index},3},#{&&:#{==:#{pane_index},0},#{&&:#{==:#{pane_pipe},0},#{==:#{pane_dead},0}}}}}}}", got)
	gotReplace := guard(testServer(), testSlot(), modeReplace)
	require.Contains(t, gotReplace, "#{==:#{pane_dead},0}")
	require.NotContains(t, gotReplace, "pane_pipe")

	t.Run("bad nonce refused before any call", func(t *testing.T) {
		t.Parallel()
		fx := &fakeExec{fn: func([]string) execResult { return okOut("") }}
		r := &runner{socket: testSocket, exec: fx.run}
		req := testReq()
		req.nonce = "gifford-pre-#,}-0123456789abcdef"
		_, err := r.open(context.Background(), req)
		require.ErrorIs(t, err, ErrUsage)
		require.Empty(t, fx.calls)
	})
}

func TestPipeCommandQuoting(t *testing.T) {
	t.Parallel()
	words := []string{
		"a", "b c", "'q'", "qu'ote", `back\slash`, "$HOME", "${x}", "~user",
		"#", "##", "%", "%H", "%%", ";", "a;", "cmd;arg", "{a,b}", "`tick`",
		"$(sub)", "ünïcödé", "", " lead", "trail ", "  both  ", "-O", "-I",
		"-o", "-F", "display", "if-shell", ";", "'",
	}
	require.Len(t, words, 32)
	rendered := pipeCommand(words)
	quoted := tmuxQuote(rendered)
	back := shellSplit(t, strings.NewReplacer("##", "#", "%%", "%").Replace(tmuxUnquote(t, quoted)))
	require.Equal(t, words, back)

	for _, w := range []string{"a\x00b", "a\nb"} {
		req := testReq()
		req.argv = []string{"gifford", w}
		fx := &fakeExec{fn: func([]string) execResult { return okOut("") }}
		r := &runner{socket: testSocket, exec: fx.run}
		_, err := r.open(context.Background(), req)
		require.Error(t, err)
		require.Empty(t, fx.calls)
	}
}

func TestOpenOutcome(t *testing.T) {
	t.Parallel()
	newOutcomeRunner := func(out execResult) *runner {
		fx := &fakeExec{fn: func([]string) execResult { return out }}
		r := newRunner(testSocket)
		r.exec = fx.run
		return r
	}
	run := func(t *testing.T, out execResult) openOutcome {
		t.Helper()
		r := newOutcomeRunner(out)
		got, err := r.open(context.Background(), testReq())
		require.NoError(t, err)
		return got
	}

	t.Run("opened pre 0 and 1", func(t *testing.T) {
		t.Parallel()
		for _, pre := range []string{"0", "1"} {
			got := run(t, okOut("gifford-pre-"+testNonce+" "+pre+"\ngifford-opened-"+testNonce+"\n\x1b[0mrest\ntail"))
			require.True(t, got.opened)
			require.Equal(t, pre == "1", got.prePipe)
			require.Equal(t, "\x1b[0mrest\ntail", string(got.snapshot))
		}
	})

	t.Run("refusals", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			fields string
			reason refuseReason
		}{
			{"0 0 4321 1793231354 $0 3 0", refuseServerReplaced},
			{"0 0 1234 999 $0 3 0", refuseServerReplaced},
			{"0 0 1234 1793231354 $9 3 0", refusePaneMoved},
			{"0 0 1234 1793231354 $0 4 0", refusePaneMoved},
			{"0 1 1234 1793231354 $0 3 0", refuseDead},
			{"1 0 1234 1793231354 $0 3 0", refusePipePresent},
		}
		for _, tc := range cases {
			got := run(t, okOut("gifford-refused-"+testNonce+" "+tc.fields+"\n"))
			require.True(t, got.refused, tc.fields)
			require.Equal(t, tc.reason, got.reason, tc.fields)
		}
	})

	t.Run("nothing opened", func(t *testing.T) {
		t.Parallel()
		for _, stderr := range []string{"can't find pane: %9", "target pane has exited"} {
			r := newOutcomeRunner(execResult{err: errors.New("exit status 1"), stderr: []byte(stderr)})
			got, err := r.open(context.Background(), testReq())
			require.NoError(t, err)
			require.True(t, got.nothing)
		}
		withPre := execResult{err: errors.New("exit status 1"), stderr: []byte("can't find pane"), stdout: []byte("gifford-pre-" + testNonce + " 0\n")}
		r := newOutcomeRunner(withPre)
		got, err := r.open(context.Background(), testReq())
		require.NoError(t, err)
		require.True(t, got.nothing)
	})

	t.Run("unknown", func(t *testing.T) {
		t.Parallel()
		r := newOutcomeRunner(execResult{err: context.DeadlineExceeded})
		_, err := r.open(context.Background(), testReq())
		require.ErrorIs(t, err, context.DeadlineExceeded)

		for _, out := range []string{
			"gifford-pre-" + testNonce + " 0\n",
			"gifford-opened-x 0\ngifford-opened-" + testNonce + "\n",
			"gifford-refused-" + testNonce + " 0 0\n",
			"gifford-refused-other 0 0 1 2 $0 3 0\n",
			"l1\nl2\nl3\n",
		} {
			got := run(t, okOut(out))
			require.Equal(t, openOutcome{}, got, "%q", out)
		}
	})

	t.Run("unreachable is distinct", func(t *testing.T) {
		t.Parallel()
		r := newOutcomeRunner(execResult{err: errors.New("exit status 1"), stderr: []byte("no server running on /tmp/x")})
		_, err := r.pane(context.Background(), "%0")
		require.ErrorIs(t, err, errUnreachable)
	})
}

func TestPaneFactsValidation(t *testing.T) {
	t.Parallel()
	good, err := parsePaneFacts("%0\t$0\tshell\t3\t0\t100\t24\t0\t0", "%0")
	require.NoError(t, err)
	require.Equal(t, paneFacts{id: "%0", sessionID: "$0", sessionName: "shell", windowIndex: 3, paneIndex: 0, cols: 100, rows: 24}, good)
	for _, tc := range []struct {
		name   string
		line   string
		target string
	}{
		{"empty", "", "%0"},
		{"fields", "a\tb", "%0"},
		{"id", "x\t$0\ts\t0\t0\t10\t10\t0\t0", "%0"},
		{"other pane", "%1\t$0\ts\t0\t0\t10\t10\t0\t0", "%0"},
		{"session", "%0\t0\ts\t0\t0\t10\t10\t0\t0", ""},
		{"name", "%0\t$0\t\t0\t0\t10\t10\t0\t0", ""},
		{"index", "%0\t$0\ts\t-1\t0\t10\t10\t0\t0", ""},
		{"size", "%0\t$0\ts\t0\t0\t0\t10\t0\t0", ""},
		{"pipe", "%0\t$0\ts\t0\t0\t10\t10\t2\t0", ""},
		{"word", "%0\t$0\ts\t0\t0\t10\t10\t0\tx", ""},
	} {
		_, err := parsePaneFacts(tc.line, tc.target)
		require.ErrorIs(t, err, ErrPaneNotFound, tc.name)
	}
}

func TestServerFactsValidation(t *testing.T) {
	t.Parallel()
	got, err := parseServerFacts("1234\t1793231354\t/tmp/tmux.sock\n", testSocket)
	require.NoError(t, err)
	require.Equal(t, testServer(), got)
	for _, tc := range []struct {
		name string
		line string
	}{
		{"empty", ""},
		{"fields", "1\t2"},
		{"pid", "1\t1793231354\t/tmp/tmux.sock"},
		{"start", "1234\t0\t/tmp/tmux.sock"},
		{"socket", "1234\t1793231354\t/tmp/other.sock"},
	} {
		_, err := parseServerFacts(tc.line, testSocket)
		require.Error(t, err, tc.name)
	}
}
