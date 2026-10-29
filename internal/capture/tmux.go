package capture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// tmuxTimeout bounds every tmux client call. Expiry ends that client,
// a child of this package, through the call context.
const tmuxTimeout = 5 * time.Second

// factsCap bounds fact answers; errCap bounds tmux stderr.
const factsCap = 64 * 1024

const errCap = 4 * 1024

// errUnreachable reports a tmux call that failed because its server is
// gone. It is distinct from ErrPaneNotFound; callers map it to
// ErrTmuxCommand.
var errUnreachable = errors.New("tmux server unreachable")

// serverFacts identifies a tmux server instance and its socket.
type serverFacts struct {
	pid       int
	startTime int64
	socket    string
}

// paneFacts reports one pane: its id, slot, size and pipe state.
type paneFacts struct {
	id          string
	sessionID   string
	sessionName string
	windowIndex int
	paneIndex   int
	cols        int
	rows        int
	pipe        bool
	dead        bool
}

// openMode selects the guard's pipe condition: normal refuses a present
// pipe, replace allows it.
type openMode int

const (
	modeNormal openMode = iota
	modeReplace
)

// openRequest describes one guarded conditional open: the expected
// server and slot, the attempt nonce, the ingest argv that becomes the
// pipe command, and the snapshot byte bound from the validated limits.
type openRequest struct {
	target      string
	mode        openMode
	server      serverFacts
	slot        paneSlot
	nonce       string
	argv        []string
	snapshotMax int64
}

// paneSlot is the logical pane slot a guard pins.
type paneSlot struct {
	sessionID   string
	windowIndex int
	paneIndex   int
}

// refuseReason classifies a refusal at the decision instant.
type refuseReason int

const (
	refuseNone refuseReason = iota
	refuseServerReplaced
	refusePaneMoved
	refuseDead
	refusePipePresent
)

// openOutcome is one of the §5.4 outcomes, decided from the output alone.
type openOutcome struct {
	opened  bool
	refused bool
	reason  refuseReason
	prePipe bool
	// snapshot holds the open branch's capture-pane bytes on opened.
	snapshot []byte
	// nothing is set when tmux ran nothing after a missing pane.
	nothing bool
}

// tmuxRunner reaches tmux through typed methods only. No method takes a
// free argument list.
type tmuxRunner interface {
	server(ctx context.Context) (serverFacts, error)
	pane(ctx context.Context, target string) (paneFacts, error)
	panes(ctx context.Context) ([]paneFacts, error)
	open(ctx context.Context, req openRequest) (openOutcome, error)
}

// execResult carries one tmux call's bounded output.
type execResult struct {
	stdout    []byte
	stderr    []byte
	truncated bool
	err       error
}

// execFunc runs tmux with argv under ctx: stdin nil, stdout capped at
// stdoutCap, stderr capped at errCap. The environment is the caller's
// without TMUX unless keepTMUX.
type execFunc func(ctx context.Context, argv []string, keepTMUX bool, stdoutCap int) execResult

// runner is the real tmuxRunner over one socket.
type runner struct {
	socket string
	exec   execFunc
}

// newRunner builds the runner over socket with the default executor.
func newRunner(socket string) *runner {
	return &runner{socket: socket, exec: defaultExec}
}

// defaultExec runs the tmux client with no shell.
func defaultExec(ctx context.Context, argv []string, keepTMUX bool, stdoutCap int) execResult {
	cmd := exec.CommandContext(ctx, "tmux", argv...)
	cmd.Stdin = nil
	if !keepTMUX {
		env := os.Environ()
		out := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, "TMUX=") {
				out = append(out, kv)
			}
		}
		cmd.Env = out
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &stdout, cap: stdoutCap}
	cmd.Stderr = &limitedWriter{w: &stderr, cap: errCap}
	err := cmd.Run()
	return execResult{stdout: stdout.Bytes(), stderr: stderr.Bytes(), truncated: stdout.Len() >= stdoutCap, err: err}
}

// limitedWriter caps what it keeps; excess is dropped but reported.
type limitedWriter struct {
	w   *bytes.Buffer
	cap int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	room := l.cap - l.w.Len()
	if room <= 0 {
		return len(p), nil
	}
	if len(p) > room {
		p = p[:room]
	}
	n, err := l.w.Write(p)
	if err != nil {
		return n, fmt.Errorf("tmux output: %w", err)
	}
	return n, nil
}

// withTimeout bounds one call.
func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, tmuxTimeout)
}

// call runs one -S call and maps transport failures.
func (r *runner) call(ctx context.Context, stdoutCap int, args ...string) execResult {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	argv := append([]string{"-S", r.socket}, args...)
	res := r.exec(ctx, argv, false, stdoutCap)
	if res.err != nil && isUnreachable(res.stderr) {
		res.err = fmt.Errorf("%w: %s", errUnreachable, firstLine(res.stderr))
	}
	return res
}

var (
	tmuxPanePattern    = regexp.MustCompile(`^%[0-9]{1,9}$`)
	tmuxSessionPattern = regexp.MustCompile(`^\$[0-9]{1,9}$`)
	hexNoncePattern    = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// server reads the server facts.
func (r *runner) server(ctx context.Context) (serverFacts, error) {
	res := r.call(ctx, factsCap, "display-message", "-p", "#{pid}\t#{start_time}\t#{socket_path}")
	if res.err != nil {
		return serverFacts{}, res.err
	}
	return parseServerFacts(string(res.stdout), r.socket)
}

// parseServerFacts validates one server-format line.
func parseServerFacts(out, socket string) (serverFacts, error) {
	line := firstLine([]byte(out))
	fields := strings.Split(line, "\t")
	if len(fields) != 3 {
		return serverFacts{}, fmt.Errorf("%w: malformed server facts", ErrTmuxCommand)
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid <= 1 {
		return serverFacts{}, fmt.Errorf("%w: bad server pid", ErrTmuxCommand)
	}
	start, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || start <= 0 {
		return serverFacts{}, fmt.Errorf("%w: bad server start time", ErrTmuxCommand)
	}
	if fields[2] != socket {
		return serverFacts{}, fmt.Errorf("%w: server socket mismatch", ErrTmuxCommand)
	}
	return serverFacts{pid: pid, startTime: start, socket: socket}, nil
}

// pane reads one pane's facts.
func (r *runner) pane(ctx context.Context, target string) (paneFacts, error) {
	if badTarget(target) {
		return paneFacts{}, fmt.Errorf("%w: bad pane target", ErrUsage)
	}
	res := r.call(ctx, factsCap, "display-message", "-p", "-t", target, paneFormat)
	if res.err != nil {
		return paneFacts{}, res.err
	}
	return parsePaneFacts(firstLine(res.stdout), target)
}

// paneFormat is the 9-field tab-separated pane format.
const paneFormat = "#{pane_id}\t#{session_id}\t#{session_name}\t#{window_index}\t#{pane_index}\t#{pane_width}\t#{pane_height}\t#{pane_pipe}\t#{pane_dead}"

// panes lists every pane on the server.
func (r *runner) panes(ctx context.Context) ([]paneFacts, error) {
	res := r.call(ctx, factsCap, "list-panes", "-a", "-F", paneFormat)
	if res.err != nil {
		return nil, res.err
	}
	var out []paneFacts
	for _, line := range strings.Split(string(res.stdout), "\n") {
		if line == "" {
			continue
		}
		pf, err := parsePaneFacts(line, "")
		if err != nil {
			return nil, err
		}
		out = append(out, pf)
	}
	return out, nil
}

// parsePaneFacts validates one pane-format line. An empty target skips
// the identity check, for list-panes rows.
func parsePaneFacts(line, target string) (paneFacts, error) {
	bad := func() (paneFacts, error) {
		return paneFacts{}, fmt.Errorf("%w: target not found", ErrPaneNotFound)
	}
	if line == "" {
		return bad()
	}
	f := strings.Split(line, "\t")
	if len(f) != 9 {
		return bad()
	}
	if !tmuxPanePattern.MatchString(f[0]) {
		return bad()
	}
	if target != "" && strings.HasPrefix(target, "%") && f[0] != target {
		return bad()
	}
	if !tmuxSessionPattern.MatchString(f[1]) {
		return bad()
	}
	if len(f[2]) < 1 || len(f[2]) > 128 || strings.IndexFunc(f[2], unicode.IsControl) >= 0 {
		return bad()
	}
	nums := make([]int, 6)
	for i, s := range f[3:9] {
		n, err := strconv.Atoi(s)
		if err != nil {
			return bad()
		}
		nums[i] = n
	}
	wi, pi, cols, rows, pipe, dead := nums[0], nums[1], nums[2], nums[3], nums[4], nums[5]
	if wi < 0 || wi > 999999 || pi < 0 || pi > 999999 {
		return bad()
	}
	if cols < 1 || cols > 10000 || rows < 1 || rows > 10000 {
		return bad()
	}
	if (pipe != 0 && pipe != 1) || (dead != 0 && dead != 1) {
		return bad()
	}
	return paneFacts{
		id: f[0], sessionID: f[1], sessionName: f[2],
		windowIndex: wi, paneIndex: pi, cols: cols, rows: rows,
		pipe: pipe == 1, dead: dead == 1,
	}, nil
}

// badTarget refuses NUL, newlines and control characters in a -t value.
func badTarget(target string) bool {
	return target == "" || strings.IndexFunc(target, func(r rune) bool {
		return r == 0 || r == '\n' || unicode.IsControl(r)
	}) >= 0
}

// discoverSocket runs the one call without -S, keeping TMUX.
func (r *runner) discoverSocket(ctx context.Context) (string, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	res := r.exec(ctx, []string{"display-message", "-p", "#{socket_path}"}, true, 256)
	if res.err != nil {
		return "", fmt.Errorf("pass --tmux-socket: %w", ErrUsage)
	}
	sock := string(bytes.TrimSpace(res.stdout))
	if !strings.HasPrefix(sock, "/") || len(sock) > 128 || strings.IndexFunc(sock, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("pass --tmux-socket: %w", ErrUsage)
	}
	return sock, nil
}

// guard builds the server-side conditional from validated values: the
// expected server pid and start time, and the slot. Every value is
// digits or $ plus digits, so none can carry ',', '}' or '#' into it.
func guard(server serverFacts, slot paneSlot, mode openMode) string {
	tail := "#{==:#{pane_dead},0}"
	if mode == modeNormal {
		tail = "#{&&:#{==:#{pane_pipe},0}," + tail + "}"
	}
	return "#{&&:#{==:#{pid}," + strconv.Itoa(server.pid) +
		"},#{&&:#{==:#{start_time}," + strconv.FormatInt(server.startTime, 10) +
		"},#{&&:#{==:#{session_id}," + slot.sessionID +
		"},#{&&:#{==:#{window_index}," + strconv.Itoa(slot.windowIndex) +
		"},#{&&:#{==:#{pane_index}," + strconv.Itoa(slot.paneIndex) +
		"}," + tail + "}}}}}"
}

// openBranch renders the open branch: a pre line, the pipe open, the
// opened token, and the snapshot. No branch string ends with ';'.
func openBranch(target, nonce string, argv []string) string {
	return "display-message -p 'gifford-pre-" + nonce + " #{pane_pipe}' ; " +
		"pipe-pane -O -t <" + target + "> " + tmuxQuote(pipeCommand(argv)) + " ; " +
		"display-message -p gifford-opened-" + nonce + " ; " +
		"capture-pane -p -e -S - -t <" + target + ">"
}

// refuseBranch renders the refusal branch: one token line with the
// decision-instant facts.
func refuseBranch(nonce string) string {
	return "display-message -p 'gifford-refused-" + nonce +
		" #{pane_pipe} #{pane_dead} #{pid} #{start_time} #{session_id} #{window_index} #{pane_index}'"
}

// open runs one guarded conditional open and classifies its output.
func (r *runner) open(ctx context.Context, req openRequest) (openOutcome, error) {
	if !hexNoncePattern.MatchString(req.nonce) {
		return openOutcome{}, fmt.Errorf("%w: bad attempt nonce", ErrUsage)
	}
	if badTarget(req.target) {
		return openOutcome{}, fmt.Errorf("%w: bad pane target", ErrUsage)
	}
	for _, a := range req.argv {
		if strings.ContainsAny(a, "\x00\n") {
			return openOutcome{}, fmt.Errorf("%w: bad ingest word", ErrUnsafePath)
		}
	}
	if req.snapshotMax < 65536 || req.snapshotMax > 268435456 {
		return openOutcome{}, fmt.Errorf("%w: bad snapshot bound", ErrUsage)
	}
	slot := req.slot
	res := r.call(ctx, int(req.snapshotMax)+4096, "if-shell", "-F", "-t", req.target,
		guard(req.server, slot, req.mode), openBranch(req.target, req.nonce, req.argv), refuseBranch(req.nonce))
	return classifyOpen(res, req)
}

// classifyOpen decides the outcome from the output alone. A zero outcome
// with no error is unknown: a pipe may be open and the caller takes its
// abort path.
func classifyOpen(res execResult, req openRequest) (openOutcome, error) {
	if res.truncated {
		return openOutcome{}, fmt.Errorf("%w: oversized tmux answer", ErrTmuxCommand)
	}
	if res.err != nil {
		if isMissingPane(res.stderr) {
			return openOutcome{nothing: true}, nil
		}
		return openOutcome{}, res.err
	}
	if out, ok := parseOpened(string(res.stdout), req); ok {
		return out, nil
	}
	if fields, ok := parseRefused(string(res.stdout), req.nonce); ok {
		return classifyRefusal(fields, req), nil
	}
	return openOutcome{}, nil
}

// parseOpened accepts the pre line, the opened token and the snapshot
// that follows them.
func parseOpened(out string, req openRequest) (openOutcome, bool) {
	pre := "gifford-pre-" + req.nonce
	opened := "gifford-opened-" + req.nonce
	i := strings.IndexByte(out, '\n')
	if i < 0 {
		return openOutcome{}, false
	}
	first, afterFirst := out[:i], out[i+1:]
	j := strings.IndexByte(afterFirst, '\n')
	if j < 0 {
		return openOutcome{}, false
	}
	second, afterSecond := afterFirst[:j], afterFirst[j+1:]
	if second != opened {
		return openOutcome{}, false
	}
	v, ok := parsePreLine(first, pre)
	if !ok {
		return openOutcome{}, false
	}
	return openOutcome{opened: true, prePipe: v == 1, snapshot: []byte(afterSecond)}, true
}

// parsePreLine accepts `gifford-pre-<nonce> <0|1>`.
func parsePreLine(line, pre string) (int, bool) {
	rest, ok := strings.CutPrefix(line, pre+" ")
	if !ok || (rest != "0" && rest != "1") {
		return 0, false
	}
	if rest == "1" {
		return 1, true
	}
	return 0, true
}

// parseRefused accepts exactly one `gifford-refused-<nonce>` line with 7
// valid fields: pipe dead pid start session window pane.
func parseRefused(out, nonce string) ([7]string, bool) {
	var zero [7]string
	lines := strings.Split(out, "\n")
	if len(lines) == 2 && lines[1] == "" {
		lines = lines[:1]
	}
	if len(lines) != 1 {
		return zero, false
	}
	head, fields, ok := strings.Cut(lines[0], " ")
	if !ok || head != "gifford-refused-"+nonce {
		return zero, false
	}
	parts := strings.Split(fields, " ")
	if len(parts) != 7 {
		return zero, false
	}
	var out7 [7]string
	copy(out7[:], parts)
	if (out7[0] != "0" && out7[0] != "1") || (out7[1] != "0" && out7[1] != "1") {
		return zero, false
	}
	if !allDigits(out7[2]) || !allDigits(out7[3]) || !allDigits(out7[5]) || !allDigits(out7[6]) {
		return zero, false
	}
	if !tmuxSessionPattern.MatchString(out7[4]) {
		return zero, false
	}
	return out7, true
}

// classifyRefusal maps the decision-instant fields to a reason.
func classifyRefusal(fields [7]string, req openRequest) openOutcome {
	pipe, dead, pid, start, session, wi, pi := fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]
	if pid != strconv.Itoa(req.server.pid) || start != strconv.FormatInt(req.server.startTime, 10) {
		return openOutcome{refused: true, reason: refuseServerReplaced}
	}
	if session != req.slot.sessionID || wi != strconv.Itoa(req.slot.windowIndex) || pi != strconv.Itoa(req.slot.paneIndex) {
		return openOutcome{refused: true, reason: refusePaneMoved}
	}
	if dead == "1" {
		return openOutcome{refused: true, reason: refuseDead}
	}
	if req.mode == modeNormal && pipe == "1" {
		return openOutcome{refused: true, reason: refusePipePresent}
	}
	return openOutcome{}
}

// allDigits reports ASCII digits only, at least one.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// firstLine returns the text before the first newline.
func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

// isUnreachable reports a failed call whose server is gone.
func isUnreachable(stderr []byte) bool {
	s := string(stderr)
	return strings.Contains(s, "no server running") || strings.Contains(s, "error connecting to")
}

// isMissingPane reports fact 15: the pane is gone.
func isMissingPane(stderr []byte) bool {
	s := string(stderr)
	return strings.Contains(s, "can't find pane") || strings.Contains(s, "target pane has exited")
}

// pipeCommand renders argv as the shell-command of tmux pipe-pane -O:
// "exec" and each word in POSIX single quotes for /bin/sh -c, so the
// program replaces the shell and is the pipe's only holder; then every '#'
// and '%' doubled, because tmux expands the string as a format and through
// strftime before running it.
func pipeCommand(argv []string) string {
	words := make([]string, len(argv))
	for i, a := range argv {
		words[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	cmd := "exec " + strings.Join(words, " ")
	return strings.NewReplacer("#", "##", "%", "%%").Replace(cmd)
}

// tmuxQuote renders s as one word of the tmux command language: single
// quotes, inside which nothing is replaced, and each ' as '\”.
func tmuxQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
