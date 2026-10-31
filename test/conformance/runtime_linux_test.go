//go:build conformance && linux

package conformance

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	conformanceRecipient = "conformance-recipient"
	conformanceSender    = "conformance-sender"
	tmuxInitialTarget    = "conformance:0.0"
	profileWarning       = "sin medir; using generic"
	tmuxSetOption        = "set-option"
)

type privateEnvironment struct {
	root     string
	database string
	env      []string
}

func newPrivateEnvironment(config *suiteConfig) (privateEnvironment, error) {
	root, err := os.MkdirTemp(config.root, "r-*")
	if err != nil {
		return privateEnvironment{}, fmt.Errorf("create run root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return privateEnvironment{}, fmt.Errorf("secure run root: %w", err)
	}
	configHome := filepath.Join(root, "config")
	dataHome := filepath.Join(root, "data")
	runtimeHome := filepath.Join(root, "runtime")
	for _, directory := range []string{
		configHome,
		dataHome,
		runtimeHome,
		filepath.Join(configHome, "gifford"),
	} {
		if err := secureMkdirAll(directory); err != nil {
			return privateEnvironment{}, err
		}
	}
	configuration := []byte("schema_version: 1\nnode_id: node-conformance\n")
	if err := os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"), configuration, 0o600); err != nil {
		return privateEnvironment{}, fmt.Errorf("write private config: %w", err)
	}
	environment := tmuxEnvironment(os.Environ())
	environment = setEnv(environment, "XDG_CONFIG_HOME", configHome)
	environment = setEnv(environment, "XDG_DATA_HOME", dataHome)
	environment = setEnv(environment, "XDG_RUNTIME_DIR", runtimeHome)
	environment = setEnv(environment, "GORACE", "atexit_sleep_ms=0")
	return privateEnvironment{
		root: root, database: filepath.Join(dataHome, "gifford", "gifford.db"), env: environment,
	}, nil
}

func tmuxEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name != "TMUX" && name != "TMUX_PANE" && name != "TERM" {
			result = append(result, entry)
		}
	}
	return append(result, "TERM=xterm-256color")
}

type managedProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	mu      sync.Mutex
	waitErr error
}

func startManagedProcess(cmd *exec.Cmd) (*managedProcess, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	process := &managedProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

func (process *managedProcess) result() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.waitErr
}

func (process *managedProcess) wait(timeout time.Duration) error {
	select {
	case <-process.done:
		return process.result()
	case <-time.After(timeout):
		return fmt.Errorf("process %d did not stop", process.cmd.Process.Pid)
	}
}

func (process *managedProcess) terminate() error {
	if process == nil || process.cmd == nil || process.cmd.Process == nil {
		return nil
	}
	select {
	case <-process.done:
		return process.result()
	default:
	}
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("terminate process: %w", err)
	}
	select {
	case <-process.done:
		return process.result()
	case <-time.After(5 * time.Second):
	}
	if err := process.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill process: %w", err)
	}
	select {
	case <-process.done:
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("process %d survived kill", process.cmd.Process.Pid)
	}
}

func startHub(ctx context.Context, config *suiteConfig, environment privateEnvironment) (*managedProcess, error) {
	cmd := exec.CommandContext(ctx, config.gifford, "hub")
	cmd.Env = environment.env
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("open hub stderr: %w", err)
	}
	cmd.Stdout = io.Discard
	process, err := startManagedProcess(cmd)
	if err != nil {
		return nil, fmt.Errorf("start hub: %w", err)
	}
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			if !found && strings.Contains(line, "ready: serving") {
				found = true
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
		return process, nil
	case <-process.done:
		return nil, fmt.Errorf("hub stopped before ready: %w", process.result())
	case <-time.After(15 * time.Second):
		_ = process.terminate()
		return nil, errors.New("hub did not become ready")
	case <-ctx.Done():
		_ = process.terminate()
		return nil, fmt.Errorf("wait for hub: %w", ctx.Err())
	}
}

func writeHarnessWrapper(config *suiteConfig, environment privateEnvironment, mutant string) (string, error) {
	path := filepath.Join(environment.root, "harness-wrapper")
	var script strings.Builder
	script.WriteString("#!/bin/sh\nset -eu\n")
	for _, name := range []string{"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_RUNTIME_DIR"} {
		original := config.originalXDG[name]
		if original.set {
			fmt.Fprintf(&script, "export %s=%s\n", name, shellQuote(original.value))
		} else {
			fmt.Fprintf(&script, "unset %s\n", name)
		}
	}
	if config.harness == syntheticHarness {
		fmt.Fprintf(&script, "export %s=1\n", syntheticModeEnv)
		fmt.Fprintf(&script, "export %s=%s\n", syntheticMutantEnv, shellQuote(mutant))
	}
	fmt.Fprintf(&script, "exec %s \"$@\"\n", shellQuote(config.command[0]))
	if err := os.WriteFile(path, []byte(script.String()), 0o700); err != nil {
		return "", fmt.Errorf("write harness wrapper: %w", err)
	}
	return path, nil
}

func writeRunLauncher(
	config *suiteConfig,
	environment privateEnvironment,
	wrapper, mutant string,
) (string, error) {
	profileHarness := config.profileHarness
	if mutant == mutantProfileWarning {
		profileHarness = "codex"
	}
	arguments := make([]string, 0, 7+len(config.command))
	arguments = append(arguments,
		config.gifford,
		"run", "--agent", conformanceRecipient, "--harness", profileHarness, "--", wrapper,
	)
	arguments = append(arguments, config.command[1:]...)
	var command strings.Builder
	command.WriteString("#!/bin/sh\nset -eu\nexec")
	for _, argument := range arguments {
		command.WriteByte(' ')
		command.WriteString(shellQuote(argument))
	}
	command.WriteByte('\n')
	path := filepath.Join(environment.root, "run-recipient")
	if err := os.WriteFile(path, []byte(command.String()), 0o700); err != nil {
		return "", fmt.Errorf("write run launcher: %w", err)
	}
	return path, nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

type terminalQuery struct {
	query    []byte
	response []byte
}

var terminalQueries = []terminalQuery{
	{query: []byte("\x1b[c"), response: []byte("\x1b[?1;2c")},
	{query: []byte("\x1b[>c"), response: []byte("\x1b[>0;95;0c")},
	{query: []byte("\x1b[>q"), response: []byte("\x1bP>|xterm 390\x1b\\")},
	{query: []byte("\x1b[>0q"), response: []byte("\x1bP>|xterm 390\x1b\\")},
	{query: []byte("\x1b[6n"), response: []byte("\x1b[1;1R")},
	{query: []byte("\x1b]10;?\x1b\\"), response: []byte("\x1b]10;rgb:eeee/eeee/eeee\x1b\\")},
	{query: []byte("\x1b]10;?\a"), response: []byte("\x1b]10;rgb:eeee/eeee/eeee\x1b\\")},
	{query: []byte("\x1b]11;?\x1b\\"), response: []byte("\x1b]11;rgb:1111/2222/3333\x1b\\")},
	{query: []byte("\x1b]11;?\a"), response: []byte("\x1b]11;rgb:1111/2222/3333\x1b\\")},
}

type terminalResponder struct {
	mu     sync.Mutex
	output []byte
	seen   []int
	pty    *os.File
	cmd    *exec.Cmd
	served chan error
	waited chan error
}

func startTerminalResponder(
	ctx context.Context,
	tmux string,
	environment []string,
	socket string,
) (*terminalResponder, error) {
	cmd := exec.CommandContext(ctx, tmux, "-S", socket, "-f", "/dev/null", "attach-session", "-t", "conformance")
	cmd.Env = environment
	terminal, err := pty.Start(cmd)
	if err != nil {
		return nil, fmt.Errorf("attach terminal to tmux: %w", err)
	}
	if err := pty.Setsize(terminal, &pty.Winsize{Rows: 40, Cols: 120}); err != nil {
		_ = terminal.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("size attached terminal: %w", err)
	}
	responder := &terminalResponder{
		pty: terminal, cmd: cmd, served: make(chan error, 1), waited: make(chan error, 1),
		seen: make([]int, len(terminalQueries)),
	}
	go responder.serve()
	go func() { responder.waited <- cmd.Wait() }()
	return responder, nil
}

func (responder *terminalResponder) serve() {
	buffer := make([]byte, 32*1024)
	for {
		count, err := responder.pty.Read(buffer)
		if count > 0 {
			responder.mu.Lock()
			responder.output = append(responder.output, buffer[:count]...)
			var replies [][]byte
			for index, query := range terminalQueries {
				observed := bytes.Count(responder.output, query.query)
				for responder.seen[index] < observed {
					replies = append(replies, query.response)
					responder.seen[index]++
				}
			}
			if len(responder.output) > 1<<20 {
				responder.output = append([]byte(nil), responder.output[len(responder.output)-(64<<10):]...)
				for index := range responder.seen {
					responder.seen[index] = bytes.Count(responder.output, terminalQueries[index].query)
				}
			}
			responder.mu.Unlock()
			for _, reply := range replies {
				if _, writeErr := responder.pty.Write(reply); writeErr != nil {
					responder.served <- writeErr
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, unix.EIO) {
				err = nil
			}
			responder.served <- err
			return
		}
	}
}

func (responder *terminalResponder) sawStartupQueries() bool {
	responder.mu.Lock()
	defer responder.mu.Unlock()
	return responder.seen[0] > 0 && responder.seen[1] > 0 && responder.seen[2] > 0
}

func (responder *terminalResponder) close() error {
	if responder == nil {
		return nil
	}
	_ = responder.pty.Close()
	if responder.cmd.Process != nil {
		_ = responder.cmd.Process.Kill()
	}
	var result error
	select {
	case err := <-responder.served:
		result = err
	case <-time.After(2 * time.Second):
		result = errors.New("tmux terminal responder did not stop")
	}
	select {
	case err := <-responder.waited:
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			result = errors.Join(result, err)
		}
	case <-time.After(2 * time.Second):
		result = errors.Join(result, errors.New("tmux attach process did not stop"))
	}
	return result
}

type tmuxSession struct {
	command     string
	environment []string
	socket      string
	pipeLog     string
	target      string
	panePID     int
	serverPID   int
	terminal    *terminalResponder
}

func startPrivateTMUX(
	ctx context.Context,
	environment privateEnvironment,
	cwd, launcher string,
) (*tmuxSession, error) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		return nil, fmt.Errorf("tmux is required: %w", err)
	}
	session := &tmuxSession{
		command: tmux, environment: environment.env,
		socket:  filepath.Join(environment.root, "tmux.sock"),
		pipeLog: filepath.Join(environment.root, "panel-output.log"),
		target:  tmuxInitialTarget,
	}
	fail := func(cause error) (*tmuxSession, error) {
		_ = session.close()
		return nil, cause
	}
	if _, err := session.run(ctx, "new-session", "-d", "-s", "conformance", "-x", "120", "-y", "40"); err != nil {
		return fail(err)
	}
	options := [][]string{
		{tmuxSetOption, "-s", "escape-time", "10"},
		{tmuxSetOption, "-s", "extended-keys", "off"},
		{tmuxSetOption, "-g", "default-terminal", "screen-256color"},
		{"set-window-option", "-g", "window-size", "manual"},
		{"resize-window", "-t", "conformance", "-x", "120", "-y", "40"},
	}
	for _, arguments := range options {
		if _, err := session.run(ctx, arguments...); err != nil {
			return fail(err)
		}
	}
	serverPID, err := session.formatPID(ctx, "#{pid}")
	if err != nil {
		return fail(err)
	}
	session.serverPID = serverPID
	pipeFile, err := os.OpenFile(session.pipeLog, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fail(fmt.Errorf("create private pane log: %w", err))
	}
	if err := errors.Join(pipeFile.Chmod(0o600), pipeFile.Close()); err != nil {
		return fail(fmt.Errorf("secure private pane log: %w", err))
	}
	if err := session.pipePane(ctx); err != nil {
		return fail(err)
	}
	terminal, err := startTerminalResponder(ctx, tmux, environment.env, session.socket)
	if err != nil {
		return fail(err)
	}
	session.terminal = terminal
	if err := waitFor(ctx, 5*time.Second, 20*time.Millisecond, terminal.sawStartupQueries); err != nil {
		return fail(errors.New("attached terminal did not answer tmux startup queries"))
	}
	if _, err := session.run(ctx, "respawn-pane", "-k", "-c", cwd, "-t", session.target, launcher); err != nil {
		return fail(err)
	}
	panePID, err := session.formatPID(ctx, "#{pane_pid}")
	if err != nil {
		return fail(err)
	}
	session.panePID = panePID
	return session, nil
}

func (session *tmuxSession) pipePane(ctx context.Context) error {
	pipeCommand := "cat >> " + shellQuote(session.pipeLog)
	_, err := session.run(ctx, "pipe-pane", "-O", "-t", session.target, pipeCommand)
	return err
}

func (session *tmuxSession) reserveWindow(ctx context.Context) (string, error) {
	// tmux 3.4 exits its server when new-window follows resize-window while
	// window-size is manual. S4 has already observed the resize and UNKNOWN;
	// use the attached terminal's measured size for the replacement window.
	if _, err := session.run(ctx, "set-window-option", "-g", "window-size", "latest"); err != nil {
		return "", err
	}
	output, err := session.run(ctx, "new-window", "-d", "-P", "-F",
		"#{session_name}:#{window_index}.#{pane_index}", "-t", "conformance:", "exec sleep 180")
	if err != nil {
		return "", err
	}
	target := strings.TrimSpace(output)
	if !strings.HasPrefix(target, "conformance:") || !strings.Contains(target, ".") {
		return "", fmt.Errorf("unexpected tmux window target %q", boundedText(target, 200))
	}
	return target, nil
}

func (session *tmuxSession) activateWindow(ctx context.Context, target, cwd, launcher string) error {
	session.target = target
	if err := session.pipePane(ctx); err != nil {
		return err
	}
	if _, err := session.run(ctx, "select-window", "-t", session.target); err != nil {
		return err
	}
	if err := session.resize(ctx, 120, 40); err != nil {
		return err
	}
	if _, err := session.run(ctx, "respawn-pane", "-k", "-c", cwd, "-t", session.target, launcher); err != nil {
		return err
	}
	panePID, err := session.formatPID(ctx, "#{pane_pid}")
	if err != nil {
		return err
	}
	session.panePID = panePID
	return nil
}

func (session *tmuxSession) resize(ctx context.Context, cols, rows int) error {
	_, err := session.run(ctx, "resize-window", "-t", session.target,
		"-x", strconv.Itoa(cols), "-y", strconv.Itoa(rows))
	return err
}

func (session *tmuxSession) run(ctx context.Context, arguments ...string) (string, error) {
	commandArgs := make([]string, 0, 4+len(arguments))
	commandArgs = append(commandArgs, "-S", session.socket, "-f", "/dev/null")
	commandArgs = append(commandArgs, arguments...)
	commandCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, session.command, commandArgs...)
	cmd.Env = session.environment
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("tmux %s: %w: %s", arguments[0], err, boundedText(string(output), 2000))
	}
	return string(output), nil
}

func (session *tmuxSession) formatPID(ctx context.Context, format string) (int, error) {
	output, err := session.run(ctx, "display-message", "-p", "-t", session.target, format)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, fmt.Errorf("parse tmux pid: %w", err)
	}
	return pid, nil
}

func (session *tmuxSession) capture(ctx context.Context) (string, error) {
	output, err := session.run(ctx, "capture-pane", "-p", "-t", session.target)
	if err != nil {
		return "", fmt.Errorf("capture tmux pane: %w", err)
	}
	return output, nil
}

func (session *tmuxSession) paneGeometry(ctx context.Context) (col, row, cols, rows int, visible bool, err error) {
	output, err := session.run(ctx, "display-message", "-p", "-t", session.target,
		"#{cursor_x} #{cursor_y} #{pane_width} #{pane_height} #{cursor_flag}")
	if err != nil {
		return 0, 0, 0, 0, false, err
	}
	fields := strings.Fields(output)
	if len(fields) != 5 {
		return 0, 0, 0, 0, false, fmt.Errorf("unexpected tmux pane geometry %q", boundedText(output, 200))
	}
	values := make([]int, len(fields))
	for index, field := range fields {
		values[index], err = strconv.Atoi(field)
		if err != nil {
			return 0, 0, 0, 0, false, fmt.Errorf("parse tmux pane geometry: %w", err)
		}
	}
	return values[0], values[1], values[2], values[3], values[4] == 1, nil
}

func (session *tmuxSession) containsOutput(needle string) bool {
	raw, err := os.ReadFile(session.pipeLog)
	return err == nil && bytes.Contains(raw, []byte(needle))
}

func (session *tmuxSession) outputOccurrences(needle string) int {
	raw, err := os.ReadFile(session.pipeLog)
	if err != nil {
		return 0
	}
	return bytes.Count(raw, []byte(needle))
}

func (session *tmuxSession) completedOutputLines(needle string) int {
	raw, err := os.ReadFile(session.pipeLog)
	if err != nil {
		return 0
	}
	// pipe-pane includes every in-progress redraw. The CRLF-terminated copy is
	// the physical line submitted to the harness.
	return bytes.Count(raw, []byte(needle+"\r\n"))
}

func (session *tmuxSession) sendBytes(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	arguments := []string{"send-keys", "-H", "-t", session.target}
	for _, value := range data {
		arguments = append(arguments, fmt.Sprintf("%02x", value))
	}
	_, err := session.run(ctx, arguments...)
	return err
}

func (session *tmuxSession) close() error {
	if session == nil || session.command == "" {
		return nil
	}
	tracked := map[processIdentity]bool{}
	if session.serverPID > 0 {
		if process, ok := processByPID(session.serverPID); ok {
			tracked[process.identity()] = true
		}
		for _, process := range descendants(session.serverPID) {
			tracked[process.identity()] = true
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, killErr := session.run(ctx, "kill-server")
	terminalErr := session.terminal.close()
	processErr := waitProcessesGone(tracked, 5*time.Second)
	removeErr := os.Remove(session.socket)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(killErr, terminalErr, processErr, removeErr)
}

func openReadOnlyDatabase(ctx context.Context, path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		return nil, fmt.Errorf("open read-only database: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("ping read-only database: %w", err)
	}
	return database, nil
}

type processInfo struct {
	pid       int
	ppid      int
	startTime uint64
	name      string
}

type processIdentity struct {
	pid       int
	startTime uint64
}

func (process processInfo) identity() processIdentity {
	return processIdentity{pid: process.pid, startTime: process.startTime}
}

func processByPID(pid int) (processInfo, bool) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processInfo{}, false
	}
	line := string(raw)
	closeParen := strings.LastIndexByte(line, ')')
	openParen := strings.IndexByte(line, '(')
	if openParen < 0 || closeParen <= openParen || closeParen+4 >= len(line) {
		return processInfo{}, false
	}
	fields := strings.Fields(line[closeParen+1:])
	if len(fields) <= 19 {
		return processInfo{}, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return processInfo{}, false
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return processInfo{}, false
	}
	name := line[openParen+1 : closeParen]
	if executable, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe")); err == nil {
		name = filepath.Base(executable)
	}
	return processInfo{pid: pid, ppid: ppid, startTime: startTime, name: name}, true
}

func processTable() map[int]processInfo {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	table := make(map[int]processInfo)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		process, ok := processByPID(pid)
		if ok {
			table[pid] = process
		}
	}
	return table
}

func descendants(root int) []processInfo {
	table := processTable()
	children := make(map[int][]processInfo)
	for _, process := range table {
		children[process.ppid] = append(children[process.ppid], process)
	}
	queue := []int{root}
	var result []processInfo
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			result = append(result, child)
			queue = append(queue, child.pid)
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].pid < result[right].pid })
	return result
}

func processTreeEvidence(processes []processInfo) string {
	parts := make([]string, 0, len(processes))
	for _, process := range processes {
		parts = append(parts, fmt.Sprintf("pid=%d ppid=%d name=%s", process.pid, process.ppid, process.name))
	}
	return strings.Join(parts, "; ")
}

func processAlive(identity processIdentity) bool {
	process, ok := processByPID(identity.pid)
	return ok && process.startTime == identity.startTime
}

func signalProcess(identity processIdentity, signal syscall.Signal) error {
	pidfd, err := unix.PidfdOpen(identity.pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open pidfd for %d: %w", identity.pid, err)
	}
	defer unix.Close(pidfd)
	if !processAlive(identity) {
		return nil
	}
	if err := unix.PidfdSendSignal(pidfd, unix.Signal(signal), nil, 0); err != nil &&
		!errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal process %d through pidfd: %w", identity.pid, err)
	}
	return nil
}

func waitProcessesGone(processes map[processIdentity]bool, timeout time.Duration) error {
	if len(processes) == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		var remaining []processIdentity
		for process := range processes {
			if processAlive(process) {
				remaining = append(remaining, process)
			}
		}
		if len(remaining) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			sort.Slice(remaining, func(left, right int) bool { return remaining[left].pid < remaining[right].pid })
			pids := make([]int, 0, len(remaining))
			var signalErr error
			for _, process := range remaining {
				pids = append(pids, process.pid)
				signalErr = errors.Join(signalErr, signalProcess(process, syscall.SIGKILL))
			}
			cleanupDeadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(cleanupDeadline) {
				allGone := true
				for _, process := range remaining {
					if processAlive(process) {
						allGone = false
						break
					}
				}
				if allGone {
					return errors.Join(fmt.Errorf("suite processes required SIGKILL: %v", pids), signalErr)
				}
				time.Sleep(20 * time.Millisecond)
			}
			var survived []int
			for _, process := range remaining {
				if processAlive(process) {
					survived = append(survived, process.pid)
				}
			}
			return errors.Join(fmt.Errorf("suite processes survived SIGKILL: %v", survived), signalErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitFor(ctx context.Context, timeout, interval time.Duration, condition func() bool) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if condition() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errors.New("condition timed out")
		case <-ticker.C:
		}
	}
}

// staysTrue observes a negative invariant for the whole interval. It returns
// as soon as the invariant breaks; the timeout is success, not evidence that
// any state changed.
func staysTrue(ctx context.Context, duration, interval time.Duration, condition func() bool) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if !condition() {
			return errors.New("condition changed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-ticker.C:
		}
	}
}
