package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/tui"
)

const (
	e2eHarnessEnv      = "GIFFORD_E2E_FAKE_HARNESS"
	e2eRecipientEnv    = "GIFFORD_E2E_RECIPIENT_HARNESS"
	e2eRecipientLogEnv = "GIFFORD_E2E_RECIPIENT_LOG"
	e2eLinux           = "linux"
	e2eWrapperMode     = "wrapper"
	e2ePanelAgent      = "recipient-panel"
	e2eBinaryEnv       = "GIFFORD_E2E_GIFFORD_BINARY"
	e2eJSONRPCVersion  = "2.0"
	e2eNameField       = "name"
	e2eToField         = "to"
	e2eSubjectField    = "subject"
	e2eBodyField       = "body"
	e2eClientIDField   = "client_msg_id"
	e2eMessageIDField  = "message_id"
	e2eGenerationField = "delivery_generation"
	e2eStatusField     = "status"
	e2eArgsField       = "args"
	e2eStoredRemote    = "STORED_REMOTE"
	e2eBeta            = "beta"
	e2eRPCTimeout      = 10 * time.Second
)

var (
	e2eGiffordBinary         string
	e2eMeasuredGiffordBinary string
)

func TestMain(m *testing.M) {
	for _, arg := range os.Args[1:] {
		if arg == "--version" {
			_, _ = fmt.Fprintln(os.Stdout, "gifford-e2e-harness 1.0")
			os.Exit(0)
		}
	}
	if os.Getenv(e2eHarnessEnv) == "1" {
		os.Exit(runE2EHarness())
	}
	if mode := os.Getenv(e2eRecipientEnv); mode != "" {
		os.Exit(runE2ERecipientHarness(mode))
	}
	var buildDir string
	if runtime.GOOS == e2eLinux {
		var err error
		buildDir, err = os.MkdirTemp("", "gifford-e2e-bin-*")
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create e2e build dir: %v\n", err)
			os.Exit(2)
		}
		e2eGiffordBinary = filepath.Join(buildDir, "gifford")
		e2eMeasuredGiffordBinary = filepath.Join(buildDir, "gifford-measured")
		if err := buildE2EBinary(e2eGiffordBinary, ""); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build e2e gifford: %v\n", err)
			_ = os.RemoveAll(buildDir)
			os.Exit(2)
		}
		overlay, err := prepareE2EProfileOverlay(buildDir)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "prepare e2e profile overlay: %v\n", err)
			_ = os.RemoveAll(buildDir)
			os.Exit(2)
		}
		if err := buildE2EBinary(e2eMeasuredGiffordBinary, overlay); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build measured e2e gifford: %v\n", err)
			_ = os.RemoveAll(buildDir)
			os.Exit(2)
		}
	}
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func buildE2EBinary(output, overlay string) error {
	args := []string{"build", "-race"}
	if overlay != "" {
		args = append(args, "-overlay", overlay)
	}
	args = append(args, "-o", output, ".")
	cmd := exec.Command("go", args...) //nolint:noctx // TestMain has no context.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func prepareE2EProfileOverlay(buildDir string) (string, error) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("locate e2e source")
	}
	packageDir := filepath.Dir(sourceFile)
	realProfile := filepath.Clean(filepath.Join(packageDir, "..", "..", "profiles", "generic.yaml"))
	realRaw, err := os.ReadFile(realProfile)
	if err != nil {
		return "", fmt.Errorf("read real generic profile: %w", err)
	}
	parsedProfile, err := profiles.ParseFile("generic", realRaw)
	if err != nil {
		return "", fmt.Errorf("parse real generic profile: %w", err)
	}
	if parsedProfile.Measured {
		return "", fmt.Errorf("real generic profile unexpectedly measured")
	}
	fixtureRaw, err := os.ReadFile(filepath.Join(packageDir, "testdata", "e2e-harness-profile.yaml"))
	if err != nil {
		return "", fmt.Errorf("read e2e profile fixture: %w", err)
	}
	fixtureCopy := filepath.Join(buildDir, "e2e-generic.yaml")
	if err := os.WriteFile(fixtureCopy, fixtureRaw, 0o600); err != nil {
		return "", fmt.Errorf("copy e2e profile fixture: %w", err)
	}
	overlayRaw, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{realProfile: fixtureCopy}})
	if err != nil {
		return "", fmt.Errorf("encode build overlay: %w", err)
	}
	overlayPath := filepath.Join(buildDir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlayRaw, 0o600); err != nil {
		return "", fmt.Errorf("write build overlay: %w", err)
	}
	return overlayPath, nil
}

func runE2ERecipientHarness(mode string) int {
	if mode == "sleep" {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
		<-stop
		signal.Stop(stop)
		return 0
	}
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return 2
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()
	if mode == "tool" {
		child := exec.Command(os.Args[0]) //nolint:noctx // the harness owns its tool child.
		child.Env = append(os.Environ(), e2eRecipientEnv+"=sleep")
		child.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
		if err := child.Start(); err != nil {
			return 2
		}
		defer func() {
			_ = child.Process.Kill()
			_ = child.Wait()
		}()
	}
	logPath := os.Getenv(e2eRecipientLogEnv)
	draft := []byte{}
	drawArea := func() bool {
		if mode == "unbounded" {
			_, err := fmt.Fprint(os.Stdout, "\x1b[?2026h\r\x1b[2Ke2e> \x1b[5C\x1b[?2026l")
			return err == nil
		}
		_, err := fmt.Fprintf(os.Stdout, "\x1b[?2026h\r\x1b[2Ke2e-input-top\r\n\x1b[2Ke2e> %s\r\n\x1b[2Ke2e-input-bottom\x1b[1A\r\x1b[5C\x1b[?2026l", draft)
		return err == nil
	}
	redraw := func() bool {
		_, err := fmt.Fprintf(os.Stdout, "\x1b[?2026h\r\x1b[2Ke2e> %s\x1b[?2026l", draft)
		return err == nil
	}
	if !drawArea() {
		return 2
	}
	if mode == "noisy" {
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				_, _ = fmt.Fprint(os.Stdout, "\x1b[?2026h\r\x1b[2Ke2e> \x1b[?2026l")
			}
		}()
	}
	buf := make([]byte, 256)
	for {
		n, readErr := os.Stdin.Read(buf)
		for _, b := range buf[:n] {
			switch b {
			case '\r', '\n':
				if len(draft) > 0 && logPath != "" {
					file, openErr := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
					if openErr != nil {
						return 2
					}
					_, writeErr := fmt.Fprintln(file, string(draft))
					closeErr := file.Close()
					if writeErr != nil || closeErr != nil {
						return 2
					}
				}
				draft = draft[:0]
			case 0x03, 0x15:
				draft = draft[:0]
			case 0x7f, 0x08:
				if len(draft) > 0 {
					draft = draft[:len(draft)-1]
				}
			default:
				if b >= 0x20 {
					draft = append(draft, b)
				}
			}
			if !redraw() {
				return 2
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return 0
			}
			return 2
		}
	}
}

type e2eHarnessCommand struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type e2eRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type e2eRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type e2eRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *e2eRPCError    `json:"error,omitempty"`
}

func runE2EHarness() int {
	binary := os.Getenv(e2eBinaryEnv)
	if binary == "" {
		return 2
	}
	mcp := exec.Command(binary, "mcp") //nolint:noctx // the harness lifetime owns the child.
	stdin, err := mcp.StdinPipe()
	if err != nil {
		return 2
	}
	stdout, err := mcp.StdoutPipe()
	if err != nil {
		return 2
	}
	mcp.Stderr = os.Stderr
	if err := mcp.Start(); err != nil {
		return 2
	}
	reader := bufio.NewReader(stdout)
	nextID := int64(1)
	initialize, _ := json.Marshal(map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{e2eNameField: "gifford-e2e", "version": "1"},
	})
	if _, err := e2eMCPCall(stdin, reader, nextID, "initialize", initialize); err != nil {
		_ = stdin.Close()
		_ = mcp.Wait()
		return 2
	}
	nextID++
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		var command e2eHarnessCommand
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, `{"harness_error":%q}`+"\n", err.Error())
			continue
		}
		if command.Method == "exit" {
			_ = stdin.Close()
			if err := mcp.Wait(); err != nil {
				return 2
			}
			_, _ = fmt.Fprintln(os.Stdout, `{"stopped":true}`)
			return 0
		}
		if command.Method == "artifact_cli" {
			var request struct {
				Args []string `json:"args"`
			}
			if err := json.Unmarshal(command.Params, &request); err != nil {
				return 2
			}
			child := exec.Command(binary, request.Args...) //nolint:noctx // bounded by the harness lifetime.
			child.Env = os.Environ()
			var stderr bytes.Buffer
			child.Stderr = &stderr
			output, err := child.Output()
			response := e2eRPCResponse{JSONRPC: e2eJSONRPCVersion, ID: nextID}
			if err != nil {
				response.Error = &e2eRPCError{Code: -32603, Message: strings.TrimSpace(stderr.String())}
			} else {
				response.Result = bytes.TrimSpace(output)
			}
			nextID++
			if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
				return 2
			}
			continue
		}
		if command.Method == "artifact_cli_interrupt" {
			var request struct {
				Args        []string `json:"args"`
				AfterChunks int      `json:"after_chunks"`
			}
			if err := json.Unmarshal(command.Params, &request); err != nil {
				return 2
			}
			interrupted, err := runInterruptedArtifactCLI(binary, request.Args, request.AfterChunks)
			response := e2eRPCResponse{JSONRPC: e2eJSONRPCVersion, ID: nextID}
			if err != nil {
				response.Error = &e2eRPCError{Code: -32603, Message: err.Error()}
			} else {
				response.Result, _ = json.Marshal(interrupted)
			}
			nextID++
			if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
				return 2
			}
			continue
		}
		response, err := e2eMCPCall(stdin, reader, nextID, command.Method, command.Params)
		if err != nil {
			_ = stdin.Close()
			_ = mcp.Wait()
			return 2
		}
		nextID++
		_, _ = os.Stdout.Write(response)
		_, _ = os.Stdout.Write([]byte{'\n'})
	}
	_ = stdin.Close()
	if err := mcp.Wait(); err != nil {
		return 2
	}
	return 0
}

type e2eArtifactInterruptResult struct {
	UploadCalls int    `json:"upload_calls"`
	ChildError  string `json:"child_error"`
}

func runInterruptedArtifactCLI(
	binary string,
	args []string,
	afterChunks int,
) (e2eArtifactInterruptResult, error) {
	if afterChunks < 1 {
		return e2eArtifactInterruptResult{}, fmt.Errorf("invalid interruption point")
	}
	runtimeHome := os.Getenv("XDG_RUNTIME_DIR")
	if runtimeHome == "" {
		return e2eArtifactInterruptResult{}, fmt.Errorf("missing XDG_RUNTIME_DIR")
	}
	socketPath := filepath.Join(runtimeHome, "gifford", "hub.sock")
	realPath := socketPath + ".artifact-e2e-real"
	if err := os.Rename(socketPath, realPath); err != nil {
		return e2eArtifactInterruptResult{}, fmt.Errorf("move hub socket: %w", err)
	}
	restore := func() error {
		_ = os.Remove(socketPath)
		if err := os.Rename(realPath, socketPath); err != nil {
			return fmt.Errorf("restore hub socket: %w", err)
		}
		return nil
	}
	restored := false
	defer func() {
		if !restored {
			_ = restore()
		}
	}()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", socketPath)
	if err != nil {
		return e2eArtifactInterruptResult{}, fmt.Errorf("listen artifact interruption proxy: %w", err)
	}
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return e2eArtifactInterruptResult{}, fmt.Errorf("secure artifact interruption proxy: %w", err)
	}

	child := exec.Command(binary, args...) //nolint:noctx // the harness owns and kills the child.
	child.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	child.Stdout = &stdout
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		return e2eArtifactInterruptResult{}, fmt.Errorf("start interrupted artifact command: %w", err)
	}
	downstream, err := listener.Accept()
	if err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		return e2eArtifactInterruptResult{}, fmt.Errorf("accept artifact command: %w", err)
	}
	defer func() { _ = downstream.Close() }()
	upstream, err := (&net.Dialer{}).DialContext(context.Background(), "unix", realPath)
	if err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		return e2eArtifactInterruptResult{}, fmt.Errorf("connect interruption proxy to hub: %w", err)
	}
	defer func() { _ = upstream.Close() }()
	process, err := registry.CurrentProcess()
	if err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		return e2eArtifactInterruptResult{}, fmt.Errorf("describe interruption proxy: %w", err)
	}

	downstreamReader := bufio.NewReader(downstream)
	upstreamReader := bufio.NewReader(upstream)
	uploadCalls := 0
	for uploadCalls < afterChunks {
		request, err := downstreamReader.ReadBytes('\n')
		if err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			return e2eArtifactInterruptResult{}, fmt.Errorf(
				"read interrupted artifact request after %d uploads: %w; stderr: %s",
				uploadCalls, err, strings.TrimSpace(stderr.String()))
		}
		method, rewritten, err := rewriteE2EArtifactRequest(request, process)
		if err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			return e2eArtifactInterruptResult{}, err
		}
		if err := writeE2EFull(upstream, rewritten); err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			return e2eArtifactInterruptResult{}, err
		}
		response, err := upstreamReader.ReadBytes('\n')
		if err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			return e2eArtifactInterruptResult{}, fmt.Errorf("read hub response: %w", err)
		}
		if err := writeE2EFull(downstream, response); err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			return e2eArtifactInterruptResult{}, err
		}
		if method == hub.MethodUploadArtifactChunk {
			uploadCalls++
		}
	}
	if err := child.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return e2eArtifactInterruptResult{}, fmt.Errorf("kill artifact command: %w", err)
	}
	childErr := child.Wait()
	_ = downstream.Close()
	_ = upstream.Close()
	_ = listener.Close()
	if err := restore(); err != nil {
		return e2eArtifactInterruptResult{}, err
	}
	restored = true
	return e2eArtifactInterruptResult{
		UploadCalls: uploadCalls, ChildError: fmt.Sprint(childErr),
	}, nil
}

func rewriteE2EArtifactRequest(
	line []byte,
	process protocol.ProcessIdentity,
) (string, []byte, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(line), &envelope); err != nil {
		return "", nil, fmt.Errorf("decode interrupted artifact request: %w", err)
	}
	var method string
	if err := json.Unmarshal(envelope["method"], &method); err != nil {
		return "", nil, fmt.Errorf("decode interrupted artifact method: %w", err)
	}
	if method == hub.MethodRegisterEndpoint {
		var params map[string]json.RawMessage
		if err := json.Unmarshal(envelope["params"], &params); err != nil {
			return "", nil, fmt.Errorf("decode interrupted endpoint registration: %w", err)
		}
		params["process"], _ = json.Marshal(process)
		envelope["params"], _ = json.Marshal(params)
		var err error
		line, err = json.Marshal(envelope)
		if err != nil {
			return "", nil, fmt.Errorf("rewrite interrupted endpoint registration: %w", err)
		}
		line = append(line, '\n')
	}
	return method, line, nil
}

func writeE2EFull(writer io.Writer, value []byte) error {
	for offset := 0; offset < len(value); {
		n, err := writer.Write(value[offset:])
		if err != nil || n == 0 {
			if err == nil {
				err = io.ErrShortWrite
			}
			return fmt.Errorf("write artifact interruption proxy: %w", err)
		}
		offset += n
	}
	return nil
}

func e2eMCPCall(
	stdin io.Writer,
	reader *bufio.Reader,
	id int64,
	method string,
	params json.RawMessage,
) ([]byte, error) {
	request := e2eRPCRequest{JSONRPC: e2eJSONRPCVersion, ID: id, Method: method, Params: params}
	if err := json.NewEncoder(stdin).Encode(request); err != nil {
		return nil, err
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(line), nil
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type e2eProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	mu      sync.Mutex
	waitErr error
}

func startE2EProcess(cmd *exec.Cmd) (*e2eProcess, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	process := &e2eProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

func (p *e2eProcess) wait() error {
	select {
	case <-p.done:
		return p.result()
	case <-time.After(e2eRPCTimeout):
		_ = p.cmd.Process.Kill()
		<-p.done
		return fmt.Errorf("process %d did not stop", p.cmd.Process.Pid)
	}
}

func (p *e2eProcess) result() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *e2eProcess) terminateAndWait() error {
	select {
	case <-p.done:
		return p.result()
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return p.wait()
}

type e2eAgent struct {
	process *e2eProcess
	outer   *os.File
	reader  *bufio.Reader
	stderr  *lockedBuffer
}

func startE2EAgent(
	ctx context.Context,
	t *testing.T,
	env []string,
	agent protocol.AgentID,
) *e2eAgent {
	return startE2EAgentBinary(ctx, t, env, agent, e2eGiffordBinary)
}

func startE2EAgentBinary(
	ctx context.Context,
	t *testing.T,
	env []string,
	agent protocol.AgentID,
	binary string,
) *e2eAgent {
	t.Helper()
	outer, slave, err := openE2EPTY()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary,
		"run", "--agent", string(agent), "--harness", "generic", "--", os.Args[0])
	cmd.Env = append(append([]string{}, env...),
		e2eHarnessEnv+"=1", e2eBinaryEnv+"="+binary, "TERM=xterm-256color")
	cmd.Stdin = slave
	cmd.Stdout = slave
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	_ = slave.Close()
	if err != nil {
		_ = outer.Close()
	}
	require.NoError(t, err)
	return &e2eAgent{process: process, outer: outer, reader: bufio.NewReader(outer), stderr: stderr}
}

func (a *e2eAgent) rpc(t *testing.T, method string, params any) e2eRPCResponse {
	t.Helper()
	rawParams, err := json.Marshal(params)
	require.NoError(t, err)
	rawCommand, err := json.Marshal(e2eHarnessCommand{Method: method, Params: rawParams})
	require.NoError(t, err)
	_ = a.outer.SetReadDeadline(time.Now().Add(e2eRPCTimeout))
	_, err = a.outer.Write(append(rawCommand, '\n'))
	require.NoError(t, err)
	for {
		line, readErr := a.reader.ReadString('\n')
		require.NoError(t, readErr, "agent stderr: %s", a.stderr.String())
		line = strings.TrimSpace(line)
		start, end := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
		if start < 0 || end < start {
			continue
		}
		var response e2eRPCResponse
		if json.Unmarshal([]byte(line[start:end+1]), &response) != nil || response.JSONRPC != "2.0" {
			continue
		}
		return response
	}
}

func (a *e2eAgent) callTool(t *testing.T, name string, args any, result any) {
	t.Helper()
	response := a.rpc(t, "tools/call", map[string]any{e2eNameField: name, "arguments": args})
	require.Nil(t, response.Error, "agent stderr: %s", a.stderr.String())
	decodeE2EToolResult(t, response.Result, result)
}

func (a *e2eAgent) artifactCLI(t *testing.T, result any, args ...string) {
	t.Helper()
	response := a.rpc(t, "artifact_cli", map[string]any{e2eArgsField: args})
	require.Nil(t, response.Error, "agent stderr: %s", a.stderr.String())
	require.NoError(t, json.Unmarshal(response.Result, result))
}

func (a *e2eAgent) artifactCLIError(t *testing.T, args ...string) string {
	t.Helper()
	response := a.rpc(t, "artifact_cli", map[string]any{e2eArgsField: args})
	require.NotNil(t, response.Error, "artifact command unexpectedly succeeded")
	return response.Error.Message
}

func (a *e2eAgent) interruptArtifactCLI(
	t *testing.T,
	afterChunks int,
	args ...string,
) e2eArtifactInterruptResult {
	t.Helper()
	response := a.rpc(t, "artifact_cli_interrupt", map[string]any{
		e2eArgsField: args, "after_chunks": afterChunks,
	})
	require.Nil(t, response.Error, "agent stderr: %s", a.stderr.String())
	var result e2eArtifactInterruptResult
	require.NoError(t, json.Unmarshal(response.Result, &result))
	return result
}

func (a *e2eAgent) stop(t *testing.T) {
	t.Helper()
	select {
	case <-a.process.done:
		require.NoError(t, a.process.result(), "agent stderr: %s", a.stderr.String())
		_ = a.outer.Close()
		return
	default:
	}
	raw, err := json.Marshal(e2eHarnessCommand{Method: "exit"})
	require.NoError(t, err)
	_, err = a.outer.Write(append(raw, '\n'))
	require.NoError(t, err)
	require.NoError(t, a.process.wait(), "agent stderr: %s", a.stderr.String())
	require.NoError(t, a.outer.Close())
}

type e2eToolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

func decodeE2EToolResult(t *testing.T, raw json.RawMessage, result any) {
	t.Helper()
	var tool e2eToolResult
	require.NoError(t, json.Unmarshal(raw, &tool))
	require.Len(t, tool.Content, 1)
	require.Equal(t, "text", tool.Content[0].Type)
	require.NoError(t, json.Unmarshal([]byte(tool.Content[0].Text), result))
}

type e2eMCPClient struct {
	process *e2eProcess
	stdin   io.WriteCloser
	reader  *bufio.Reader
	stderr  *lockedBuffer
	nextID  int64
}

func startE2EMCP(ctx context.Context, t *testing.T, env []string) *e2eMCPClient {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, "mcp")
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	require.NoError(t, err)
	return &e2eMCPClient{
		process: process, stdin: stdin, reader: bufio.NewReader(stdout), stderr: stderr, nextID: 1,
	}
}

func (c *e2eMCPClient) call(t *testing.T, method string, params any) e2eRPCResponse {
	t.Helper()
	rawParams, err := json.Marshal(params)
	require.NoError(t, err)
	request := e2eRPCRequest{JSONRPC: e2eJSONRPCVersion, ID: c.nextID, Method: method, Params: rawParams}
	c.nextID++
	require.NoError(t, json.NewEncoder(c.stdin).Encode(request))
	line, err := c.reader.ReadBytes('\n')
	require.NoError(t, err, "mcp stderr: %s", c.stderr.String())
	var response e2eRPCResponse
	require.NoError(t, json.Unmarshal(line, &response))
	return response
}

func (c *e2eMCPClient) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, c.stdin.Close())
	require.NoError(t, c.process.wait(), "mcp stderr: %s", c.stderr.String())
}

type e2eEnvironment struct {
	env        []string
	runtimeDir string
	dataDir    string
	database   string
}

func newE2EEnvironment(t *testing.T) e2eEnvironment {
	t.Helper()
	// t.TempDir includes the full test name, which can exceed Unix socket path limits.
	root, err := os.MkdirTemp("", "gifford-e2e-*") //nolint:usetesting
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	configHome := filepath.Join(root, "config")
	dataHome := filepath.Join(root, "data")
	runtimeHome := filepath.Join(root, "runtime")
	for _, dir := range []string{configHome, dataHome, runtimeHome, filepath.Join(configHome, "gifford")} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o700))
	}
	require.NoError(t, os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-a\n"), 0o600))
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_DATA_HOME="+dataHome,
		"XDG_RUNTIME_DIR="+runtimeHome,
		"GORACE=atexit_sleep_ms=0",
	)
	dataDir := filepath.Join(dataHome, "gifford")
	return e2eEnvironment{
		env: env, runtimeDir: filepath.Join(runtimeHome, "gifford"),
		dataDir: dataDir, database: filepath.Join(dataDir, "gifford.db"),
	}
}

func startE2EHub(ctx context.Context, t *testing.T, environment e2eEnvironment) (*e2eProcess, *lockedBuffer) {
	return startE2EHubBinary(ctx, t, environment, e2eGiffordBinary)
}

func startE2EHubBinary(
	ctx context.Context,
	t *testing.T,
	environment e2eEnvironment,
	binary string,
) (*e2eProcess, *lockedBuffer) {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, "hub")
	cmd.Env = environment.env
	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)
	stdout := &lockedBuffer{}
	cmd.Stdout = stdout
	process, err := startE2EProcess(cmd)
	require.NoError(t, err)
	ready := make(chan struct{})
	lines := &lockedBuffer{}
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(lines, line)
			if !found && strings.Contains(line, "ready: serving") {
				found = true
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-process.done:
		require.FailNow(t, "hub stopped before ready", "stderr: %s", lines.String())
	case <-time.After(e2eRPCTimeout):
		require.FailNow(t, "hub did not become ready", "stderr: %s", lines.String())
	}
	require.Empty(t, stdout.String())
	return process, lines
}

func runE2ECLI(ctx context.Context, t *testing.T, env []string, result any, args ...string) {
	runE2ECLIBinary(ctx, t, env, e2eGiffordBinary, result, args...)
}

func runE2ECLIBinary(
	ctx context.Context,
	t *testing.T,
	env []string,
	binary string,
	result any,
	args ...string,
) {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	require.NoError(t, err, "stderr: %s", stderr.String())
	require.NoError(t, json.Unmarshal(output, result), "output: %s", output)
}

type e2eRecipient struct {
	process *e2eProcess
	outer   *os.File
	stderr  *lockedBuffer
	output  *lockedBuffer
	logPath string
}

func startE2ERecipient(
	ctx context.Context,
	t *testing.T,
	environment e2eEnvironment,
	agent protocol.AgentID,
	mode string,
) *e2eRecipient {
	t.Helper()
	outer, slave, err := openE2EPTY()
	require.NoError(t, err)
	logPath := filepath.Join(environment.dataDir, "e2e-recipient-"+string(agent)+".log")
	command := []string{os.Args[0]}
	if mode == e2eWrapperMode {
		command = []string{"sh", "-c", `"$1"; true`, "e2e-wrapper", os.Args[0]}
	}
	cmdArgs := make([]string, 0, 6+len(command))
	cmdArgs = append(cmdArgs, "run", "--agent", string(agent), "--harness", "generic", "--")
	cmdArgs = append(cmdArgs, command...)
	cmd := exec.CommandContext(ctx, e2eMeasuredGiffordBinary, cmdArgs...)
	harnessMode := mode
	if mode == e2eWrapperMode {
		harnessMode = "quiet"
	}
	cmd.Env = append(append([]string{}, environment.env...),
		e2eRecipientEnv+"="+harnessMode, e2eRecipientLogEnv+"="+logPath,
		"TERM=xterm-256color")
	cmd.Stdin = slave
	cmd.Stdout = slave
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	_ = slave.Close()
	if err != nil {
		_ = outer.Close()
	}
	require.NoError(t, err)
	recipient := &e2eRecipient{
		process: process, outer: outer, stderr: stderr, output: &lockedBuffer{}, logPath: logPath,
	}
	go func() { _, _ = io.Copy(recipient.output, outer) }()
	require.Eventually(t, func() bool {
		return strings.Contains(recipient.output.String(), "e2e> ")
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient stderr: %s", stderr.String())
	return recipient
}

func (r *e2eRecipient) write(t *testing.T, text string) {
	t.Helper()
	_, err := io.WriteString(r.outer, text)
	require.NoError(t, err)
}

func (r *e2eRecipient) deliveredLines(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(r.logPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func openE2EReadDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func readE2ECLIJSON(
	ctx context.Context,
	env []string,
	binary string,
	result any,
	args ...string,
) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	if err := json.Unmarshal(output, result); err != nil {
		return fmt.Errorf("decode %q: %w", output, err)
	}
	return nil
}

func sendE2EMessageAndWaitApproval(
	ctx context.Context,
	t *testing.T,
	environment e2eEnvironment,
	sender *e2eAgent,
	recipient protocol.AgentID,
	clientID string,
) (protocol.AdmissionResult, protocol.ApprovalRequest) {
	t.Helper()
	var admitted protocol.AdmissionResult
	sender.callTool(t, "send_message", map[string]any{
		e2eToField: recipient, e2eSubjectField: "wiring",
		e2eBodyField: "real endpoint to guarded proxy", e2eClientIDField: clientID,
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	require.True(t, admitted.MessageID.Valid())
	var found protocol.ApprovalRequest
	require.Eventually(t, func() bool {
		var pending protocol.ApprovalList
		if err := readE2ECLIJSON(ctx, environment.env, e2eMeasuredGiffordBinary,
			&pending, "ctl", "approvals", "--pending"); err != nil {
			return false
		}
		for _, approval := range pending.Approvals {
			if approval.ObjectID == string(admitted.MessageID) && approval.AgentID == recipient {
				found = approval
				return true
			}
		}
		return false
	}, 12*time.Second, 50*time.Millisecond, "automatic worker did not open approval")
	return admitted, found
}

func runE2ECLIError(ctx context.Context, env []string, binary string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stderr.String(), err
}

func requireNoE2ESockets(t *testing.T, runtimeDir string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, name := range []string{"hub.sock", "daemon.sock"} {
			if _, err := os.Lstat(filepath.Join(runtimeDir, name)); !errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRealProcessArtifactLifecycle(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux process ancestry and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })

	sender := startE2EAgent(ctx, t, environment.env, "artifact-sender")
	recipient := startE2EAgent(ctx, t, environment.env, "artifact-recipient")
	t.Cleanup(func() {
		_ = sender.process.terminateAndWait()
		_ = sender.outer.Close()
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	outsideStderr, outsideErr := runE2ECLIError(ctx, environment.env, e2eGiffordBinary,
		"artifact", "list")
	require.Error(t, outsideErr)
	require.Contains(t, outsideStderr, "hub")

	sourceDir := t.TempDir()
	outputDir := t.TempDir()
	marker := []byte("GIFFORD-H24-PLAINTEXT-MARKER")
	large := make([]byte, 3*1024*1024+512*1024)
	for offset := 0; offset < len(large); offset += len(marker) {
		copy(large[offset:], marker)
	}
	files := []struct {
		name string
		body []byte
	}{
		{name: "empty.bin", body: []byte{}},
		{name: "one.bin", body: []byte{0x7f}},
		{name: "large.bin", body: large},
	}
	type sentArtifact struct {
		ArtifactID  protocol.OpaqueID      `json:"artifact_id"`
		State       protocol.ArtifactState `json:"state"`
		ChunkCount  int64                  `json:"chunk_count"`
		ChunksSent  int64                  `json:"chunks_sent"`
		ResumedFrom int64                  `json:"resumed_from"`
		NoticeSent  bool                   `json:"notice_sent"`
	}
	type receivedArtifact struct {
		Path   string             `json:"path"`
		Size   int64              `json:"size"`
		SHA256 protocol.SHA256Hex `json:"sha256"`
	}
	for _, item := range files {
		t.Logf("artifact fixture %s", item.name)
		path := filepath.Join(sourceDir, item.name)
		require.NoError(t, os.WriteFile(path, item.body, 0o600))
		var sent sentArtifact
		sender.artifactCLI(t, &sent, "artifact", "send", path,
			"--to", "artifact-recipient", "--purpose", "e2e verification")
		require.Equal(t, protocol.ArtifactStateLocalAvailable, sent.State)
		require.True(t, sent.NoticeSent)
		wantChunks := int64((len(item.body) + 1024*1024 - 1) / (1024 * 1024))
		if wantChunks == 0 {
			wantChunks = 1
		}
		require.Equal(t, wantChunks, sent.ChunkCount)
		require.Equal(t, wantChunks, sent.ChunksSent)
		var pending protocol.ListPendingArtifactsResult
		recipient.artifactCLI(t, &pending, "artifact", "list")
		require.NotEmpty(t, pending.Artifacts)

		if len(item.body) > len(marker) {
			requireCiphertextAtRest(t, environment, marker)
		}
		var received receivedArtifact
		recipient.artifactCLI(t, &received, "artifact", "receive",
			string(sent.ArtifactID), "--out", outputDir)
		require.Equal(t, int64(len(item.body)), received.Size)
		back, err := os.ReadFile(received.Path)
		require.NoError(t, err)
		require.Equal(t, item.body, back)

		var status protocol.ArtifactStatusResult
		sender.artifactCLI(t, &status, "artifact", "status", string(sent.ArtifactID))
		require.Equal(t, protocol.ArtifactStateAcknowledged, status.State)
		storeDir := filepath.Join(environment.dataDir, "artifacts", "store", string(sent.ArtifactID))
		_, err = os.Stat(storeDir)
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	// A corrupted stored chunk must never produce plaintext or an ACK. The
	// recipient is permanently failed and ciphertext remains for diagnosis.
	tamperPath := filepath.Join(sourceDir, "tamper.bin")
	require.NoError(t, os.WriteFile(tamperPath, []byte("tamper plaintext"), 0o600))
	var tampered sentArtifact
	sender.artifactCLI(t, &tampered, "artifact", "send", tamperPath,
		"--to", "artifact-recipient", "--purpose", "tamper verification")
	tamperedChunk := filepath.Join(environment.dataDir, "artifacts", "store",
		string(tampered.ArtifactID), "00000000.chunk")
	rawChunk, err := os.ReadFile(tamperedChunk)
	require.NoError(t, err)
	rawChunk[len(rawChunk)-1] ^= 1
	require.NoError(t, os.WriteFile(tamperedChunk, rawChunk, 0o600))
	tamperOutput := t.TempDir()
	verificationError := recipient.artifactCLIError(t, "artifact", "receive",
		string(tampered.ArtifactID), "--out", tamperOutput)
	require.Contains(t, verificationError, "artifact verification failed")
	require.NotContains(t, verificationError, "tamper plaintext")
	entries, err := os.ReadDir(tamperOutput)
	require.NoError(t, err)
	require.Empty(t, entries)
	var tamperedStatus protocol.ArtifactStatusResult
	sender.artifactCLI(t, &tamperedStatus, "artifact", "status", string(tampered.ArtifactID))
	require.Equal(t, protocol.ArtifactStateLocalAvailable, tamperedStatus.State)
	require.Equal(t, protocol.ArtifactStateFailedPermanent, tamperedStatus.Recipients[0].State)
	require.DirExists(t, filepath.Dir(tamperedChunk))

	// Publishing uses a suffix and preserves an existing destination.
	collisionPath := filepath.Join(sourceDir, "collision.txt")
	require.NoError(t, os.WriteFile(collisionPath, []byte("new content"), 0o600))
	var collision sentArtifact
	sender.artifactCLI(t, &collision, "artifact", "send", collisionPath,
		"--to", "artifact-recipient", "--purpose", "collision verification")
	collisionOutput := t.TempDir()
	existing := filepath.Join(collisionOutput, "collision.txt")
	require.NoError(t, os.WriteFile(existing, []byte("keep me"), 0o600))
	var collisionReceived receivedArtifact
	recipient.artifactCLI(t, &collisionReceived, "artifact", "receive",
		string(collision.ArtifactID), "--out", collisionOutput)
	require.Equal(t, filepath.Join(collisionOutput, "collision.txt (1)"), collisionReceived.Path)
	require.Equal(t, []byte("keep me"), mustReadE2EFile(t, existing))
	require.Equal(t, []byte("new content"), mustReadE2EFile(t, collisionReceived.Path))

	var notices hub.ListPendingMessagesResult
	recipient.callTool(t, "list_pending_messages", map[string]any{}, &notices)
	require.GreaterOrEqual(t, len(notices.Messages), len(files)+2)
	sender.stop(t)
	recipient.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func mustReadE2EFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

func TestRealProcessArtifactUploadResumesAfterSenderKill(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux process ancestry and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	sender := startE2EAgent(ctx, t, environment.env, "artifact-resume-sender")
	recipient := startE2EAgent(ctx, t, environment.env, "artifact-resume-recipient")
	t.Cleanup(func() {
		_ = sender.process.terminateAndWait()
		_ = sender.outer.Close()
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})

	source := filepath.Join(t.TempDir(), "resume.bin")
	body := bytes.Repeat([]byte("resume-marker-"), 270_000)
	body = body[:3*1024*1024+512*1024]
	require.NoError(t, os.WriteFile(source, body, 0o600))
	args := []string{
		"artifact", "send", source, "--to", "artifact-resume-recipient",
		"--purpose", "resume verification",
	}
	interrupted := sender.interruptArtifactCLI(t, 2, args...)
	require.Equal(t, 2, interrupted.UploadCalls)
	require.Contains(t, interrupted.ChildError, "killed")

	db := openE2EReadDB(t, environment.database)
	var artifactID protocol.OpaqueID
	var storedChunks int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT artifact_id FROM artifact_transfers
		WHERE sender_agent = 'artifact-resume-sender'`).Scan(&artifactID))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_chunks
		WHERE artifact_id = ?`, artifactID).Scan(&storedChunks))
	require.Equal(t, int64(2), storedChunks)
	keyPath := filepath.Join(environment.dataDir, "artifacts", "outgoing", string(artifactID)+".key")
	require.Equal(t, os.FileMode(0o600), fileMode(t, keyPath))

	var resumed struct {
		ArtifactID  protocol.OpaqueID      `json:"artifact_id"`
		State       protocol.ArtifactState `json:"state"`
		ChunkCount  int64                  `json:"chunk_count"`
		ChunksSent  int64                  `json:"chunks_sent"`
		ResumedFrom int64                  `json:"resumed_from"`
	}
	sender.artifactCLI(t, &resumed, args...)
	require.Equal(t, artifactID, resumed.ArtifactID)
	require.Equal(t, int64(4), resumed.ChunkCount)
	require.Equal(t, int64(2), resumed.ResumedFrom)
	require.Equal(t, int64(2), resumed.ChunksSent)
	require.Equal(t, protocol.ArtifactStateLocalAvailable, resumed.State)
	_, err := os.Stat(keyPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	var received struct {
		Path string `json:"path"`
	}
	recipient.artifactCLI(t, &received, "artifact", "receive", string(artifactID),
		"--out", t.TempDir())
	require.Equal(t, body, mustReadE2EFile(t, received.Path))
	sender.stop(t)
	recipient.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func TestRealProcessArtifactReconcilesAfterHubRestart(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux process ancestry and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	firstHub, firstHubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() { _ = firstHub.terminateAndWait() })
	firstSender := startE2EAgent(ctx, t, environment.env, "artifact-restart-sender")
	firstRecipient := startE2EAgent(ctx, t, environment.env, "artifact-restart-recipient")
	t.Cleanup(func() {
		_ = firstSender.process.terminateAndWait()
		_ = firstSender.outer.Close()
		_ = firstRecipient.process.terminateAndWait()
		_ = firstRecipient.outer.Close()
	})

	source := filepath.Join(t.TempDir(), "restart.bin")
	body := bytes.Repeat([]byte("restart-marker-"), 250_000)
	body = body[:3*1024*1024+512*1024]
	require.NoError(t, os.WriteFile(source, body, 0o600))
	args := []string{
		"artifact", "send", source, "--to", "artifact-restart-recipient",
		"--purpose", "restart verification",
	}
	interrupted := firstSender.interruptArtifactCLI(t, 2, args...)
	require.Equal(t, 2, interrupted.UploadCalls)

	db := openE2EReadDB(t, environment.database)
	var artifactID protocol.OpaqueID
	require.NoError(t, db.QueryRowContext(ctx, `SELECT artifact_id FROM artifact_transfers
		WHERE sender_agent = 'artifact-restart-sender'`).Scan(&artifactID))
	firstSender.stop(t)
	firstRecipient.stop(t)
	require.NoError(t, firstHub.terminateAndWait(), "hub stderr: %s", firstHubStderr.String())
	missingChunk := filepath.Join(environment.dataDir, "artifacts", "store",
		string(artifactID), "00000001.chunk")
	require.NoError(t, os.Remove(missingChunk))

	secondHub, secondHubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() { _ = secondHub.terminateAndWait() })
	var storedChunks int64
	require.Eventually(t, func() bool {
		return db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_chunks
			WHERE artifact_id = ?`, artifactID).Scan(&storedChunks) == nil && storedChunks == 1
	}, e2eRPCTimeout, 20*time.Millisecond)
	secondSender := startE2EAgent(ctx, t, environment.env, "artifact-restart-sender")
	secondRecipient := startE2EAgent(ctx, t, environment.env, "artifact-restart-recipient")
	t.Cleanup(func() {
		_ = secondSender.process.terminateAndWait()
		_ = secondSender.outer.Close()
		_ = secondRecipient.process.terminateAndWait()
		_ = secondRecipient.outer.Close()
	})

	var resumed struct {
		State       protocol.ArtifactState `json:"state"`
		ChunksSent  int64                  `json:"chunks_sent"`
		ResumedFrom int64                  `json:"resumed_from"`
	}
	secondSender.artifactCLI(t, &resumed, args...)
	require.Equal(t, int64(1), resumed.ResumedFrom)
	require.Equal(t, int64(3), resumed.ChunksSent)
	require.Equal(t, protocol.ArtifactStateLocalAvailable, resumed.State)
	var received struct {
		Path string `json:"path"`
	}
	secondRecipient.artifactCLI(t, &received, "artifact", "receive", string(artifactID),
		"--out", t.TempDir())
	require.Equal(t, body, mustReadE2EFile(t, received.Path))
	secondSender.stop(t)
	secondRecipient.stop(t)
	require.NoError(t, secondHub.terminateAndWait(), "hub stderr: %s", secondHubStderr.String())
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}

func requireCiphertextAtRest(t *testing.T, environment e2eEnvironment, marker []byte) {
	t.Helper()
	paths := []string{environment.database, environment.database + "-wal"}
	storeRoot := filepath.Join(environment.dataDir, "artifacts", "store")
	_ = filepath.WalkDir(storeRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr == nil && !entry.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		require.NotContains(t, raw, marker, "plaintext marker found in %s", path)
	}
}

func TestRealProcessMessageLifecycle(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 55*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() {
		_ = hubProcess.terminateAndWait()
	})

	alpha := startE2EAgent(ctx, t, environment.env, "alpha")
	t.Cleanup(func() {
		select {
		case <-alpha.process.done:
		default:
			_ = alpha.process.terminateAndWait()
		}
		_ = alpha.outer.Close()
	})
	beta := startE2EAgent(ctx, t, environment.env, e2eBeta)
	t.Cleanup(func() {
		select {
		case <-beta.process.done:
		default:
			_ = beta.process.terminateAndWait()
		}
		_ = beta.outer.Close()
	})

	var pending hub.ListPendingMessagesResult
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	const firstBody = "first end-to-end body"
	var first protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "first", e2eBodyField: firstBody,
		e2eClientIDField: "e2e-message-one",
	}, &first)
	require.Equal(t, protocol.AdmissionStatusAccepted, first.Result)
	require.True(t, first.MessageID.Valid())

	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, first.MessageID, pending.Messages[0].MessageID)
	firstGeneration := pending.Messages[0].DeliveryGeneration
	var readFirst protocol.LogicalMessage
	beta.callTool(t, "read_message", map[string]any{
		"id": first.MessageID, e2eGenerationField: firstGeneration,
	}, &readFirst)
	require.Equal(t, firstBody, readFirst.Body)
	var acked map[string]bool
	beta.callTool(t, "ack_message", map[string]any{
		"id": first.MessageID, e2eGenerationField: firstGeneration, e2eStatusField: "ACKNOWLEDGED",
	}, &acked)
	require.True(t, acked["acked"])

	var firstStatus hub.GetDeliveryStatusResult
	alpha.callTool(t, "get_delivery_status", map[string]any{e2eMessageIDField: first.MessageID}, &firstStatus)
	require.Len(t, firstStatus.Recipients, 1)
	require.Equal(t, hub.AckMessageAcknowledged, firstStatus.Recipients[0].AckStatus)

	var links protocol.LinkList
	runE2ECLI(ctx, t, environment.env, &links, "ctl", "link", "set", "alpha", e2eBeta, "hold")
	const (
		secondBody = "held end-to-end body"
		secondID   = "e2e-message-two"
	)
	var second protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "held", e2eBodyField: secondBody,
		e2eClientIDField: secondID,
	}, &second)
	require.Equal(t, protocol.AdmissionStatusAcceptedHeld, second.Result)
	require.Equal(t, protocol.DecisionReasonLinkHold, second.ControlReason)
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	var held protocol.Explanation
	runE2ECLI(ctx, t, environment.env, &held,
		"ctl", "channel", "explain", "--message", string(second.MessageID))
	require.Equal(t, protocol.DecisionOutcomeHold, held.Outcome)
	require.Equal(t, protocol.DecisionReasonLinkHold, held.Reason)
	require.Equal(t, protocol.ExplanationScopeLink, held.Blocker)

	runE2ECLI(ctx, t, environment.env, &links, "ctl", "link", "set", "alpha", e2eBeta, "allow")
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, second.MessageID, pending.Messages[0].MessageID)
	secondGeneration := pending.Messages[0].DeliveryGeneration
	var readSecond protocol.LogicalMessage
	beta.callTool(t, "read_message", map[string]any{
		"id": second.MessageID, e2eGenerationField: secondGeneration,
	}, &readSecond)
	require.Equal(t, secondBody, readSecond.Body)
	beta.callTool(t, "ack_message", map[string]any{
		"id": second.MessageID, e2eGenerationField: secondGeneration, e2eStatusField: "ACKNOWLEDGED",
	}, &acked)
	require.True(t, acked["acked"])

	var released protocol.Explanation
	runE2ECLI(ctx, t, environment.env, &released,
		"ctl", "channel", "explain", "--message", string(second.MessageID))
	require.Equal(t, protocol.DecisionOutcomeAllow, released.Outcome)
	require.Equal(t, protocol.DecisionReasonHoldReleased, released.Reason)

	var retried protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "held", e2eBodyField: secondBody,
		e2eClientIDField: secondID,
	}, &retried)
	require.Equal(t, second.MessageID, retried.MessageID)
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	outside := startE2EMCP(ctx, t, environment.env)
	t.Cleanup(func() {
		_ = outside.stdin.Close()
		select {
		case <-outside.process.done:
		default:
			_ = outside.process.terminateAndWait()
		}
	})
	tools := outside.call(t, "tools/list", map[string]any{})
	require.Nil(t, tools.Error, "mcp stderr: %s", outside.stderr.String())
	var listed struct {
		Tools []json.RawMessage `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(tools.Result, &listed))
	require.Empty(t, listed.Tools)
	failed := outside.call(t, "tools/call", map[string]any{
		e2eNameField: "send_message", "arguments": map[string]any{
			e2eToField: e2eBeta, e2eSubjectField: "outside", e2eBodyField: "must fail",
		},
	})
	require.NotNil(t, failed.Error)
	require.Equal(t, -32001, failed.Error.Code)
	require.Equal(t, "No verified identity", failed.Error.Message)
	outside.stop(t)

	alpha.stop(t)
	beta.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(),
		"hub stderr: %s", hubStderr.String())
	requireNoE2ESockets(t, environment.runtimeDir)
}

func TestUnifiedNodeApprovalDeliversThroughRealProxy(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })

	recipient := startE2ERecipient(ctx, t, environment, "recipient", "quiet")
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})

	db := openE2EReadDB(t, environment.database)
	require.Eventually(t, func() bool {
		var connected int
		err := db.QueryRowContext(ctx, `SELECT count(*) FROM sessions
			WHERE agent_id = 'recipient' AND state = 'CONNECTED'
			AND capture_mode = 'pty_proxy' AND deliverable = 1 AND ended_at IS NULL`).Scan(&connected)
		return err == nil && connected == 1
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient session did not connect")
	// The measured prompt must be both visible and quiet before the manual
	// delivery; this also leaves time for the two-second Proxy reconciliation.
	time.Sleep(2300 * time.Millisecond)

	var admitted protocol.AdmissionResult
	sender.callTool(t, "send_message", map[string]any{
		e2eToField: "recipient", e2eSubjectField: "wiring",
		e2eBodyField: "real endpoint to real proxy", e2eClientIDField: "e2e-wiring-positive",
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	require.True(t, admitted.MessageID.Valid())

	var pending protocol.ApprovalList
	require.Eventually(t, func() bool {
		pending = protocol.ApprovalList{}
		if err := readE2ECLIJSON(ctx, environment.env, e2eMeasuredGiffordBinary,
			&pending, "ctl", "approvals", "--pending"); err != nil {
			return false
		}
		return len(pending.Approvals) == 1
	}, 12*time.Second, 50*time.Millisecond, "automatic worker did not open approval")
	approval := pending.Approvals[0]
	require.Equal(t, string(admitted.MessageID), approval.ObjectID)
	require.Equal(t, protocol.AgentID("recipient"), approval.AgentID)
	require.Equal(t, protocol.ApprovalStatePending, approval.State)

	var resolved protocol.ApprovalRequest
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &resolved,
		"ctl", "approve", string(approval.ApprovalID), "--deliver-now")
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Eventually(t, func() bool {
		return len(recipient.deliveredLines(t)) == 1
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient stderr: %s", recipient.stderr.String())
	require.Equal(t, []string{"[GIFFORD] 1 pending message"}, recipient.deliveredLines(t))

	var queueState, attemptState, orchestratorDecision, channelDecision string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT dq.state, da.state,
		da.orchestrator_decision_id, da.channel_decision_id
		FROM delivery_queue AS dq JOIN delivery_attempts AS da
		ON da.message_id = dq.message_id AND da.agent_id = dq.agent_id
		AND da.delivery_generation = dq.delivery_generation
		WHERE dq.message_id = ? AND dq.agent_id = 'recipient'
		ORDER BY da.created_at DESC LIMIT 1`, admitted.MessageID).Scan(
		&queueState, &attemptState, &orchestratorDecision, &channelDecision))
	require.Equal(t, "DELIVERED", queueState)
	require.Equal(t, "COMMITTED", attemptState)
	require.NotEmpty(t, orchestratorDecision)
	require.NotEmpty(t, channelDecision)

	sender.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func TestUnifiedNodeDefersUnsafeRealProcessTreesAndHumanDraft(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	tests := []struct {
		name       string
		mode       string
		humanDraft bool
		guardText  string
	}{
		{name: "human draft", mode: "quiet", humanDraft: true, guardText: "human draft is not CLOSED"},
		{name: "tool child", mode: "tool", guardText: "untrusted harness descendant present"},
		{name: "wrapper grandchild", mode: e2eWrapperMode, guardText: "untrusted harness descendant present"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			environment := newE2EEnvironment(t)
			hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
			t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
			agent := protocol.AgentID("recipient-" + strings.ReplaceAll(test.name, " ", "-"))
			recipient := startE2ERecipient(ctx, t, environment, agent, test.mode)
			t.Cleanup(func() {
				_ = recipient.process.terminateAndWait()
				_ = recipient.outer.Close()
			})
			sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
			t.Cleanup(func() {
				select {
				case <-sender.process.done:
				default:
					_ = sender.process.terminateAndWait()
				}
				_ = sender.outer.Close()
			})
			db := openE2EReadDB(t, environment.database)
			var draftState string
			require.Eventually(t, func() bool {
				return db.QueryRowContext(ctx, `SELECT draft_state FROM input_guards
					WHERE agent_id = ?`, agent).Scan(&draftState) == nil
			}, 12*time.Second, 20*time.Millisecond, "proxy never published a guard")
			if test.humanDraft {
				recipient.write(t, "human draft")
				require.Eventually(t, func() bool {
					return db.QueryRowContext(ctx, `SELECT draft_state FROM input_guards
						WHERE agent_id = ?`, agent).Scan(&draftState) == nil && draftState == "OPEN"
				}, e2eRPCTimeout, 20*time.Millisecond, "human draft never became OPEN")
			}
			admitted, approval := sendE2EMessageAndWaitApproval(
				ctx, t, environment, sender, agent, "e2e-defer-"+string(agent))

			stderr, approveErr := runE2ECLIError(ctx, environment.env, e2eMeasuredGiffordBinary,
				"ctl", "approve", string(approval.ApprovalID), "--deliver-now")
			require.Error(t, approveErr, "stderr: %s", stderr)
			var approvalState, queueState, selected, guards string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT state FROM approval_requests WHERE approval_id = ?`, approval.ApprovalID).
				Scan(&approvalState))
			require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM delivery_queue
				WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, agent).Scan(&queueState))
			require.NoError(t, db.QueryRowContext(ctx, `SELECT selected_action, hard_guards_json
				FROM orchestrator_decisions WHERE message_id = ? AND agent_id = ?
				ORDER BY rowid DESC LIMIT 1`, admitted.MessageID, agent).Scan(&selected, &guards))
			require.Equal(t, "PENDING", approvalState)
			require.Equal(t, "QUEUED", queueState)
			require.Equal(t, "DEFER", selected)
			require.Contains(t, guards, test.guardText)
			var attempts int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts
				WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, agent).Scan(&attempts))
			require.Zero(t, attempts)
			require.Empty(t, recipient.deliveredLines(t))

			sender.stop(t)
			_ = recipient.process.terminateAndWait()
			_ = recipient.outer.Close()
			require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
		})
	}
}

func TestUnifiedNodeNonQuietOutputAbortsAndRequeues(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	recipient := startE2ERecipient(ctx, t, environment, "recipient-noisy", "noisy")
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	db := openE2EReadDB(t, environment.database)
	require.Eventually(t, func() bool {
		var count int
		return db.QueryRowContext(ctx, `SELECT count(*) FROM input_guards
			WHERE agent_id = 'recipient-noisy'`).Scan(&count) == nil && count == 1
	}, 12*time.Second, 20*time.Millisecond, "proxy never published a guard")
	admitted, approval := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, "recipient-noisy", "e2e-nonquiet")

	var resolved protocol.ApprovalRequest
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &resolved,
		"ctl", "approve", string(approval.ApprovalID), "--deliver-now")
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	var attemptState, reason, queueState string
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, `SELECT state, terminal_reason FROM delivery_attempts
			WHERE message_id = ? AND agent_id = 'recipient-noisy'
			ORDER BY rowid DESC LIMIT 1`, admitted.MessageID).Scan(&attemptState, &reason)
		return err == nil && attemptState == "ABORTED_HUMAN_PRIORITY"
	}, e2eRPCTimeout, 20*time.Millisecond, "delivery attempt did not abort")
	require.Equal(t, "HUMAN_PRIORITY", reason)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = 'recipient-noisy'`, admitted.MessageID).Scan(&queueState))
	require.Equal(t, "QUEUED", queueState)
	var uncertain int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND state = 'UNCERTAIN'`, admitted.MessageID).Scan(&uncertain))
	require.Zero(t, uncertain)
	require.Empty(t, recipient.deliveredLines(t))

	sender.stop(t)
	_ = recipient.process.terminateAndWait()
	_ = recipient.outer.Close()
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func TestUnifiedNodePanelReadsRealStateAndUnavailableModel(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	recipient := startE2ERecipient(ctx, t, environment, e2ePanelAgent, "quiet")
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	client, err := tui.NewClient(tui.ClientOptions{
		SocketPath: filepath.Join(environment.runtimeDir, "daemon.sock"), Timeout: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var agents protocol.AgentListResult
		if client.Call(ctx, protocol.ControlMethodAgentList, protocol.AgentListParams{}, &agents) != nil {
			return false
		}
		for _, agent := range agents.Agents {
			if agent.AgentID == e2ePanelAgent && agent.Keyboard != nil {
				return true
			}
		}
		return false
	}, 12*time.Second, 20*time.Millisecond, "panel client never observed the recipient")
	admitted, _ := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, e2ePanelAgent, "e2e-panel")

	var agents protocol.AgentListResult
	require.NoError(t, client.Call(ctx, protocol.ControlMethodAgentList,
		protocol.AgentListParams{}, &agents))
	var panelAgent *protocol.AgentListItem
	for i := range agents.Agents {
		if agents.Agents[i].AgentID == e2ePanelAgent {
			panelAgent = &agents.Agents[i]
			break
		}
	}
	require.NotNil(t, panelAgent)
	require.Equal(t, protocol.SessionStateConnected, panelAgent.SessionState)
	require.True(t, panelAgent.Deliverable)
	require.NotNil(t, panelAgent.Keyboard)
	require.Equal(t, protocol.DraftStateClosed, panelAgent.Keyboard.DraftState)
	require.Equal(t, int64(1), panelAgent.Queue.Queued)

	var messages protocol.MessageListResult
	require.NoError(t, client.Call(ctx, protocol.ControlMethodMessageList,
		protocol.MessageListParams{AgentID: e2ePanelAgent}, &messages))
	require.Len(t, messages.Messages, 1)
	require.Equal(t, admitted.MessageID, messages.Messages[0].MessageID)
	require.Equal(t, protocol.DeliveryQueueState("QUEUED"), messages.Messages[0].State)

	var status protocol.ModelStatusResult
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &status,
		"ctl", "model", "status")
	require.Nil(t, status.ActiveProfile)
	require.Nil(t, status.LastCall)
	for _, metadata := range []protocol.ModelMetadata{
		status.Deployment.BaseRevision, status.Deployment.AdapterRevision,
		status.Deployment.DatasetVersion, status.Deployment.ABVariant,
	} {
		require.Equal(t, protocol.MetadataAvailabilityUnavailable, metadata.Availability)
		require.Empty(t, metadata.Value)
	}

	sender.stop(t)
	_ = recipient.process.terminateAndWait()
	_ = recipient.outer.Close()
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func TestUnifiedNodeRecoversCommitSentAsUncertainAndRejectsWithoutRedelivery(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 55*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, _ := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	recipient := startE2ERecipient(ctx, t, environment, "recipient-recovery", "quiet")
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	db := openE2EReadDB(t, environment.database)
	require.Eventually(t, func() bool {
		var count int
		return db.QueryRowContext(ctx, `SELECT count(*) FROM input_guards
			WHERE agent_id = 'recipient-recovery'`).Scan(&count) == nil && count == 1
	}, 12*time.Second, 20*time.Millisecond, "proxy never published a guard")
	time.Sleep(350 * time.Millisecond)
	admitted, approval := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, "recipient-recovery", "e2e-recovery")

	var approveOut, approveErrOut bytes.Buffer
	approveCmd := exec.CommandContext(ctx, e2eMeasuredGiffordBinary,
		"ctl", "approve", string(approval.ApprovalID), "--deliver-now")
	approveCmd.Env = environment.env
	approveCmd.Stdout = &approveOut
	approveCmd.Stderr = &approveErrOut
	approveProcess, err := startE2EProcess(approveCmd)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var state string
		return db.QueryRowContext(ctx, `SELECT state FROM delivery_attempts
			WHERE message_id = ? AND agent_id = 'recipient-recovery'
			ORDER BY rowid DESC LIMIT 1`, admitted.MessageID).Scan(&state) == nil &&
			state == "COMMIT_SENT"
	}, e2eRPCTimeout, 100*time.Microsecond, "attempt never exposed COMMIT_SENT")
	require.NoError(t, hubProcess.cmd.Process.Kill())
	require.Error(t, hubProcess.wait(), "SIGKILL unexpectedly looked clean")
	_ = approveProcess.wait()
	// A commit frame may already have crossed the socket. UNCERTAIN means the
	// node must not guess either way; record the observed line count as the
	// ceiling that reject must preserve.
	time.Sleep(350 * time.Millisecond)
	linesBeforeRestart := append([]string(nil), recipient.deliveredLines(t)...)

	restartedHub, restartedStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = restartedHub.terminateAndWait() })
	var attemptState, queueState string
	var uncertainApproval sql.NullString
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, `SELECT da.state, da.uncertain_approval_id, dq.state
			FROM delivery_attempts AS da JOIN delivery_queue AS dq
			ON dq.message_id = da.message_id AND dq.agent_id = da.agent_id
			AND dq.delivery_generation = da.delivery_generation
			WHERE da.message_id = ? AND da.agent_id = 'recipient-recovery'
			ORDER BY da.rowid DESC LIMIT 1`, admitted.MessageID).Scan(
			&attemptState, &uncertainApproval, &queueState)
		return err == nil && attemptState == "UNCERTAIN" && uncertainApproval.Valid
	}, e2eRPCTimeout, 20*time.Millisecond, "restart did not recover COMMIT_SENT")
	require.Equal(t, "QUEUED", queueState)

	var pending protocol.ApprovalList
	require.NoError(t, readE2ECLIJSON(ctx, environment.env, e2eMeasuredGiffordBinary,
		&pending, "ctl", "approvals", "--pending"))
	var recoveryApproval protocol.ApprovalRequest
	for _, candidate := range pending.Approvals {
		if candidate.ApprovalID == protocol.ApprovalID(uncertainApproval.String) {
			recoveryApproval = candidate
			break
		}
	}
	require.Equal(t, string(admitted.MessageID), recoveryApproval.ObjectID)
	require.Equal(t, protocol.AgentID("recipient-recovery"), recoveryApproval.AgentID)
	stderr, deliverErr := runE2ECLIError(ctx, environment.env, e2eMeasuredGiffordBinary,
		"ctl", "approve", string(recoveryApproval.ApprovalID), "--deliver-now")
	require.Error(t, deliverErr, "stderr: %s", stderr)
	require.Contains(t, stderr, "Delivery deferred: RECIPIENT_NOT_DELIVERABLE")
	var stillPending string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM approval_requests
		WHERE approval_id = ?`, recoveryApproval.ApprovalID).Scan(&stillPending))
	require.Equal(t, "PENDING", stillPending)
	time.Sleep(350 * time.Millisecond)
	require.Equal(t, linesBeforeRestart, recipient.deliveredLines(t),
		"blocked uncertain deliver_now must not emit a byte")
	var attemptsBeforeReject int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = 'recipient-recovery'`, admitted.MessageID).
		Scan(&attemptsBeforeReject))
	require.Equal(t, 1, attemptsBeforeReject)

	var rejected protocol.ApprovalRequest
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &rejected,
		"ctl", "reject", string(recoveryApproval.ApprovalID), "--reason", "operator refuses uncertain retry")
	require.Equal(t, protocol.ApprovalStateRejected, rejected.State)
	var resolution sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT da.resolution_approval_id, dq.state
		FROM delivery_attempts AS da JOIN delivery_queue AS dq
		ON dq.message_id = da.message_id AND dq.agent_id = da.agent_id
		AND dq.delivery_generation = da.delivery_generation
		WHERE da.message_id = ? AND da.agent_id = 'recipient-recovery'
		ORDER BY da.rowid DESC LIMIT 1`, admitted.MessageID).Scan(&resolution, &queueState))
	require.Equal(t, string(recoveryApproval.ApprovalID), resolution.String)
	require.Equal(t, "CANCELLED", queueState)
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, linesBeforeRestart, recipient.deliveredLines(t),
		"reject must not emit a recovery delivery")
	var attempts int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = 'recipient-recovery'`, admitted.MessageID).Scan(&attempts))
	require.Equal(t, 1, attempts)

	sender.stop(t)
	_ = recipient.process.terminateAndWait()
	_ = recipient.outer.Close()
	require.NoError(t, restartedHub.terminateAndWait(), "hub stderr: %s", restartedStderr.String())
}

func TestUnifiedNodeKeepsEndpointRequestsRecipientScoped(t *testing.T) {
	if runtime.GOOS != e2eLinux {
		t.Skip("requires Linux PTYs, /proc and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	firstRecipient := startE2ERecipient(ctx, t, environment, "recipient-one", "quiet")
	secondRecipient := startE2ERecipient(ctx, t, environment, "recipient-two", "quiet")
	for _, recipient := range []*e2eRecipient{firstRecipient, secondRecipient} {
		t.Cleanup(func() {
			_ = recipient.process.terminateAndWait()
			_ = recipient.outer.Close()
		})
	}
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	db := openE2EReadDB(t, environment.database)
	require.Eventually(t, func() bool {
		var count int
		return db.QueryRowContext(ctx, `SELECT count(*) FROM input_guards
			WHERE agent_id IN ('recipient-one', 'recipient-two')`).Scan(&count) == nil && count == 2
	}, 12*time.Second, 20*time.Millisecond, "both proxies never published guards")
	time.Sleep(350 * time.Millisecond)
	firstMessage, firstApproval := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, "recipient-one", "e2e-recipient-one")
	secondMessage, secondApproval := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, "recipient-two", "e2e-recipient-two")
	require.NotEqual(t, firstMessage.MessageID, secondMessage.MessageID)
	require.NotEqual(t, firstApproval.ApprovalID, secondApproval.ApprovalID)
	require.Equal(t, protocol.AgentID("recipient-one"), firstApproval.AgentID)
	require.Equal(t, protocol.AgentID("recipient-two"), secondApproval.AgentID)

	var rejected protocol.ApprovalRequest
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &rejected,
		"ctl", "reject", string(firstApproval.ApprovalID), "--reason", "recipient-one test cleanup")
	require.Equal(t, protocol.ApprovalStateRejected, rejected.State)
	var firstState, secondState string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM approval_requests
		WHERE approval_id = ?`, firstApproval.ApprovalID).Scan(&firstState))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM approval_requests
		WHERE approval_id = ?`, secondApproval.ApprovalID).Scan(&secondState))
	require.Equal(t, "REJECTED", firstState)
	require.Equal(t, "PENDING", secondState, "resolving one recipient must not resolve the other")
	runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &rejected,
		"ctl", "reject", string(secondApproval.ApprovalID), "--reason", "recipient-two test cleanup")

	sender.stop(t)
	for _, recipient := range []*e2eRecipient{firstRecipient, secondRecipient} {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	}
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}
