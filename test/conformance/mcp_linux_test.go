//go:build conformance && linux

package conformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/term"
)

const conformanceJSONRPCVersion = "2.0"

type harnessCommand struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func runSyntheticChild() int {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	signal.Stop(stop)
	return 0
}

func runSyntheticHarness(mutant string) int {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return 2
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()

	var child *managedProcess
	var cancel context.CancelFunc
	if mutant == mutantChild {
		var childContext context.Context
		childContext, cancel = context.WithCancel(context.Background())
		cmd := exec.CommandContext(childContext, os.Args[0])
		cmd.Env = setEnv(os.Environ(), syntheticChildEnv, "1")
		child, err = startManagedProcess(cmd)
		if err != nil {
			cancel()
			return 2
		}
		defer func() {
			cancel()
			_ = child.terminate()
		}()
	}

	draft := make([]byte, 0, 256)
	redraw := func() bool {
		visible := draft
		if mutant == mutantNoEcho {
			visible = nil
		}
		_, writeErr := fmt.Fprintf(os.Stdout, "\x1b[?2026h\r\x1b[2Ke2e> %s\x1b[?2026l", visible)
		return writeErr == nil
	}
	if !redraw() {
		return 2
	}
	buffer := make([]byte, 256)
	for {
		count, readErr := os.Stdin.Read(buffer)
		for _, value := range buffer[:count] {
			switch value {
			case '\r', '\n':
				if mutant == mutantIgnoreSubmit {
					break
				}
				if len(draft) > 0 {
					visible := draft
					if mutant == mutantNoEcho {
						visible = nil
					}
					if _, err := fmt.Fprintf(os.Stdout, "\r\x1b[2Ke2e> %s\r\n", visible); err != nil {
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
				if value >= 0x20 {
					draft = append(draft, value)
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

func runSenderHarness() int {
	binary := os.Getenv(senderBinaryEnv)
	if binary == "" {
		return 2
	}
	cmd := exec.CommandContext(context.Background(), binary, "mcp")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 2
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 2
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 2
	}
	reader := bufio.NewReader(stdout)
	nextID := int64(1)
	initialize, _ := json.Marshal(map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "gifford-conformance", "version": "1"},
	})
	if _, err := callMCP(stdin, reader, nextID, "initialize", initialize); err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		return 2
	}
	nextID++
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		var command harnessCommand
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, `{"harness_error":%q}`+"\n", err.Error())
			continue
		}
		if command.Method == "exit" {
			_ = stdin.Close()
			if err := cmd.Wait(); err != nil {
				return 2
			}
			_, _ = fmt.Fprintln(os.Stdout, `{"stopped":true}`)
			return 0
		}
		response, err := callMCP(stdin, reader, nextID, command.Method, command.Params)
		if err != nil {
			_ = stdin.Close()
			_ = cmd.Wait()
			return 2
		}
		nextID++
		_, _ = os.Stdout.Write(response)
		_, _ = os.Stdout.Write([]byte{'\n'})
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		return 2
	}
	return 0
}

func callMCP(
	stdin io.Writer,
	reader *bufio.Reader,
	id int64,
	method string,
	params json.RawMessage,
) ([]byte, error) {
	request := rpcRequest{JSONRPC: conformanceJSONRPCVersion, ID: id, Method: method, Params: params}
	if err := json.NewEncoder(stdin).Encode(request); err != nil {
		return nil, err
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(line), nil
}

type senderAgent struct {
	process *managedProcess
	outer   *os.File
	reader  *bufio.Reader
}

func startSenderAgent(
	ctx context.Context,
	config *suiteConfig,
	environment privateEnvironment,
) (*senderAgent, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate sender harness: %w", err)
	}
	outer, slave, err := pty.Open()
	if err != nil {
		return nil, fmt.Errorf("open sender PTY: %w", err)
	}
	if err := pty.Setsize(outer, &pty.Winsize{Rows: 40, Cols: 120}); err != nil {
		_ = outer.Close()
		_ = slave.Close()
		return nil, fmt.Errorf("size sender PTY: %w", err)
	}
	cmd := exec.CommandContext(ctx, config.gifford,
		"run", "--agent", conformanceSender, "--harness", genericHarness, "--", executable)
	cmd.Env = setEnv(environment.env, senderModeEnv, "1")
	cmd.Env = setEnv(cmd.Env, senderBinaryEnv, config.gifford)
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = io.Discard
	process, err := startManagedProcess(cmd)
	_ = slave.Close()
	if err != nil {
		_ = outer.Close()
		return nil, fmt.Errorf("start sender: %w", err)
	}
	return &senderAgent{process: process, outer: outer, reader: bufio.NewReader(outer)}, nil
}

func (agent *senderAgent) callTool(ctx context.Context, name string, arguments, result any) error {
	params, err := json.Marshal(map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		return fmt.Errorf("encode tool arguments: %w", err)
	}
	rawCommand, err := json.Marshal(harnessCommand{
		Method: "tools/call",
		Params: params,
	})
	if err != nil {
		return fmt.Errorf("encode sender command: %w", err)
	}
	if err := agent.outer.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return fmt.Errorf("set sender deadline: %w", err)
	}
	if _, err := agent.outer.Write(append(rawCommand, '\n')); err != nil {
		return fmt.Errorf("write sender command: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := agent.reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read sender response: %w", err)
		}
		line = strings.TrimSpace(line)
		start, end := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
		if start < 0 || end < start {
			continue
		}
		var response rpcResponse
		if json.Unmarshal([]byte(line[start:end+1]), &response) != nil || response.JSONRPC != conformanceJSONRPCVersion {
			continue
		}
		if response.Error != nil {
			return fmt.Errorf("sender tool %s failed with code %d", name, response.Error.Code)
		}
		return decodeToolResult(response.Result, result)
	}
}

func decodeToolResult(raw json.RawMessage, result any) error {
	var tool struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &tool); err != nil {
		return fmt.Errorf("decode tool result: %w", err)
	}
	if len(tool.Content) != 1 || tool.Content[0].Type != "text" {
		return errors.New("sender tool returned an unexpected content shape")
	}
	if err := json.Unmarshal([]byte(tool.Content[0].Text), result); err != nil {
		return fmt.Errorf("decode tool text: %w", err)
	}
	return nil
}

func (agent *senderAgent) stop() error {
	if agent == nil {
		return nil
	}
	raw, err := json.Marshal(harnessCommand{Method: "exit"})
	if err == nil {
		_, err = agent.outer.Write(append(raw, '\n'))
	}
	if err == nil {
		err = agent.process.wait(10 * time.Second)
	}
	if err != nil {
		err = errors.Join(err, agent.process.terminate())
	}
	closeErr := agent.outer.Close()
	return errors.Join(err, closeErr)
}
