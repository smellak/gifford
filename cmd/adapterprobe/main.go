// Command adapterprobe verifies the documented Codex app-server channel of an
// installed Codex binary without starting a model turn.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	probeTimeout       = 5 * time.Second
	versionTimeout     = 2 * time.Second
	maxProtocolMessage = 1 << 20
	codexAppServerDocs = "https://developers.openai.com/codex/app-server"
)

var dottedVersion = regexp.MustCompile(`\d+(\.\d+)+`)

// verdict is the operator-facing result. Verifiable means the documented
// initialize handshake succeeded against this binary. Attachable stays false
// because app-server is its own process; the official protocol documents no
// way to attach it to an already-running interactive Codex CLI session.
type verdict struct {
	Harness       protocol.Harness      `json:"harness"`
	Version       string                `json:"version,omitempty"`
	Channel       string                `json:"channel,omitempty"`
	Verifiable    bool                  `json:"verifiable"`
	Attachable    bool                  `json:"attachable"`
	Capabilities  protocol.Capabilities `json:"capabilities"`
	Verdict       string                `json:"verdict"`
	Documentation string                `json:"documentation"`
}

type initializeResponse struct {
	ID     int64 `json:"id"`
	Result *struct {
		UserAgent      string `json:"userAgent"`
		PlatformFamily string `json:"platformFamily"`
		PlatformOS     string `json:"platformOs"`
	} `json:"result"`
	Error *struct {
		Code int `json:"code"`
	} `json:"error"`
}

func main() {
	code := runMain(os.Args[1:], os.Stdout, os.Stderr)
	if code != 0 {
		os.Exit(code) //nolint:forbidigo // command entrypoint owns its exit status
	}
}

func runMain(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: adapterprobe <path-to-codex>")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	result := probe(ctx, args[0])
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		_, _ = fmt.Fprintf(stderr, "adapterprobe: write verdict: %v\n", err)
		return 1
	}
	return 0
}

func probe(ctx context.Context, binary string) verdict {
	result := verdict{
		Harness:       protocol.HarnessCodex,
		Verdict:       "sin canal verificable",
		Documentation: codexAppServerDocs,
	}
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return result
	}
	result.Version = probeVersion(ctx, resolved)
	userAgent, err := initializeAppServer(ctx, resolved)
	if err != nil {
		return result
	}
	if result.Version == "" {
		result.Version = parseVersion(userAgent)
	}
	if result.Version == "" {
		return result
	}
	result.Channel = "codex app-server JSONL/stdio"
	result.Verifiable = true
	result.Capabilities.StructuredEvents = true
	result.Verdict = "canal verificable; sin adjuncion documentada a una sesion CLI interactiva"
	return result
}

func probeVersion(ctx context.Context, binary string) string {
	versionCtx, cancel := context.WithTimeout(ctx, versionTimeout)
	defer cancel()
	command := exec.CommandContext(versionCtx, binary, "--version") // #nosec G204,G702 -- operator supplies the harness path
	output, err := command.CombinedOutput()
	if err != nil {
		return ""
	}
	return parseVersion(string(output))
}

func parseVersion(output string) string { return dottedVersion.FindString(output) }

func initializeAppServer(ctx context.Context, binary string) (string, error) {
	commandCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	command := exec.CommandContext(commandCtx, binary, "app-server", "--listen", "stdio://") // #nosec G204,G702 -- operator supplies the harness path
	stdin, err := command.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("stdin: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("stdout: %w", err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return "", fmt.Errorf("start app-server: %w", err)
	}
	waited := false
	defer func() {
		_ = stdin.Close()
		cancel()
		if !waited {
			_ = command.Wait()
		}
	}()
	encoder := json.NewEncoder(stdin)
	if err := encoder.Encode(map[string]any{
		"method": "initialize",
		"id":     1,
		"params": map[string]any{
			"clientInfo": map[string]string{
				"name":    "gifford_adapterprobe",
				"title":   "Gifford Adapter Probe",
				"version": "0.1.0",
			},
		},
	}); err != nil {
		return "", fmt.Errorf("write initialize: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), maxProtocolMessage)
	for scanner.Scan() {
		var response initializeResponse
		if json.Unmarshal(scanner.Bytes(), &response) != nil || response.ID != 1 {
			continue
		}
		if response.Error != nil {
			return "", fmt.Errorf("initialize error %d", response.Error.Code)
		}
		if response.Result == nil || strings.TrimSpace(response.Result.UserAgent) == "" ||
			strings.TrimSpace(response.Result.PlatformFamily) == "" || strings.TrimSpace(response.Result.PlatformOS) == "" {
			return "", errors.New("initialize result incomplete")
		}
		if err := encoder.Encode(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
			return "", fmt.Errorf("write initialized: %w", err)
		}
		_ = stdin.Close()
		_ = command.Wait()
		waited = true
		return response.Result.UserAgent, nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read initialize: %w", err)
	}
	if ctx.Err() != nil {
		return "", fmt.Errorf("initialize: %w", ctx.Err())
	}
	return "", errors.New("app-server closed before initialize")
}
