package adapters

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultProbeTimeout        = 5 * time.Second
	defaultVersionProbeTimeout = 2 * time.Second
	maxHarnessProtocolMessage  = 1 << 20
	codexAppServerDocs         = "https://developers.openai.com/codex/app-server"
)

var dottedVersion = regexp.MustCompile(`\d+(\.\d+)+`)

// ProbeOptions identifies the harness binary to inspect. Binary and Harness
// are supplied by the operator; the probe never guesses an installed harness.
type ProbeOptions struct {
	Binary         string
	Harness        protocol.Harness
	Timeout        time.Duration
	VersionTimeout time.Duration
}

// ProbeResult is the operator-facing result of inspecting one documented
// harness channel. Verifiable means the channel initialized successfully.
// Attachable means that channel can attach to an existing interactive CLI
// session, rather than starting its own harness process.
type ProbeResult struct {
	Harness       protocol.Harness      `json:"harness"`
	Version       string                `json:"version,omitempty"`
	Channel       string                `json:"channel,omitempty"`
	Verifiable    bool                  `json:"verifiable"`
	Attachable    bool                  `json:"attachable"`
	Capabilities  protocol.Capabilities `json:"capabilities"`
	Verdict       string                `json:"verdict"`
	Documentation string                `json:"documentation,omitempty"`
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

// ProbeHarness inspects one operator-selected harness binary without starting
// a model turn. Unsupported, unavailable or incomplete channels return the
// conservative "sin canal verificable" result.
func ProbeHarness(ctx context.Context, opts ProbeOptions) ProbeResult {
	result := ProbeResult{Harness: opts.Harness, Verdict: "sin canal verificable"}
	if opts.Binary == "" || !opts.Harness.Valid() {
		return result
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultProbeTimeout
	}
	if opts.VersionTimeout <= 0 {
		opts.VersionTimeout = defaultVersionProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	resolved, err := exec.LookPath(opts.Binary)
	if err != nil {
		return result
	}
	result.Version = probeHarnessVersion(probeCtx, resolved, opts.VersionTimeout)
	if opts.Harness != protocol.HarnessCodex {
		return result
	}
	result.Documentation = codexAppServerDocs
	userAgent, err := initializeCodexAppServer(probeCtx, resolved)
	if err != nil {
		return result
	}
	if result.Version == "" {
		result.Version = parseHarnessVersion(userAgent)
	}
	if result.Version == "" {
		return result
	}
	result.Channel = "codex app-server JSONL/stdio"
	result.Verifiable = true
	result.Capabilities.StructuredEvents = true
	result.Verdict = "version verificada y capabilities; sin adjuncion documentada a una sesion CLI interactiva"
	return result
}

func probeHarnessVersion(ctx context.Context, binary string, timeout time.Duration) string {
	versionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(versionCtx, binary, "--version") // #nosec G204,G702 -- operator supplies the harness path
	output, err := command.CombinedOutput()
	if err != nil {
		return ""
	}
	return parseHarnessVersion(string(output))
}

func parseHarnessVersion(output string) string { return dottedVersion.FindString(output) }

func initializeCodexAppServer(ctx context.Context, binary string) (string, error) {
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
	scanner.Buffer(make([]byte, 4096), maxHarnessProtocolMessage)
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
