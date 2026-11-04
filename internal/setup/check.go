package setup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"
)

// ErrHubUnavailable means the MCP server could not reach a running node.
var ErrHubUnavailable = errors.New("no running Gifford node: start it with gifford up, then run gifford setup --check")

const checkRequests = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"gifford-setup","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
`

// CheckMCP starts the MCP server exactly as the harnesses do and requires
// it to answer initialize and to list Gifford's four message tools.
func CheckMCP(ctx context.Context, gifford string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, gifford, "mcp")
	cmd.Stdin = strings.NewReader(checkRequests)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	tools, found := listedTools(out)
	if found {
		var missing []string
		for _, tool := range MessageTools {
			if !slices.Contains(tools, tool) {
				missing = append(missing, tool)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("gifford mcp answered without %s", strings.Join(missing, ", "))
		}
		return nil
	}
	if strings.Contains(stderr.String(), "hub unavailable") {
		return ErrHubUnavailable
	}
	if err != nil {
		return fmt.Errorf("gifford mcp did not answer: %w: %s", err, firstLine(stderr.Bytes()))
	}
	return errors.New("gifford mcp did not list its tools")
}

func listedTools(out []byte) ([]string, bool) {
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for scanner.Scan() {
		var frame struct {
			ID     json.Number `json:"id"`
			Result struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			} `json:"result"`
		}
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.ID.String() != "2" {
			continue
		}
		names := make([]string, 0, len(frame.Result.Tools))
		for _, tool := range frame.Result.Tools {
			names = append(names, tool.Name)
		}
		return names, true
	}
	return nil, false
}
