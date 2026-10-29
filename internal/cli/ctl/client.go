package ctl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

type rpcCallError struct{ rpc protocol.RPCError }

func (e *rpcCallError) Error() string {
	if e.rpc.Data == nil {
		return e.rpc.Message
	}
	return fmt.Sprintf("%s: %s", e.rpc.Message, e.rpc.Data.ErrorCode)
}

func call(cmd *cobra.Command, method protocol.ControlMethod, params, result any) error {
	dirs, err := xdg.Resolve()
	if err != nil {
		return cliError(exitcode.Error, err)
	}
	conn, err := (&net.Dialer{}).DialContext(cmd.Context(), "unix", filepath.Join(dirs.Runtime, "daemon.sock"))
	if err != nil {
		return cliError(exitcode.Error, fmt.Errorf("connect to control socket: %w", err))
	}
	defer conn.Close()
	rawParams, err := json.Marshal(params)
	if err != nil {
		return cliError(exitcode.Error, err)
	}
	req := protocol.RPCRequest{JSONRPC: protocol.JSONRPCVersion, ID: 1, Method: method, Params: protocol.Payload(rawParams)}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return cliError(exitcode.Error, fmt.Errorf("write control request: %w", err))
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return cliError(exitcode.Error, fmt.Errorf("read control response: %w", err))
	}
	var response protocol.RPCResponse
	if err := json.Unmarshal(line, &response); err != nil {
		return cliError(exitcode.Error, fmt.Errorf("decode control response: %w", err))
	}
	if err := response.Validate(); err != nil {
		return cliError(exitcode.Error, fmt.Errorf("invalid control response: %w", err))
	}
	if response.Error != nil {
		code := exitcode.Error
		if response.Error.Data != nil && response.Error.Data.ErrorCode == protocol.ErrorCodePolicyDenied {
			code = exitcode.Refused
		}
		return cliError(code, &rpcCallError{rpc: *response.Error})
	}
	if err := json.Unmarshal(response.Result, result); err != nil {
		return cliError(exitcode.Error, fmt.Errorf("decode control result: %w", err))
	}
	return nil
}

func printJSON(cmd *cobra.Command, value any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		return cliError(exitcode.Error, err)
	}
	return nil
}

func cliError(code int, err error) error { return &deps.ExitError{Code: code, Err: err} }
