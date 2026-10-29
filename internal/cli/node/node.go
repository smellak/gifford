// Package node implements the `gifford node` subcommand tree. Every database
// mutation is sent to the running hub over daemon.sock; the CLI never opens
// gifford.db.
package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/xdg"
)

var errHubNotRunning = errors.New("gifford node: hub is not running")

const (
	maxJoinTokenBytes = 128
	joinRestartNotice = "gifford node join: restart the hub to activate federation"
)

// NewCmd returns the gifford node command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Group("node", "Identities, peers, certificates and policies",
		newEnrollTokenCmd(), newJoinCmd(), newPeersCmd(), newRevokeCmd(),
		deps.Leaf("keys", "Manage this node's signing and encryption keys"),
	)
}

func newEnrollTokenCmd() *cobra.Command {
	var organizationID string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "enroll-token",
		Short: "Create a single-use, short-lived enrollment token for a new peer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if ttl <= 0 || ttl > registry.EnrollmentTokenTTL || ttl%time.Millisecond != 0 {
				return cliError(fmt.Errorf("ttl must be between 1ms and %s", registry.EnrollmentTokenTTL))
			}
			ttlMS := ttl.Milliseconds()
			var result protocol.NodeEnrollTokenResult
			if err := call(cmd.Context(), protocol.ControlMethodNodeEnrollToken,
				protocol.NodeEnrollTokenParams{OrganizationID: organizationID, TTLMS: &ttlMS}, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
	cmd.Flags().StringVar(&organizationID, "org", "", "organization allowed to use the token (default: local organization)")
	cmd.Flags().DurationVar(&ttl, "ttl", registry.EnrollmentTokenTTL, "token lifetime (maximum 15m)")
	return cmd
}

func newJoinCmd() *cobra.Command {
	var fingerprint, tokenFile string
	var tokenFD int
	cmd := &cobra.Command{
		Use:   "join <url>",
		Short: "Join a federation using an enrollment token from a peer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fingerprint == "" {
				return cliError(fmt.Errorf("--fingerprint is required"))
			}
			connection, err := dialControl(cmd.Context())
			if err != nil {
				return err
			}
			defer func() { _ = connection.Close() }()
			token, err := readJoinToken(cmd, tokenFile, tokenFD)
			if err != nil {
				return cliError(err)
			}
			var result protocol.NodeJoinResult
			if err := callConnection(cmd.Context(), connection, protocol.ControlMethodNodeJoin, protocol.NodeJoinParams{
				URL: args[0], Token: token, Fingerprint: protocol.SHA256Hex(fingerprint),
			}, &result); err != nil {
				return err
			}
			if err := printJSON(cmd, result); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(cmd.ErrOrStderr(), joinRestartNotice); err != nil {
				return cliError(err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&fingerprint, "fingerprint", "", "accepting node certificate SHA-256 fingerprint")
	cmd.Flags().StringVar(&tokenFile, "token-file", "", "read the enrollment token from a mode-0600 file")
	cmd.Flags().IntVar(&tokenFD, "token-fd", -1, "read the enrollment token from an already-open file descriptor")
	cmd.MarkFlagsMutuallyExclusive("token-file", "token-fd")
	return cmd
}

func newPeersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "peers",
		Short: "List known federation peers and their status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var result protocol.NodePeersResult
			if err := call(cmd.Context(), protocol.ControlMethodNodePeers, protocol.EmptyParams{}, &result); err != nil {
				return err
			}
			type peerView struct {
				NodeID         protocol.NodeID `json:"node_id"`
				OrganizationID string          `json:"organization_id"`
				Status         string          `json:"status"`
				Endpoint       string          `json:"endpoint,omitempty"`
				Fingerprint    string          `json:"fingerprint,omitempty"`
			}
			peers := make([]peerView, 0, len(result.Peers))
			for _, peer := range result.Peers {
				peers = append(peers, peerView{
					NodeID: peer.NodeID, OrganizationID: peer.OrganizationID,
					Status: peer.Status, Endpoint: peer.Endpoint,
					Fingerprint: abbreviatedFingerprint(string(peer.Fingerprint)),
				})
			}
			return printJSON(cmd, peers)
		},
	}
}

func newRevokeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke <node>",
		Short: "Revoke a peer or its credentials",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var result protocol.NodeRevokeResult
			if err := call(cmd.Context(), protocol.ControlMethodNodeRevoke,
				protocol.NodeRevokeParams{NodeID: protocol.NodeID(args[0])}, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}

func call(ctx context.Context, method protocol.ControlMethod, params, result any) error {
	connection, err := dialControl(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	return callConnection(ctx, connection, method, params, result)
}

func dialControl(ctx context.Context) (net.Conn, error) {
	dirs, err := xdg.Resolve()
	if err != nil {
		return nil, cliError(err)
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dirs.Runtime, "daemon.sock"))
	if err != nil {
		return nil, cliError(errHubNotRunning)
	}
	return connection, nil
}

func callConnection(
	ctx context.Context,
	connection net.Conn,
	method protocol.ControlMethod,
	params, result any,
) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return cliError(fmt.Errorf("set node control deadline: %w", err))
		}
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		_ = connection.SetDeadline(time.Unix(1, 0))
	})
	defer func() {
		if !stopCancel() {
			<-cancelDone
		}
		_ = connection.SetDeadline(time.Time{})
	}()
	rawParams, err := json.Marshal(params)
	if err != nil {
		return cliError(err)
	}
	request := protocol.RPCRequest{
		JSONRPC: protocol.JSONRPCVersion, ID: 1, Method: method, Params: protocol.Payload(rawParams),
	}
	if err := request.Validate(); err != nil {
		return cliError(err)
	}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		if ctx.Err() != nil {
			return cliError(ctx.Err())
		}
		return cliError(fmt.Errorf("write node control request: %w", err))
	}
	line, err := bufio.NewReader(connection).ReadBytes('\n')
	if err != nil {
		if ctx.Err() != nil {
			return cliError(ctx.Err())
		}
		return cliError(fmt.Errorf("read node control response: %w", err))
	}
	var response protocol.RPCResponse
	if err := json.Unmarshal(line, &response); err != nil {
		return cliError(fmt.Errorf("decode node control response: %w", err))
	}
	if err := response.Validate(); err != nil {
		return cliError(fmt.Errorf("invalid node control response: %w", err))
	}
	if response.Error != nil {
		return cliError(errors.New(response.Error.Message))
	}
	if err := json.Unmarshal(response.Result, result); err != nil {
		return cliError(fmt.Errorf("decode node control result: %w", err))
	}
	return nil
}

func readJoinToken(cmd *cobra.Command, path string, descriptor int) (string, error) {
	if descriptor < 0 && cmd.Flags().Changed("token-fd") {
		return "", fmt.Errorf("invalid enrollment token descriptor")
	}
	if path != "" {
		return readJoinTokenFile(path)
	}
	if descriptor >= 0 {
		return readJoinTokenDescriptor(descriptor)
	}
	return decodeJoinToken(cmd.InOrStdin())
}

func readJoinTokenFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open enrollment token file: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat enrollment token file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return "", fmt.Errorf("enrollment token file must be regular with mode 0600")
	}
	return decodeJoinToken(file)
}

func readJoinTokenDescriptor(descriptor int) (string, error) {
	file := os.NewFile(uintptr(descriptor), "enrollment-token")
	if file == nil {
		return "", fmt.Errorf("invalid enrollment token descriptor")
	}
	defer func() { _ = file.Close() }()
	return decodeJoinToken(file)
}

func decodeJoinToken(reader io.Reader) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maxJoinTokenBytes+2))
	if err != nil {
		return "", fmt.Errorf("read enrollment token: %w", err)
	}
	raw = bytes.TrimSuffix(raw, []byte{'\n'})
	raw = bytes.TrimSuffix(raw, []byte{'\r'})
	if len(raw) == 0 || len(raw) > maxJoinTokenBytes || bytes.ContainsAny(raw, " \t\r\n") {
		return "", fmt.Errorf("invalid enrollment token input")
	}
	return string(raw), nil
}

func printJSON(cmd *cobra.Command, value any) error {
	encoder := json.NewEncoder(cmd.OutOrStdout())
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return cliError(err)
	}
	return nil
}

func abbreviatedFingerprint(value string) string {
	const length = 12
	if len(value) <= length {
		return value
	}
	return value[:length]
}

func cliError(err error) error { return &deps.ExitError{Code: exitcode.Error, Err: err} }
