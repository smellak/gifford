package ctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/protocol"
)

const modelStatusUse = "status"

// newModelCmd returns the `gifford ctl model` group: inspect the active model
// adapter.
func newModelCmd() *cobra.Command {
	return deps.Group("model", "Inspect the active model adapter",
		newModelStatusCmd(),
		newModelRegisterCmd(),
		newModelActivateCmd(),
		newModelRollbackCmd(),
	)
}

func newModelActivateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "activate <adapter> <revision>",
		Short: "Activate a registered adapter revision whose complete gate passed",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			params := protocol.ActivateAdapterRequest{AdapterName: args[0], AdapterRevision: args[1]}
			var result protocol.ModelActivationResult
			if err := call(cmd, protocol.ControlMethodModelActivate, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}

func newModelRollbackCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rollback",
		Short: "Roll back ANALYZE_AGENT_STATE to the complete base tuple immediately",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.RollbackToBaseRequest{Contract: protocol.ModelContractAnalyzeAgentState}
			var result protocol.ModelActivationResult
			if err := call(cmd, protocol.ControlMethodModelRollback, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}

func newModelRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register <metadata.json>",
		Short: "Import and bind one sealed metadata-only adapter record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return fmt.Errorf("open model metadata: %w", err)
			}
			defer func() { _ = file.Close() }()
			data, err := readModelMetadata(file)
			if err != nil {
				return err
			}
			var params protocol.RegisterModelMetadataRequest
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&params); err != nil {
				return fmt.Errorf("decode model metadata: invalid JSON document")
			}
			if err := dec.Decode(&struct{}{}); err != io.EOF {
				return fmt.Errorf("decode model metadata: invalid JSON document")
			}
			var result protocol.ModelMetadataRegistrationResult
			if err := call(cmd, protocol.ControlMethodModelRegister, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}

func readModelMetadata(file *os.File) ([]byte, error) {
	const max = 1 << 20
	data, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, fmt.Errorf("read model metadata: %w", err)
	}
	if len(data) > max {
		return nil, fmt.Errorf("read model metadata: document exceeds %d bytes", max)
	}
	return data, nil
}

func newModelStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   modelStatusUse,
		Short: "Show the observed model profile and deployment metadata",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var result protocol.ModelStatusResult
			if err := call(cmd, protocol.ControlMethodModelStatus, protocol.EmptyParams{}, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}
