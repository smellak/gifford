package ctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
		newModelRegisterPipelineCmd(),
		newModelActivateCmd(),
		newModelRollbackCmd(),
	)
}

func newModelRegisterPipelineCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register-pipeline <evidence.json>",
		Short: "Verify pipeline evidence, compute its gate, and register only a pass",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return fmt.Errorf("open model pipeline evidence: %w", err)
			}
			defer func() { _ = file.Close() }()
			data, err := readModelMetadata(file)
			if err != nil {
				return err
			}
			var params protocol.RegisterModelPipelineRequest
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&params); err != nil {
				return fmt.Errorf("decode model pipeline evidence: invalid JSON document")
			}
			if err := dec.Decode(&struct{}{}); err != io.EOF {
				return fmt.Errorf("decode model pipeline evidence: invalid JSON document")
			}
			if err := resolvePipelineEvidencePaths(args[0], &params); err != nil {
				return err
			}
			var result protocol.ModelMetadataRegistrationResult
			if err := call(cmd, protocol.ControlMethodModelRegisterPipeline, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}

func resolvePipelineEvidencePaths(documentPath string, params *protocol.RegisterModelPipelineRequest) error {
	document, err := filepath.Abs(documentPath)
	if err != nil {
		return fmt.Errorf("resolve model pipeline evidence: %w", err)
	}
	base := filepath.Dir(document)
	refs := []*protocol.ModelEvidenceReference{
		&params.Manifest, &params.TrainingRunMetadata, &params.AdapterMetadata,
		&params.BasePredictions, &params.CandidatePredictions, &params.SafetyManifest,
		&params.SafetyBasePredictions, &params.SafetyCandidatePredictions,
	}
	if params.FinalEvaluation != nil {
		refs = append(refs,
			&params.FinalEvaluation.TrainingManifest,
			&params.FinalEvaluation.ValidationCalibrationSessions,
			&params.FinalEvaluation.BlindTestSessions,
			&params.FinalEvaluation.SealedCalibrations,
		)
	}
	for _, ref := range refs {
		path := ref.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(base, path)
		}
		absolute, absErr := filepath.Abs(path)
		if absErr != nil {
			return fmt.Errorf("resolve model pipeline evidence: %w", absErr)
		}
		ref.Path = filepath.Clean(absolute)
	}
	return nil
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
	const maxModelMetadataBytes = 1 << 20
	data, err := io.ReadAll(io.LimitReader(file, maxModelMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read model metadata: %w", err)
	}
	if len(data) > maxModelMetadataBytes {
		return nil, fmt.Errorf("read model metadata: document exceeds %d bytes", maxModelMetadataBytes)
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
