// Package initcmd implements `gifford init`: it writes a first config.yaml
// and creates the state directories.
package initcmd

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford init command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var nodeID string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Write config.yaml and create the state directories",
		Long: `Write config.yaml with schema_version 1 and the node id, and create the
config, data and runtime directories as gifford doctor does: the
directories 0700, config.yaml 0600. --node-id sets the node id; without
it, init generates one. An existing config.yaml is never overwritten: init
refuses instead.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.OutOrStdout(), nodeID)
		},
	}
	cmd.Flags().StringVar(&nodeID, "node-id", "", "node id: lowercase letters, digits, '.', '_' and '-' (default: generated)")
	return cmd
}

func run(out io.Writer, nodeID string) error {
	if nodeID == "" {
		nodeID = newNodeID()
	} else if !protocol.NodeID(nodeID).Valid() {
		return exit(exitcode.Usage, fmt.Errorf("invalid --node-id %q: want %s", nodeID, protocol.NodeID("").JSONPattern()))
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return exit(exitcode.Error, err)
	}
	if _, err := xdg.EnsureAll(dirs); err != nil {
		return exit(exitcode.Error, fmt.Errorf("%w; see gifford doctor", err))
	}
	path := config.DefaultPath(dirs.Config)
	if err := writeNew(path, configText(nodeID)); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return exit(exitcode.Refused, fmt.Errorf("%s already exists, and init never overwrites it", path))
		}
		return exit(exitcode.Error, err)
	}
	if _, err := config.Load(path); err != nil {
		return exit(exitcode.Error, err)
	}
	if _, err := fmt.Fprintf(out, "gifford init: wrote %s (node_id %s)\n"+
		"gifford init: directories %s, %s and %s (0700)\n"+
		"next: gifford up\n", path, nodeID, dirs.Config, dirs.Data, dirs.Runtime); err != nil {
		return exit(exitcode.Error, err)
	}
	return nil
}

// configText is the first config.yaml: the schema version and the node id.
func configText(nodeID string) string {
	return "# gifford configuration, written by gifford init.\n" +
		fmt.Sprintf("schema_version: %d\n", config.SupportedSchemaVersion) +
		"node_id: " + nodeID + "\n"
}

// writeNew creates path, 0600, with text, and fails with fs.ErrExist when
// it exists. A failed write leaves no file.
func writeNew(path, text string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	_, werr := f.WriteString(text)
	if err := errors.Join(werr, f.Close()); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// newNodeID is a random node id, "node-" and 8 hex digits.
func newNodeID() string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails.
	return "node-" + hex.EncodeToString(b[:])
}

func exit(code int, err error) error {
	return &deps.ExitError{Code: code, Err: fmt.Errorf("gifford init: %w", err)}
}
