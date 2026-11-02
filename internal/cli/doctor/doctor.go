// Package doctor implements `gifford doctor`: it creates and validates
// gifford's three XDG-based directories and reports on config.yaml. Unlike
// every other command in this subphase, it has real behaviour, not a
// not-implemented stub.
package doctor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/socketpath"
	"github.com/smellak/gifford/internal/xdg"
)

const maxDaemonTemporaryDigits = 10

type namedSocketPath struct {
	name string
	path string
}

// NewCmd returns the gifford doctor command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var peer string
	var agent string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Validate the installation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.OutOrStdout(), peer, agent)
		},
	}
	cmd.Flags().StringVar(&peer, "peer", "", "also check federation status for this peer (not implemented yet)")
	cmd.Flags().StringVar(&agent, "agent", "", "also check socket paths for this agent")
	return cmd
}

// reportWriter writes doctor's report lines, keeping the first write
// error so callers handle it once at the end.
type reportWriter struct {
	w   io.Writer
	err error
}

func (r *reportWriter) printf(format string, args ...any) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintf(r.w, format, args...)
}

func run(out io.Writer, peer, agent string) error {
	rep := &reportWriter{w: out}

	dirs, err := xdg.Resolve()
	if err != nil {
		rep.printf("%-11s %-4s %s\n", "XDG", "FAIL", err)
		if rep.err != nil {
			return fmt.Errorf("write doctor report: %w", rep.err)
		}
		return &deps.ExitError{Code: exitcode.Error, Err: err}
	}

	checks, ensureErr := xdg.EnsureAll(dirs)
	ok := ensureErr == nil
	for _, c := range checks {
		state := "exists"
		if c.Created {
			state = "created"
		}
		if c.OK {
			rep.printf("%-11s %-4s %s %s (0700)\n", strings.ToUpper(c.Name), "OK", state, c.Path)
			continue
		}
		ok = false
		detail := fmt.Sprintf("mode %04o, want 0700", c.Mode)
		if info, statErr := os.Stat(c.Path); statErr == nil && !info.IsDir() {
			detail = "not a directory"
		}
		rep.printf("%-11s %-4s %s %s (%s)\n", strings.ToUpper(c.Name), "FAIL", state, c.Path, detail)
	}
	if !reportSockets(rep, dirs.Runtime, agent) {
		ok = false
	}

	cfgPath := config.DefaultPath(dirs.Config)
	if cfg, cfgErr := config.Load(cfgPath); cfgErr != nil {
		ok = false
		rep.printf("%-11s %-4s %s\n", "CONFIG.YAML", "FAIL", cfgErr)
	} else {
		rep.printf("%-11s %-4s schema_version=%d (%s)\n", "CONFIG.YAML", "OK", cfg.SchemaVersion, cfgPath)
	}

	if peer != "" {
		rep.printf("PEER %-6s %-4s federation checks not implemented yet\n", peer, "SKIP")
	}

	if !ok {
		return &deps.ExitError{Code: exitcode.Error, Err: errors.New("doctor found problems, see the report above")}
	}
	if rep.err != nil {
		return fmt.Errorf("write doctor report: %w", rep.err)
	}
	return nil
}

func reportSockets(rep *reportWriter, runtimeDir, agent string) bool {
	basePaths := []namedSocketPath{
		{name: "hub", path: filepath.Join(runtimeDir, "hub.sock")},
		{
			name: "daemon temporary",
			path: filepath.Join(
				runtimeDir,
				".daemon.sock-"+strings.Repeat("9", maxDaemonTemporaryDigits),
			),
		},
	}
	if failures := socketFailures(basePaths); len(failures) > 0 {
		rep.printf(
			"%-11s %-4s runtime=%s: %s\n",
			"SOCKETS", "FAIL", runtimeDir, strings.Join(failures, "; "),
		)
		return false
	}

	capacity := fmt.Sprintf(
		"max_agent_bytes native-events=%d agents=%d adapters=%d",
		agentCapacity(runtimeDir, "native-events"),
		agentCapacity(runtimeDir, "agents"),
		agentCapacity(runtimeDir, "adapters"),
	)
	if agent == "" {
		rep.printf("%-11s %-4s runtime=%s %s\n", "SOCKETS", "OK", runtimeDir, capacity)
		return true
	}
	if !protocol.AgentID(agent).Valid() {
		rep.printf(
			"%-11s %-4s runtime=%s %s; agent=%s is not a valid agent id\n",
			"SOCKETS", "FAIL", runtimeDir, capacity, agent,
		)
		return false
	}

	agentPaths := []namedSocketPath{
		{name: "native-events", path: filepath.Join(runtimeDir, "native-events", agent+".sock")},
		{name: "agents", path: filepath.Join(runtimeDir, "agents", agent+".sock")},
		{name: "adapters", path: filepath.Join(runtimeDir, "adapters", agent+".sock")},
	}
	if failures := socketFailures(agentPaths); len(failures) > 0 {
		rep.printf(
			"%-11s %-4s runtime=%s %s; agent=%s: %s\n",
			"SOCKETS", "FAIL", runtimeDir, capacity, agent, strings.Join(failures, "; "),
		)
		return false
	}
	rep.printf(
		"%-11s %-4s runtime=%s %s; agent=%s fits\n",
		"SOCKETS", "OK", runtimeDir, capacity, agent,
	)
	return true
}

func socketFailures(paths []namedSocketPath) []string {
	var failures []string
	for _, candidate := range paths {
		if err := socketpath.Check(candidate.path); err != nil {
			failures = append(failures, candidate.name+": "+err.Error())
		}
	}
	return failures
}

func agentCapacity(runtimeDir, directory string) int {
	emptyAgentPath := filepath.Join(runtimeDir, directory, ".sock")
	return socketpath.MaxBytes - len(emptyAgentPath)
}
