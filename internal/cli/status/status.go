// Package status implements `gifford status`: whether the node is up, its
// registered agents and the channel state, read from the node's own
// sockets.
package status

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/cli/nodeproc"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tui"
	"github.com/smellak/gifford/internal/xdg"
)

const (
	controlSocket = "daemon.sock"
	// callTimeout bounds each dial and each control call.
	callTimeout = 2 * time.Second
)

// NewCmd returns the gifford status command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show whether the node is up, its agents and the channel state",
		Long: `Show whether the node is up, its registered agents and the global
channel state. The node is up when hub.sock and daemon.sock both answer.
The command exits 1 when the node is down.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			dirs, err := xdg.Resolve()
			if err != nil {
				return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford status: %w", err)}
			}
			r := gather(cmd.Context(), dirs.Runtime)
			if err := r.write(cmd.OutOrStdout()); err != nil {
				return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford status: %w", err)}
			}
			if !r.up() {
				return &deps.ExitError{Code: exitcode.Error, Err: errors.New("gifford status: the node is down")}
			}
			return nil
		},
	}
}

// report is what gifford status found.
type report struct {
	hubSocket, controlSocket bool
	// pid is the PID gifford up recorded, or 0 without one.
	pid     int
	channel protocol.ChannelState
	agents  []protocol.AgentListItem
	// more means the node has more agents than one page shows.
	more bool
}

func (r report) up() bool { return r.hubSocket && r.controlSocket }

// gather asks the node's sockets in runtimeDir. The control socket counts
// as answering once both control calls succeed.
func gather(ctx context.Context, runtimeDir string) report {
	r := report{hubSocket: answers(ctx, filepath.Join(runtimeDir, hubsrv.SocketName))}
	if pid, err := nodeproc.ReadPID(runtimeDir); err == nil {
		r.pid = pid
	}
	client, err := tui.NewClient(tui.ClientOptions{SocketPath: filepath.Join(runtimeDir, controlSocket), Timeout: callTimeout})
	if err != nil {
		return r
	}
	var agents protocol.AgentListResult
	if client.Call(ctx, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &r.channel) != nil ||
		client.Call(ctx, protocol.ControlMethodAgentList, protocol.AgentListParams{}, &agents) != nil {
		return r
	}
	r.controlSocket = true
	r.agents, r.more = agents.Agents, agents.NextCursor != ""
	return r
}

// answers reports whether a listener accepts on the Unix socket at path.
func answers(ctx context.Context, path string) bool {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// write prints the report: a few aligned lines, one more per agent.
func (r report) write(out io.Writer) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	node := "down"
	if r.up() {
		node = "up (not started by gifford up)"
		if r.pid != 0 {
			node = fmt.Sprintf("up (PID %d)", r.pid)
		}
	}
	_, _ = fmt.Fprintf(w, "node\t%s\n", node)
	_, _ = fmt.Fprintf(w, "sockets\t%s %s, %s %s\n",
		hubsrv.SocketName, answer(r.hubSocket), controlSocket, answer(r.controlSocket))
	if r.controlSocket {
		channel := string(r.channel.Mode)
		if r.channel.EffectiveMode != r.channel.Mode {
			channel += ", effective " + string(r.channel.EffectiveMode)
		}
		_, _ = fmt.Fprintf(w, "channel\t%s\n", channel)
		count := fmt.Sprint(len(r.agents))
		if r.more {
			count += " shown; see gifford panel for the rest"
		}
		_, _ = fmt.Fprintf(w, "agents\t%s\n", count)
		for _, a := range r.agents {
			_, _ = fmt.Fprintf(w, "\t%s\t%s\t%s\t%s\tqueued %d\n", a.AgentID, a.Harness, a.SessionState, a.Phase.Value, a.Queue.Queued)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("write the status: %w", err)
	}
	return nil
}

func answer(ok bool) string {
	if ok {
		return "ok"
	}
	return "no answer"
}
